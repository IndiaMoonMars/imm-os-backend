#!/usr/bin/env python3
"""
IMM-OS Telemetry Processor
Consumes 'telemetry.validated' Kafka topic, applies:
  1. IEEE 1588-style timestamp normalisation (UTC correction)
  2. Z-score anomaly detection (rolling 60-sample window per sensor+metric)
  3. Dual-write: normal → InfluxDB 'habitat_sensors', anomalies → 'habitat_alerts'
Each point keeps the reading's quality (fields q, delayed). Limit alarms are raised by
the health monitor (services/health_monitor.py).
"""

import os
import json
import time
import logging
import signal
import sys
import threading
from collections import defaultdict, deque
import statistics

from confluent_kafka import Consumer, KafkaError, KafkaException
from datetime import datetime, timezone, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from time_service.math_engine import calculate_all

# Enforce IST globally across logging outputs
ist_tz = timezone(timedelta(hours=5, minutes=30))
logging.Formatter.converter = lambda *args: datetime.now(ist_tz).timetuple()
logging.basicConfig(level=logging.INFO, format="[UTC %(asctime)s] [IST %(message)s")
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_GROUP_ID     = "imm-telemetry-processor"
VALIDATED_TOPIC    = "telemetry.validated"

INFLUX_URL         = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN       = os.getenv("INFLUX_TOKEN", "imm-super-secret-token")
INFLUX_ORG         = os.getenv("INFLUX_ORG", "imm_org")
INFLUX_BUCKET      = "habitat_sensors"
INFLUX_ALERTS_BUCKET = "habitat_alerts"

ZSCORE_WINDOW      = 60   # samples per rolling window
ZSCORE_THRESHOLD   = 3.0  # standard deviations for anomaly
# Waveforms: every heartbeat's R-peak is a >3σ "anomaly"; hard limits still apply.
NO_ZSCORE_SENSORS  = {"ecg_ad8232", "bno055"}   # waveform / orientation: large swings are normal

# ── InfluxDB client ─────────────────────────────────────────────────
influx   = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
# At-least-once: the main loop takes up to BATCH_MAX messages from Kafka (the ECG alone
# is ~100 points/s), writes their points synchronously, and commits the Kafka offsets
# only once InfluxDB has accepted them. If InfluxDB is down the batch is retried and
# the rest waits in Kafka, so nothing is lost; a replay after a crash rewrites the same
# points (same series and time), which InfluxDB stores once.
write_api = influx.write_api(write_options=SYNCHRONOUS)
BATCH_MAX = 1000

# ── Z-score state ───────────────────────────────────────────────────
# Key: (sensor, metric) → deque of recent values
windows: dict = defaultdict(lambda: deque(maxlen=ZSCORE_WINDOW))

def zscore(key: tuple, value: float) -> float | None:
    """Return z-score once we have enough samples, else None."""
    w = windows[key]
    w.append(value)
    if len(w) < 10:
        return None
    mean = statistics.mean(w)
    stdev = statistics.stdev(w)
    if stdev == 0:
        return 0.0
    return (value - mean) / stdev

# ── Metric fields per sensor (shared with the validator) ────────────
from telemetry_schema import SENSOR_METRICS, STATE_METRICS  # noqa: E402  (script runs from services/)
import heartbeat  # noqa: E402

# state flags and counters: a change is news, not an anomaly
NO_ZSCORE_METRICS = STATE_METRICS


def ensure_buckets():
    """Create the sensor and alert buckets if missing (fresh or pre-existing InfluxDB volume)."""
    api = influx.buckets_api()
    org_id = next((o.id for o in influx.organizations_api().find_organizations(org=INFLUX_ORG)), None)
    for name, days in ((INFLUX_BUCKET, 90), (INFLUX_ALERTS_BUCKET, 30)):
        if api.find_bucket_by_name(name) is None:
            from influxdb_client import BucketRetentionRules
            api.create_bucket(bucket_name=name, org_id=org_id,
                              retention_rules=BucketRetentionRules(type="expire", every_seconds=days * 86400))
            log.info("Created InfluxDB bucket %s (%d-day retention)", name, days)

# ── Processor ───────────────────────────────────────────────────────
MAX_FUTURE_S = 30.0                                          # node clock ahead
MAX_BACKFILL_S = float(os.getenv("MAX_BACKFILL_S", 7 * 86400))  # older = node clock broken


def normalise_timestamp(ts: int | float) -> float:
    """
    The reading's own time, in seconds with milliseconds (ECG is 100 Hz). Past readings
    keep their time: a node's store-and-forward backlog after an outage must land where
    it was measured (the validator marks it delayed), and a replayed reading must hit
    the same point to overwrite, not duplicate, it. Only a clock that is clearly wrong
    (ahead of the server, or more than MAX_BACKFILL_S behind) is replaced by the
    server time; the validator has flagged such readings.
    """
    now = time.time()
    drift = float(ts) - now
    if drift > MAX_FUTURE_S or drift < -MAX_BACKFILL_S:
        log.debug("Timestamp drift %.1fs corrected", drift)
        return round(now, 3)
    return round(float(ts), 3)

def _out(bucket: str, point, sink) -> None:
    if sink is None:
        write_api.write(bucket=bucket, record=point)
    else:
        sink.append((bucket, point))


def process_message(raw: bytes, sink: list = None):
    """Build the InfluxDB points for one validated reading: into sink, or written now."""
    try:
        envelope = json.loads(raw)
        data = envelope.get("data", envelope)  # support bare or enveloped
    except json.JSONDecodeError:
        log.warning("Non-JSON message skipped")
        return

    sensor = data.get("sensor")
    ts     = normalise_timestamp(data.get("timestamp", time.time()))
    zone   = data.get("zone", "unknown")
    node_id = str(data.get("node_id") or "unknown")
    simulated = "true" if data.get("simulated") else "false"
    quality = data.get("q") if data.get("q") in ("good", "suspect", "bad") else "good"
    delayed = "true" if data.get("delayed") else "false"
    metrics = SENSOR_METRICS.get(sensor, [])

    time_data = calculate_all(ts)
    ist_date = time_data["ist"][:10]  # Just the YYYY-MM-DD
    sol_str = str(int(time_data["msd"]))
    
    # Let's say mission day 1 is Unix epoch + 1 (arbitrary for now unless given)
    # The requirement specifically mentions "mission_day=1, sol=1, ist_date=2026-03-10". 
    # We will compute it dynamically based on the current UTC tracking.
    mission_day = str(max(1, int((ts - 1710000000) / 86400))) # basic simulation baseline

    for metric in metrics:
        value = data.get(metric)
        if value is None:
            continue

        key = (sensor, metric)
        z   = None if sensor in NO_ZSCORE_SENSORS or metric in NO_ZSCORE_METRICS else zscore(key, float(value))

        point = (
            Point(sensor)
            .tag("zone", zone)
            .tag("node_id", node_id)
            .tag("simulated", simulated)
            .tag("crew_id", str(data.get("crew_id") or "-"))
            .tag("metric", metric)
            .tag("mission_day", mission_day)
            .tag("sol", sol_str)
            .tag("ist_date", ist_date)
            .field("value", float(value))
            # quality as fields, not tags: the series stays the same, so a replayed reading
            # overwrites the original instead of duplicating it
            .field("q", quality)
            .field("delayed", delayed == "true")
            .time(int(round(ts * 1000)), WritePrecision.MS)
        )

        # Write to normal bucket
        _out(INFLUX_BUCKET, point, sink)

        # Anomaly detection (Z-score + Physical Threshold Logging)
        if z is not None and abs(z) > ZSCORE_THRESHOLD:
            log.warning("ANOMALY Z-SCORE %s.%s value=%.3f z=%.2f", sensor, metric, value, z)
            alert_point = (
                Point("anomaly")
                .tag("sensor", sensor)
                .tag("metric", metric)
                .tag("zone", zone)
                .field("value", float(value))
                .field("zscore", z)
                .time(int(round(ts * 1000)), WritePrecision.MS)
            )
            _out(INFLUX_ALERTS_BUCKET, alert_point, sink)
            
        # Limit alarms (with hysteresis, one alarm per condition, acknowledgement) are the
        # health monitor's (services/health/rules.py), not one database row per reading.

    log.debug("Processed %s ts=%d", sensor, ts)

# ── Main ────────────────────────────────────────────────────────────
def write_batch(batch: list) -> None:
    """Write [(bucket, point)] and return once InfluxDB has them (retrying while it is down)."""
    by_bucket: dict = {}
    for bucket, point in batch:
        by_bucket.setdefault(bucket, []).append(point)
    for bucket, points in by_bucket.items():
        delay, failures = 1.0, 0
        while True:
            try:
                write_api.write(bucket=bucket, record=points)
                if failures:
                    log.info("InfluxDB writes resumed after %d failed attempt(s)", failures)
                break
            except ApiException as exc:
                if exc.status and 400 <= exc.status < 500 and exc.status != 429:
                    # the data itself was refused (e.g. a field type conflict): retrying can't
                    # help, and holding the batch would stop the whole pipeline
                    log.error("InfluxDB refused %d point(s) for %s: %s", len(points), bucket, str(exc)[:300])
                    break
                err = exc
            except Exception as exc:  # connection refused, timeout: InfluxDB down or restarting
                err = exc
            failures += 1
            if failures == 1 or failures % 10 == 0:
                log.warning("InfluxDB write failed (%s); keeping %d point(s) and retrying", str(err)[:200], len(points))
            heartbeat.beat()     # alive and waiting: an InfluxDB outage is not a reason to restart us
            time.sleep(delay)
            delay = min(delay * 2, 30.0)


def main():
    for attempt in range(10):
        try:
            ensure_buckets()
            break
        except Exception as exc:  # InfluxDB still starting
            log.warning("InfluxDB bucket check failed (%s); retrying", exc)
            time.sleep(3)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "topic.metadata.refresh.interval.ms": 10000,  # topics may be created after start-up
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,                  # committed after each batch is stored
    })
    consumer.subscribe([VALIDATED_TOPIC])
    log.info("Processor subscribed to %s", VALIDATED_TOPIC)

    def _shutdown(sig, frame):
        log.info("Shutting down processor...")
        # Nothing is lost by stopping here: offsets are committed only after a batch is
        # stored. Bounded, so a hung connection can't delay the exit and the restart.
        closer = threading.Thread(target=consumer.close, daemon=True)
        closer.start()
        closer.join(5)
        os._exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    while True:
        msgs = consumer.consume(num_messages=BATCH_MAX, timeout=1.0)
        heartbeat.beat()
        if not msgs:
            continue
        batch: list = []
        for msg in msgs:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
                continue
            try:
                process_message(msg.value(), batch)
            except Exception as exc:   # one malformed reading must not stop the rest
                log.error("Skipping unprocessable message: %s", exc)
        if batch:
            write_batch(batch)
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:  # nothing to commit, or a rebalance: the next batch commits
            log.debug("commit: %s", exc)

if __name__ == "__main__":
    main()
