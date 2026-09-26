#!/usr/bin/env python3
"""
IMM-OS Telemetry Processor
Consumes 'telemetry.validated' Kafka topic, applies:
  1. IEEE 1588-style timestamp normalisation (UTC correction)
  2. Z-score anomaly detection (rolling 60-sample window per sensor+metric)
  3. Dual-write: normal → InfluxDB 'habitat_sensors', anomalies → 'habitat_alerts'
"""

import os
import json
import time
import logging
import signal
import sys
from collections import defaultdict, deque
import statistics
import asyncio
import asyncpg

from confluent_kafka import Consumer, KafkaError
from datetime import datetime, timezone, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

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
NO_ZSCORE_SENSORS  = {"ecg_ad8232"}

# ── InfluxDB client ─────────────────────────────────────────────────
influx   = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
# Batched writes: the ECG alone is ~100 points/s. Realtime views read the WebSocket
# (telemetry-ingest), so up to 1 s of write latency only affects history queries.
write_api = influx.write_api(write_options=WriteOptions(batch_size=500, flush_interval=1000, jitter_interval=0))

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
from telemetry_schema import SENSOR_METRICS  # noqa: E402  (script runs from services/)


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
def normalise_timestamp(ts: int | float) -> float:
    """Clamp sensor timestamp to ±30s from server UTC; keeps milliseconds (ECG is 100 Hz)."""
    now = time.time()
    drift = float(ts) - now
    if abs(drift) > 30:
        log.debug("Timestamp drift %.1fs corrected", drift)
        return round(now, 3)
    return round(float(ts), 3)

def process_message(raw: bytes):
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
        z   = None if sensor in NO_ZSCORE_SENSORS else zscore(key, float(value))

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
            .time(int(ts * 1000), WritePrecision.MS)
        )

        # Write to normal bucket
        write_api.write(bucket=INFLUX_BUCKET, record=point)

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
                .time(int(ts * 1000), WritePrecision.MS)
            )
            write_api.write(bucket=INFLUX_ALERTS_BUCKET, record=alert_point)
            
        # Hard Physical Thresholds Logic (habitat + EVA suit)
        hard_limit_breached = False
        breach_reason = ""
        if sensor == "scd40" and metric == "co2_ppm" and value > 1000.0:
            hard_limit_breached = True; breach_reason = "CO2 > 1000 ppm"
        elif sensor == "bme280" and metric == "temp" and (value < 10.0 or value > 35.0):
            hard_limit_breached = True; breach_reason = f"Habitat temp out of range: {value}°C"
        elif sensor == "ina219" and metric == "current_ma" and value > 3000.0:
            hard_limit_breached = True; breach_reason = "Power current spike"
        elif sensor == "o2" and metric == "o2_pct" and (value < 19.5 or value > 23.5):
            hard_limit_breached = True; breach_reason = f"O\u2082 out of range: {value}%"
        elif sensor == "mq7" and metric == "co_ppm" and value > 35.0:
            hard_limit_breached = True; breach_reason = f"CO > 35 ppm: {value}"
        # Edge node health (sysmon_driver.py on every node)
        elif sensor == "sysmon" and metric == "undervolt" and value >= 1:
            hard_limit_breached = True; breach_reason = "Edge node under-voltage (power supply too weak)"
        elif sensor == "sysmon" and metric == "cpu_temp" and value > 80.0:
            hard_limit_breached = True; breach_reason = f"Edge node overheating: {value}°C"
        # EVA Suit biosensor limits
        elif sensor == "eva_biosensor" and metric == "hr_bpm" and value > 160.0:
            hard_limit_breached = True; breach_reason = f"EVA HR critical: {value} BPM"
        elif sensor == "eva_biosensor" and metric == "spo2_pct" and value < 94.0:
            hard_limit_breached = True; breach_reason = f"EVA SpO\u2082 critical: {value}%"
        elif sensor == "eva_biosensor" and metric == "skin_temp_c" and value > 38.5:
            hard_limit_breached = True; breach_reason = f"EVA skin temp: {value}°C"
            
        if hard_limit_breached:
            log.warning(f"PHYSICAL ALARM BREACHED [{breach_reason}] {sensor} {metric} -> {value}")
            _z = z if z is not None else 0.0
            
            # Fire and forget async commit to standard PG tracking table
            async def commit_pg_alert():
                try:
                    conn = await asyncpg.connect(
                        user=os.getenv("POSTGRES_USER", "admin"),
                        password=os.getenv("POSTGRES_PASSWORD", "changeme"),
                        database=os.getenv("POSTGRES_DB", "imm_db"),
                        host=os.getenv("POSTGRES_HOST", "postgres")
                    )
                    await conn.execute("""
                        INSERT INTO alert_history (sensor_id, metric, metric_value, zscore, alert_timestamp) 
                        VALUES ($1, $2, $3, $4, to_timestamp($5))
                    """, sensor, metric, float(value), _z, ts)
                    await conn.close()
                except Exception as e:
                    log.error(f"Postgres Alarm Commit failed: {e}")
                    
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(commit_pg_alert())
            except RuntimeError: # No loop, create blocking push
                asyncio.run(commit_pg_alert())

    log.debug("Processed %s ts=%d", sensor, ts)

# ── Main ────────────────────────────────────────────────────────────
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
        "enable.auto.commit": True,
    })
    consumer.subscribe([VALIDATED_TOPIC])
    log.info("Processor subscribed to %s", VALIDATED_TOPIC)

    def _shutdown(sig, frame):
        log.info("Shutting down processor...")
        consumer.close()
        write_api.close()   # flush the pending batch
        influx.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("Kafka error: %s", msg.error())
            continue
        process_message(msg.value())

if __name__ == "__main__":
    main()
