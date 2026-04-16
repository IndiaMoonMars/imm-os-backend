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

from confluent_kafka import Consumer, KafkaError
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [processor] %(message)s")
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

# ── InfluxDB client ─────────────────────────────────────────────────
influx   = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx.write_api(write_options=SYNCHRONOUS)

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

# ── Metric fields per sensor ────────────────────────────────────────
SENSOR_METRICS = {
    "bme280":     ["temp", "hum", "pres"],
    "scd40":      ["co2_ppm", "temp", "hum"],
    "mq7":        ["co_ppm"],
    "max30100":   ["hr_bpm", "spo2_pct"],
    "ecg_ad8232": ["voltage"],
    "tsl2561":    ["lux"],
    "ina219":     ["voltage_v", "current_ma", "power_mw"],
}

# ── Processor ───────────────────────────────────────────────────────
def normalise_timestamp(ts: int | float) -> int:
    """Clamp sensor timestamp to ±30s from server UTC (IEEE 1588-style correction)."""
    now = int(time.time())
    drift = ts - now
    if abs(drift) > 30:
        log.debug("Timestamp drift %.1fs corrected", drift)
        return now
    return int(ts)

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
    metrics = SENSOR_METRICS.get(sensor, [])

    for metric in metrics:
        value = data.get(metric)
        if value is None:
            continue

        key = (sensor, metric)
        z   = zscore(key, float(value))

        point = (
            Point(sensor)
            .tag("zone", zone)
            .tag("metric", metric)
            .field("value", float(value))
            .time(ts, WritePrecision.SECONDS)
        )

        # Write to normal bucket
        write_api.write(bucket=INFLUX_BUCKET, record=point)

        # Anomaly detection
        if z is not None and abs(z) > ZSCORE_THRESHOLD:
            log.warning("ANOMALY %s.%s value=%.3f z=%.2f", sensor, metric, value, z)
            alert_point = (
                Point("anomaly")
                .tag("sensor", sensor)
                .tag("metric", metric)
                .tag("zone", zone)
                .field("value", float(value))
                .field("zscore", z)
                .time(ts, WritePrecision.SECONDS)
            )
            write_api.write(bucket=INFLUX_ALERTS_BUCKET, record=alert_point)

    log.debug("Processed %s ts=%d", sensor, ts)

# ── Main ────────────────────────────────────────────────────────────
def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([VALIDATED_TOPIC])
    log.info("Processor subscribed to %s", VALIDATED_TOPIC)

    def _shutdown(sig, frame):
        log.info("Shutting down processor...")
        consumer.close()
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
