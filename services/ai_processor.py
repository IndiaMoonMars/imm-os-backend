#!/usr/bin/env python3
"""
IMM-OS AI Processor
Consumes 'telemetry.validated' Kafka topic, applies multi-variate anomaly detection
using Isolation Forest (scikit-learn).
"""

import os
import json
import time
import logging
import signal
import sys
import pandas as pd
import numpy as np
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from confluent_kafka import Consumer, KafkaError
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.ensemble import IsolationForest
import asyncio
import asyncpg

# Enforce IST globally across logging outputs
ist_tz = timezone(timedelta(hours=5, minutes=30))
logging.Formatter.converter = lambda *args: datetime.now(ist_tz).timetuple()
logging.basicConfig(level=logging.INFO, format="[UTC %(asctime)s] [IST %(message)s")
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_GROUP_ID     = "imm-ai-processor"
VALIDATED_TOPIC    = "telemetry.validated"

INFLUX_URL         = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN       = os.getenv("INFLUX_TOKEN", "imm-super-secret-token")
INFLUX_ORG         = os.getenv("INFLUX_ORG", "imm_org")
INFLUX_BUCKET      = "ai_insights"

POSTGRES_HOST      = os.getenv("POSTGRES_HOST", "localhost")
PG_URL             = f"postgresql://imm_user:imm_pass@{POSTGRES_HOST}:5432/imm_db"

MODEL_WINDOW       = 100  # samples to look back for multi-variate context
MODEL_CONTAMINATION = 0.05 # expected % of anomalies

# ── InfluxDB client ─────────────────────────────────────────────────
influx   = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx.write_api(write_options=SYNCHRONOUS)

# ── Multi-variate State ─────────────────────────────────────────────
# We group sensors into 'systems' for correlation analysis
SYSTEMS = {
    "eclss": ["scd40.co2_ppm", "scd40.temp", "bme280.hum", "mq7.co_ppm"],
    "power": ["ina219.voltage_v", "ina219.current_ma", "ina219.power_mw"],
    "crew":  ["max30100.hr_bpm", "max30100.spo2_pct"]
}

# Key: system_name -> deque of dictionaries (timestamp + values)
system_windows = defaultdict(lambda: deque(maxlen=MODEL_WINDOW))

async def log_insight_to_pg(system: str, type: str, severity: str, summary: str, metadata: dict):
    """Log an AI insight to Postgres for history and dashboarding."""
    try:
        conn = await asyncpg.connect(PG_URL)
        await conn.execute("""
            INSERT INTO ai_insights (system_area, insight_type, severity, summary, metadata)
            VALUES ($1, $2, $3, $4, $5)
        """, system, type, severity, summary, json.dumps(metadata))
        await conn.close()
    except Exception as e:
        log.error(f"Failed to log insight to Postgres: {e}")

def run_anomaly_detection(system_name: str):
    """Perform Isolation Forest anomaly detection on the current system window."""
    window = system_windows[system_name]
    if len(window) < 50: # Need warming period
        return

    df = pd.DataFrame(window).drop(columns=['ts']).fillna(method='ffill').fillna(0)
    
    # Train Isolation Forest
    model = IsolationForest(contamination=MODEL_CONTAMINATION, random_state=42)
    # Fit on all but the last point
    model.fit(df.iloc[:-1])
    # Predict the last point
    pred = model.predict(df.iloc[[-1]])[0] # -1 for anomaly, 1 for normal
    score = -model.decision_function(df.iloc[[-1]])[0] # Higher score = more anomalous

    if pred == -1:
        log.warning(f"AI ANOMALY DETECTED in {system_name} system! Score: {score:.3f}")
        
        # Identify 'culprit' (simplified contribution analysis)
        last_row = df.iloc[-1]
        mean_row = df.mean()
        diffs = (last_row - mean_row).abs()
        culprit = diffs.idxmax()
        
        summary = f"Multi-variate anomaly in {system_name}. Primary driver: {culprit} Divergence."
        metadata = {
            "score": float(score),
            "culprit": culprit,
            "window_size": len(window),
            "timestamp": window[-1]['ts']
        }
        
        # Log to Influx
        point = (
            Point("ai_anomaly")
            .tag("system", system_name)
            .tag("culprit", culprit)
            .field("score", float(score))
            .time(window[-1]['ts'], WritePrecision.SECONDS)
        )
        write_api.write(bucket=INFLUX_BUCKET, record=point)
        
        # Log to Postgres
        asyncio.run(log_insight_to_pg(system_name, "anomaly", "warning", summary, metadata))

def process_message(raw: bytes):
    try:
        envelope = json.loads(raw)
        data = envelope.get("data", envelope)
    except json.JSONDecodeError:
        return

    sensor = data.get("sensor")
    ts = int(data.get("timestamp", time.time()))
    
    # Update all systems that include this sensor
    for sys_name, metrics in SYSTEMS.items():
        relevant_metrics = [m for m in metrics if m.startswith(f"{sensor}.")]
        if not relevant_metrics:
            continue
            
        # Get or create current state for this timestamp in this system
        # Since Kafka preserves order, we usually append. In multi-sensor systems, 
        # we might need to merge samples into the same time bucket or handle async arrival.
        # For this implementation, we take a simple approach: find the last entry 
        # or create a new one if timestamp moves forward.
        
        if not system_windows[sys_name] or system_windows[sys_name][-1]['ts'] < ts:
            # New time window
            new_entry = {'ts': ts}
            # Carry over previous values for other sensors (persistence of state)
            if system_windows[sys_name]:
                prev = system_windows[sys_name][-1]
                new_entry.update({k: v for k, v in prev.items() if k != 'ts'})
            system_windows[sys_name].append(new_entry)
            
        current = system_windows[sys_name][-1]
        for m in relevant_metrics:
            field = m.split(".")[1]
            if field in data:
                current[m] = float(data[field])
        
        # Trigger detection periodically or on every message after warming
        if len(system_windows[sys_name]) >= 50:
            run_anomaly_detection(sys_name)

# ── Main ────────────────────────────────────────────────────────────
def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([VALIDATED_TOPIC])
    log.info(f"AI Processor subscribed to {VALIDATED_TOPIC}")

    def _shutdown(sig, frame):
        log.info("Shutting down AI processor...")
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
                log.error(f"Kafka error: {msg.error()}")
            continue
        process_message(msg.value())

if __name__ == "__main__":
    main()
