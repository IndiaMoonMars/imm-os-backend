#!/usr/bin/env python3
"""
IMM-OS Telemetry Ingest Server
POST /ingest  — validates JSON schema, forwards to Kafka 'telemetry.validated'
GET  /health  — liveness probe
"""

import os
import json
import logging
from typing import Optional, Union
from enum import Enum

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from confluent_kafka import Producer, KafkaException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ingest] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
VALIDATED_TOPIC   = "telemetry.validated"

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all", "retries": 5})

app = FastAPI(title="IMM-OS Telemetry Ingest", version="0.1.0")

# ── Pydantic sensor schemas ────────────────────────────────────────

class SensorType(str, Enum):
    bme280   = "bme280"
    scd40    = "scd40"
    mq7      = "mq7"
    max30100 = "max30100"
    ecg_ad8232 = "ecg_ad8232"
    tsl2561  = "tsl2561"
    ina219   = "ina219"

class TelemetryPayload(BaseModel):
    sensor:    SensorType
    timestamp: int = Field(..., gt=0)
    sig:       Optional[str] = None

    # BME280
    temp:      Optional[float] = None
    hum:       Optional[float] = None
    pres:      Optional[float] = None
    # SCD40
    co2_ppm:   Optional[float] = None
    # MQ-7
    co_ppm:    Optional[float] = None
    # MAX30100
    hr_bpm:    Optional[float] = None
    spo2_pct:  Optional[float] = None
    # ECG
    voltage:   Optional[float] = None
    # TSL2561
    lux:       Optional[float] = None
    zone:      Optional[str]   = None
    # INA219
    voltage_v: Optional[float] = None
    current_ma:Optional[float] = None
    power_mw:  Optional[float] = None

    @validator("timestamp")
    def timestamp_reasonable(cls, v):
        import time
        now = int(time.time())
        if abs(v - now) > 86400 * 7:  # reject if >7 days drift
            raise ValueError("Timestamp too far from current time")
        return v

class IngestEnvelope(BaseModel):
    """Accepts either a bare payload or a signed envelope from encryption_layer."""
    data: Optional[TelemetryPayload] = None
    sig:  Optional[str] = None
    # flat payload fields (bare mode)
    sensor:    Optional[SensorType] = None
    timestamp: Optional[int] = None

# ── Routes ─────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "telemetry-ingest"}

@app.post("/ingest", status_code=202)
async def ingest(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid JSON body")

    # Support both bare payload and signed envelope
    if "data" in body and isinstance(body["data"], dict):
        try:
            payload = TelemetryPayload(**body["data"])
        except Exception as e:
            raise HTTPException(status_code=422, detail=str(e))
        envelope = {"data": payload.dict(), "sig": body.get("sig")}
    else:
        try:
            payload = TelemetryPayload(**body)
        except Exception as e:
            raise HTTPException(status_code=422, detail=str(e))
        envelope = {"data": payload.dict(), "sig": None}

    # Forward to Kafka
    try:
        producer.produce(
            VALIDATED_TOPIC,
            key=payload.sensor.encode(),
            value=json.dumps(envelope).encode(),
        )
        producer.poll(0)
    except KafkaException as e:
        log.error("Kafka produce error: %s", e)
        raise HTTPException(status_code=503, detail="Kafka unavailable")

    log.info("Accepted %s @ ts=%d", payload.sensor, payload.timestamp)
    return {"accepted": True, "sensor": payload.sensor, "timestamp": payload.timestamp}
