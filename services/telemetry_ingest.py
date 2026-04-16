#!/usr/bin/env python3
"""
IMM-OS Telemetry API Server
- POST /ingest  — validates JSON schema, forwards to Kafka
- GET /history  — queries InfluxDB for historical sensor data
- WS /realtime  — Kafka consumer broadcasting live frames to OpenMCT
- POST /commands — Inserts commands into postgres
"""

import os
import json
import logging
import asyncio
import threading
from typing import Optional, List
from enum import Enum
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from influxdb_client import InfluxDBClient
import asyncpg

ist_tz = timezone(timedelta(hours=5, minutes=30))
logging.Formatter.converter = lambda *args: datetime.now(ist_tz).timetuple()
logging.basicConfig(level=logging.INFO, format="[UTC %(created)f] [IST %(asctime)s] [api] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
VALIDATED_TOPIC   = "telemetry.validated"
INFLUX_URL        = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN      = os.getenv("INFLUX_TOKEN", "imm-super-secret-token")
INFLUX_ORG        = os.getenv("INFLUX_ORG", "imm_org")
INFLUX_BUCKET     = "habitat_sensors"

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})

app = FastAPI(title="IMM-OS Telemetry Server", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])

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
    # Data fields
    temp:      Optional[float] = None
    hum:       Optional[float] = None
    pres:      Optional[float] = None
    co2_ppm:   Optional[float] = None
    co_ppm:    Optional[float] = None
    hr_bpm:    Optional[float] = None
    spo2_pct:  Optional[float] = None
    voltage:   Optional[float] = None
    lux:       Optional[float] = None
    zone:      Optional[str]   = None
    voltage_v: Optional[float] = None
    current_ma:Optional[float] = None
    power_mw:  Optional[float] = None

    @validator("timestamp")
    def timestamp_reasonable(cls, v):
        import time
        now = int(time.time())
        if abs(v - now) > 86400 * 7:
            raise ValueError("Timestamp too far from current time")
        return v

class CommandReq(BaseModel):
    operator_id: str
    command_text: str

# ── Live Broadcast WebSockets ──────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except WebSocketDisconnect:
                self.disconnect(connection)

manager = ConnectionManager()

def kafka_ws_consumer_loop(loop):
    """Background thread to natively consume Kafka and pipe to WebSockets."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "openmct_ws_broadcaster",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([VALIDATED_TOPIC])
    log.info("WS Broadcaster attached to Kafka stream.")
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error(): continue
        
        raw_val = msg.value().decode('utf-8')
        # Push to WS loop
        asyncio.run_coroutine_threadsafe(manager.broadcast(raw_val), loop)

@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()
    threading.Thread(target=kafka_ws_consumer_loop, args=(loop,), daemon=True).start()

# ── Routes ─────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ingest", status_code=202)
async def ingest(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid JSON body")

    payload_data = body.get("data", body)
    try:
        payload = TelemetryPayload(**payload_data)
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))
        
    envelope = {"data": payload.dict(), "sig": body.get("sig")}

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

    return {"accepted": True, "sensor": payload.sensor}

@app.get("/history")
def get_history(start: int, end: int, sensor: str, metric: str, zone: str = None):
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()

    # Build Flux Query
    zone_filter = f'|> filter(fn: (r) => r["zone"] == "{zone}")' if zone else ''
    flux = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {start}, stop: {end})
          |> filter(fn: (r) => r["_measurement"] == "{sensor}")
          |> filter(fn: (r) => r["metric"] == "{metric}")
          |> filter(fn: (r) => r["_field"] == "value")
          {zone_filter}
          |> yield(name: "mean")
    '''
    
    results = []
    try:
        tables = query_api.query(flux)
        for table in tables:
            for record in table.records:
                results.append({
                    "timestamp": int(record.get_time().timestamp() * 1000),
                    "value": record.get_value(),
                    "sensor": sensor,
                    "metric": metric
                })
    except Exception as e:
        log.error(f"Influx query failed: {e}")
    finally:
        client.close()

    return results

@app.get("/alerts")
def get_alerts(since: int):
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    flux = f'''
        from(bucket: "{os.getenv("INFLUX_ALERTS_BUCKET", "habitat_alerts")}")
          |> range(start: {since})
          |> filter(fn: (r) => r["_measurement"] == "anomaly")
          |> filter(fn: (r) => r["_field"] == "value")
    '''
    
    results = []
    try:
        tables = query_api.query(flux)
        for table in tables:
            for record in table.records:
                results.append({
                    "timestamp": int(record.get_time().timestamp() * 1000),
                    "sensor": record.values.get("sensor"),
                    "metric": record.values.get("metric"),
                    "value": record.get_value(),
                    "zscore": record.values.get("zscore")
                })
    except Exception as e:
        log.error(f"Alerts query failed: {e}")
    finally:
        client.close()
    return results

@app.post("/commands", status_code=201)
async def log_command(req: CommandReq):
    try:
        conn = await asyncpg.connect(
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "changeme"),
            database=os.getenv("POSTGRES_DB", "imm_db"),
            host=os.getenv("POSTGRES_HOST", "postgres")
        )
        await conn.execute(
            "INSERT INTO command_history (operator_id, command_text) VALUES ($1, $2)",
            req.operator_id, req.command_text
        )
        await conn.close()
        log.info(f"Command stored: {req.operator_id} -> {req.command_text}")
        return {"logged": True}
    except Exception as e:
        log.error(f"Postgres insert failed: {e}")
        raise HTTPException(status_code=500, detail="Database failure")

@app.websocket("/realtime")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # heartbeat or client-sent filters
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
