#!/usr/bin/env python3
"""
IMM-OS Telemetry API Server
- POST /ingest  — validates JSON schema, forwards to Kafka
- GET /history  — queries InfluxDB for historical sensor data
- WS /realtime  — Kafka consumer broadcasting live frames to OpenMCT
- POST /commands — Inserts commands into postgres
"""

import os
import re
import json
import logging
import asyncio
import threading
from typing import Optional, List
from datetime import datetime, timezone, timedelta

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from influxdb_client import InfluxDBClient
import asyncpg

from services.auth import IMM_ROLES, User, current_user, edge_device, user_from_token
from services.telemetry_schema import SENSOR_METRICS, InvalidTelemetry, normalise

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

# ── Sensor schema: shared with the validator (services/telemetry_schema.py) ──

class CommandReq(BaseModel):
    command_text: str
    operator_id: Optional[str] = None  # ignored; the operator is the logged-in user

# ── Live Broadcast WebSockets ──────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    def register(self, websocket: WebSocket):
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

@app.post("/ingest", status_code=202, dependencies=[Depends(edge_device)])
async def ingest(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid JSON body")

    try:
        reading = normalise(body)
    except InvalidTelemetry as e:
        raise HTTPException(status_code=422, detail=str(e))

    envelope = {"data": reading, "sig": reading.pop("sig", None)}

    try:
        producer.produce(
            VALIDATED_TOPIC,
            key=reading["sensor"].encode(),
            value=json.dumps(envelope).encode(),
        )
        producer.poll(0)
    except KafkaException as e:
        log.error("Kafka produce error: %s", e)
        raise HTTPException(status_code=503, detail="Kafka unavailable")

    return {"accepted": True, "sensor": reading["sensor"]}

@app.get("/history", dependencies=[Depends(current_user)])
def get_history(start: int, end: int, sensor: str, metric: str, zone: str = None):
    """History for one sensor metric; start/end are Unix seconds."""
    # values go into the Flux query text, so only accept known names
    if metric not in SENSOR_METRICS.get(sensor, []):
        raise HTTPException(status_code=422, detail=f"Unknown sensor metric {sensor}.{metric}")
    if zone is not None and not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", zone):
        raise HTTPException(status_code=422, detail="Invalid zone")
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

@app.get("/alerts", dependencies=[Depends(current_user)])
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
async def log_command(req: CommandReq, user: User = Depends(current_user)):
    try:
        conn = await asyncpg.connect(
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "changeme"),
            database=os.getenv("POSTGRES_DB", "imm_db"),
            host=os.getenv("POSTGRES_HOST", "postgres")
        )
        await conn.execute(
            "INSERT INTO command_history (operator_id, command_text) VALUES ($1, $2)",
            user.username, req.command_text
        )
        await conn.close()
        log.info(f"Command stored: {user.username} -> {req.command_text}")
        return {"logged": True}
    except Exception as e:
        log.error(f"Postgres insert failed: {e}")
        raise HTTPException(status_code=500, detail="Database failure")

WS_AUTH_TIMEOUT_S = 5


async def authenticate_websocket(websocket: WebSocket) -> Optional[User]:
    """
    Browsers can't set headers on WebSocket requests, and tokens in URLs end up
    in proxy logs, so the client sends {"type": "auth", "token": "<jwt>"} as its
    first message. Returns the user, or None after closing the socket (4401).
    """
    try:
        msg = json.loads(await asyncio.wait_for(websocket.receive_text(), WS_AUTH_TIMEOUT_S))
        if msg.get("type") != "auth":
            raise ValueError("first message must be auth")
        user = user_from_token(str(msg.get("token", "")))
        if not user.roles & IMM_ROLES:
            raise ValueError("no IMM-OS role")
        return user
    except WebSocketDisconnect:
        return None
    except (asyncio.TimeoutError, ValueError, HTTPException, AttributeError, TypeError):
        await websocket.close(code=4401, reason="Unauthorized")
        return None


@app.websocket("/realtime")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    user = await authenticate_websocket(websocket)
    if user is None:
        return
    manager.register(websocket)
    try:
        while True:
            # heartbeat or client-sent filters
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
