#!/usr/bin/env python3
"""
ECLSS API — FastAPI microservice on port 8003.
Handles:
  - Lighting zone state (persisted in PostgreSQL, pushed to edge over MQTT)
      GET /api/v1/eclss/lighting
      PUT /api/v1/eclss/lighting/{zone}   ("all" updates every known zone)
  - ECLSS event logging from edge monitors → PostgreSQL
      POST /api/v1/waste/log, /api/v1/water/shower, /api/v1/water/log, /api/v1/biolab/log

Lighting commands are published retained (QoS 1) on
habitat/control/lighting/{zone}; the edge lighting_controller.py --listen
subscribes and drives the LEDs. All zones are re-published on every broker
(re)connect so the edge converges on the stored state after an outage.
"""
import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict

import asyncpg
import paho.mqtt.client as mqtt
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, conint, confloat

from services.auth import current_user, edge_device

logging.basicConfig(level=logging.INFO, format="%(asctime)s [eclss_api] %(message)s")
log = logging.getLogger(__name__)

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
LIGHTING_TOPIC = "habitat/control/lighting/{zone}"

DEFAULT_ZONES = {
    "core":    {"brightness": 80,  "kelvin": 5000},
    "airlock": {"brightness": 100, "kelvin": 6000},
    "lab":     {"brightness": 80,  "kelvin": 5000},
}

app = FastAPI(title="IMM-OS ECLSS API", version="1.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])

# ── DB helpers ─────────────────────────────────────────────────────

async def get_conn():
    return await asyncpg.connect(
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme"),
        database=os.getenv("POSTGRES_DB", "imm_db"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
    )


async def load_lighting_state(conn) -> Dict[str, dict]:
    rows = await conn.fetch("SELECT zone, brightness, kelvin FROM eclss_lighting_state ORDER BY zone")
    return {r["zone"]: {"brightness": r["brightness"], "kelvin": r["kelvin"]} for r in rows}


async def upsert_zone(conn, zone: str, brightness: int, kelvin: int):
    await conn.execute(
        """INSERT INTO eclss_lighting_state (zone, brightness, kelvin, updated_at)
           VALUES ($1, $2, $3, NOW())
           ON CONFLICT (zone) DO UPDATE
           SET brightness = EXCLUDED.brightness, kelvin = EXCLUDED.kelvin, updated_at = NOW()""",
        zone, brightness, kelvin,
    )

# ── MQTT bridge to edge ────────────────────────────────────────────

mqtt_client = None
# Last known state per zone, re-published on every (re)connect
_published_state: Dict[str, dict] = {}


def publish_zone(zone: str, state: dict) -> bool:
    """Publish a retained lighting command. Returns True if handed to the broker."""
    _published_state[zone] = state
    if mqtt_client is None or not mqtt_client.is_connected():
        return False
    payload = json.dumps({**state, "zone": zone, "ts": datetime.now(timezone.utc).isoformat()})
    result = mqtt_client.publish(LIGHTING_TOPIC.format(zone=zone), payload, qos=1, retain=True)
    return result.rc == mqtt.MQTT_ERR_SUCCESS


def _on_connect(client, userdata, flags, rc):
    if rc != 0:
        log.error(f"MQTT connect failed rc={rc}")
        return
    log.info(f"MQTT connected to {MQTT_HOST}:{MQTT_PORT}; syncing {len(_published_state)} zones")
    for zone, state in list(_published_state.items()):
        publish_zone(zone, state)


@app.on_event("startup")
async def startup():
    global mqtt_client
    conn = await get_conn()
    try:
        state = await load_lighting_state(conn)
        if not state:
            for zone, s in DEFAULT_ZONES.items():
                await upsert_zone(conn, zone, s["brightness"], s["kelvin"])
            state = dict(DEFAULT_ZONES)
    finally:
        await conn.close()
    _published_state.update(state)

    mqtt_client = mqtt.Client(client_id="imm-eclss-api", clean_session=True)
    # Broker requires auth (allow_anonymous false); user/topics in imm-os-infra mosquitto/config/acl
    if os.getenv("MQTT_USERNAME"):
        mqtt_client.username_pw_set(os.getenv("MQTT_USERNAME"), os.getenv("MQTT_PASSWORD"))
    if os.getenv("MQTT_TLS_CA"):  # broker TLS listener (8883); verifies cert + hostname
        mqtt_client.tls_set(ca_certs=os.getenv("MQTT_TLS_CA"))
    mqtt_client.on_connect = _on_connect
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
    mqtt_client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()


@app.on_event("shutdown")
async def shutdown():
    if mqtt_client is not None:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

# ── Pydantic Models ───────────────────────────────────────────────

class LightingUpdate(BaseModel):
    brightness: conint(ge=0, le=100)
    kelvin: conint(ge=2000, le=6500)

class WasteEvent(BaseModel):
    weight_kg: confloat(ge=0)
    rfid_tag: str
    container: str

class ShowerEvent(BaseModel):
    duration_seconds: confloat(ge=0)
    estimated_liters: confloat(ge=0)

class FlowEvent(BaseModel):
    event_ml: confloat(ge=0)
    daily_total_ml: confloat(ge=0)
    source: str

class BiolabEvent(BaseModel):
    ph_level: confloat(ge=0, le=14)
    water_temp_c: float

# ── Lighting ──────────────────────────────────────────────────────

@app.get("/api/v1/eclss/lighting", dependencies=[Depends(current_user)])
async def get_lighting_state():
    conn = await get_conn()
    try:
        return await load_lighting_state(conn)
    finally:
        await conn.close()


@app.put("/api/v1/eclss/lighting/{zone}", dependencies=[Depends(current_user)])
async def set_lighting_state(zone: str, state: LightingUpdate):
    new = {"brightness": state.brightness, "kelvin": state.kelvin}
    conn = await get_conn()
    try:
        async with conn.transaction():
            if zone == "all":
                zones = list((await load_lighting_state(conn)).keys())
            else:
                zones = [zone]  # unknown zones are registered on first write
            for z in zones:
                await upsert_zone(conn, z, state.brightness, state.kelvin)
        current = await load_lighting_state(conn)
    finally:
        await conn.close()

    delivered = all([publish_zone(z, new) for z in zones])
    if not delivered:
        log.warning(f"Lighting {zone} stored but MQTT broker unavailable; will sync on reconnect")
    log.info(f"Lighting override {zone} -> {new} (edge_synced={delivered})")
    return {"status": "updated", "edge_synced": delivered, "state": current}

# ── ECLSS event logging ───────────────────────────────────────────

async def _insert(sql: str, *args):
    conn = await get_conn()
    try:
        return await conn.fetchval(sql, *args)
    except Exception as e:
        log.error(f"ECLSS insert failed: {e}")
        raise HTTPException(status_code=500, detail="Database failure")
    finally:
        await conn.close()


@app.post("/api/v1/waste/log", status_code=201, dependencies=[Depends(edge_device)])
async def log_waste(event: WasteEvent):
    row_id = await _insert(
        "INSERT INTO waste_events (weight_kg, rfid_tag, container) VALUES ($1, $2, $3) RETURNING id",
        event.weight_kg, event.rfid_tag, event.container,
    )
    log.info(f"Waste Event Logged: {event.weight_kg}kg [{event.rfid_tag}]")
    return {"status": "logged", "id": row_id}


@app.post("/api/v1/water/shower", status_code=201, dependencies=[Depends(edge_device)])
async def log_shower(event: ShowerEvent):
    row_id = await _insert(
        "INSERT INTO shower_events (duration_seconds, estimated_liters) VALUES ($1, $2) RETURNING id",
        event.duration_seconds, event.estimated_liters,
    )
    log.info(f"Shower Event Logged: {event.duration_seconds}s used {event.estimated_liters}L")
    return {"status": "logged", "id": row_id}


@app.post("/api/v1/water/log", status_code=201, dependencies=[Depends(edge_device)])
async def log_flow(event: FlowEvent):
    row_id = await _insert(
        "INSERT INTO water_flow_events (event_ml, daily_total_ml, source) VALUES ($1, $2, $3) RETURNING id",
        event.event_ml, event.daily_total_ml, event.source,
    )
    log.info(f"Water Flow Event Logged: {event.event_ml}mL (Daily: {event.daily_total_ml}mL)")
    return {"status": "logged", "id": row_id}


@app.post("/api/v1/biolab/log", status_code=201, dependencies=[Depends(edge_device)])
async def log_biolab(event: BiolabEvent):
    row_id = await _insert(
        "INSERT INTO biolab_readings (ph_level, water_temp_c) VALUES ($1, $2) RETURNING id",
        event.ph_level, event.water_temp_c,
    )
    log.info(f"Biolab Event Logged: pH {event.ph_level}, Temp {event.water_temp_c}°C")
    return {"status": "logged", "id": row_id}


@app.get("/health")
def health():
    connected = mqtt_client is not None and mqtt_client.is_connected()
    return {"status": "ok", "service": "eclss-api", "mqtt_connected": connected}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
