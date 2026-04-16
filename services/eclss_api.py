from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [eclss_api] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="IMM-OS ECLSS API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])

# In-memory transient state for demo (would normally be bound to Edge over MQTT or direct Redis)
lighting_state = {
    "core": {"brightness": 80, "kelvin": 5000},
    "airlock": {"brightness": 100, "kelvin": 6000},
    "lab": {"brightness": 80, "kelvin": 5000}
}

class LightingUpdate(BaseModel):
    brightness: int
    kelvin: int

class WasteEvent(BaseModel):
    weight_kg: float
    rfid_tag: str
    container: str

class ShowerEvent(BaseModel):
    duration_seconds: float
    estimated_liters: float

class FlowEvent(BaseModel):
    event_ml: float
    daily_total_ml: float
    source: str

class BiolabEvent(BaseModel):
    ph_level: float
    water_temp_c: float

@app.get("/api/v1/eclss/lighting")
def get_lighting_state():
    return lighting_state

@app.put("/api/v1/eclss/lighting/{zone}")
def set_lighting_state(zone: str, state: LightingUpdate):
    if zone not in lighting_state and zone != "all":
        # register new dynamic zone
        lighting_state[zone] = {"brightness": state.brightness, "kelvin": state.kelvin}
        
    if zone == "all":
        for z in lighting_state:
            lighting_state[z] = {"brightness": state.brightness, "kelvin": state.kelvin}
    else:
        lighting_state[zone] = {"brightness": state.brightness, "kelvin": state.kelvin}
        
    log.info(f"Lighting override {zone} -> {state}")
    # In a full deployment, this publishes `MQTT_PUB` to `habitat/control/lighting/{zone}` to actively sync the Edge Pi.
    return {"status": "updated", "state": lighting_state}

@app.post("/api/v1/waste/log")
def log_waste(event: WasteEvent):
    log.info(f"Waste Event Logged: {event.weight_kg}kg [{event.rfid_tag}]")
    return {"status": "logged"}

@app.post("/api/v1/water/shower")
def log_shower(event: ShowerEvent):
    log.info(f"Shower Event Logged: {event.duration_seconds}s used {event.estimated_liters}L")
    return {"status": "logged"}

@app.post("/api/v1/water/log")
def log_flow(event: FlowEvent):
    log.info(f"Water Flow Event Logged: {event.event_ml}mL (Daily: {event.daily_total_ml}mL)")
    return {"status": "logged"}

@app.post("/api/v1/biolab/log")
def log_biolab(event: BiolabEvent):
    log.info(f"Biolab Event Logged: pH {event.ph_level}, Temp {event.water_temp_c}°C")
    return {"status": "logged"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
