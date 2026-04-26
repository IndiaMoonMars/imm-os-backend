from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import json
import logging
import asyncpg
from datetime import datetime, timezone, timedelta
from influxdb_client import InfluxDBClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [astra] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="IMM-OS Mission Assistant (Astra)", version="1.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])

# ── Config ──────────────────────────────────────────────────────────
PG_URL      = os.getenv("DATABASE_URL", "postgresql://imm_user:imm_pass@postgres:5432/imm_db")
INFLUX_URL   = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "imm-super-secret-token")
INFLUX_ORG   = os.getenv("INFLUX_ORG", "imm_org")

influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

class AstraQuery(BaseModel):
    crew_id: str
    query: str

class AstraResponse(BaseModel):
    text: str
    context_used: list[str]
    suggested_actions: list[str]

# ── Tools ───────────────────────────────────────────────────────────

async def get_mission_state():
    """Fetches high-level mission context from Postgres."""
    conn = await asyncpg.connect(PG_URL)
    try:
        # Get active tasks
        tasks = await conn.fetch("""
            SELECT title, status, deadline FROM tasks 
            WHERE status NOT IN ('COMPLETE', 'CANCELLED') 
            ORDER BY deadline ASC LIMIT 5
        """)
        # Get latest AI insights
        insights = await conn.fetch("""
            SELECT system_area, summary, severity FROM ai_insights 
            ORDER BY created_at DESC LIMIT 3
        """)
        return {
            "tasks": [dict(t) for t in tasks],
            "insights": [dict(i) for i in insights]
        }
    finally:
        await conn.close()

def get_telemetry_trends():
    """Fetches recent telemetry trends from InfluxDB."""
    query_api = influx.query_api()
    # Simple query for CO2 and Temperature trends in the last 30m
    flux = f'''
        from(bucket: "habitat_sensors")
            |> range(start: -30m)
            |> filter(fn: (r) => r["_field"] == "value")
            |> filter(fn: (r) => r["_measurement"] == "scd40" or r["_measurement"] == "bme280")
            |> mean()
    '''
    tables = query_api.query(flux)
    res = {}
    for table in tables:
        for record in table.records:
            res[f"{record.get_measurement()}.{record.values.get('metric', 'value')}"] = record.get_value()
    return res

# ── Response Logic ──────────────────────────────────────────────────

def generate_astra_response(query_text: str, state: dict, trends: dict) -> AstraResponse:
    """Synthesizes a response using mission context and telemetry."""
    query_lower = query_text.lower()
    
    # ── Logic for common queries (Simulated LLM Reasoning) ──────────
    
    if "status" in query_lower or "how are we" in query_lower:
        co2 = trends.get("scd40.co2_ppm", 0)
        temp = trends.get("scd40.temp", 0)
        
        text = f"Habitat systems are nominal. Average CO2 is holding at {co2:.1f} ppm and temperature is {temp:.1f}°C."
        if any(i['severity'] == 'warning' for i in state['insights']):
            text += " However, AI anomalies have been detected in the ECLSS system. Suggest checking the AI Dashboard."
        
        context = ["influxdb.habitat_sensors", "postgres.ai_insights"]
        actions = ["Review Anomaly Heatmap", "Run HVAC Diagnostic"]
        
    elif "task" in query_lower or "todo" in query_lower:
        next_task = state['tasks'][0]['title'] if state['tasks'] else "none"
        text = f"You have {len(state['tasks'])} pending tasks. Your immediate priority is: '{next_task}'."
        context = ["postgres.tasks"]
        actions = ["Open Scheduling Dashboard", "Update Task Status"]
        
    else:
        # Fallback "General Support" response
        text = "I've analyzed the current telemetry and mission logs. Everything looks stable, but I'm monitoring a slight variance in CO2 levels across Zone 1. How else can I assist with the mission today?"
        context = ["influxdb.habitat_sensors", "postgres.tasks"]
        actions = ["View Telemetry Trends", "Search Procedures"]

    return AstraResponse(text=text, context_used=context, suggested_actions=actions)

# ── Endpoints ───────────────────────────────────────────────────────

@app.post("/api/v1/astra/query", response_model=AstraResponse)
async def query_astra(payload: AstraQuery):
    log.info(f"Query from {payload.crew_id}: {payload.query}")
    
    # 1. Gather Context
    state = await get_mission_state()
    trends = get_telemetry_trends()
    
    # 2. Synthesize
    response = generate_astra_response(payload.query, state, trends)
    
    return response

@app.get("/api/v1/astra/health")
def health():
    return {"status": "Astra Core Online"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8009)
