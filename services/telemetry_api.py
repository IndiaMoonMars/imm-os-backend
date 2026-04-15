"""
IMM-OS Backend — Telemetry API Router
Provides REST endpoints that query InfluxDB for live + historical sensor data.

Topics produced by the sensor simulator / real sensors:
  imm/habitat/{node_id}/telemetry/{measurement}

These routes are mounted at /api so nginx proxies /api/ → this service.
"""

import os
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError

log = logging.getLogger("telemetry_api")

# ── InfluxDB connection (injected via env vars) ───────────────────────────────

INFLUX_URL    = os.getenv("INFLUX_URL",    "http://localhost:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "imm-super-secret-token")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "imm_org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "telemetry")

router = APIRouter(prefix="/api/telemetry", tags=["Telemetry"])

# Known node IDs (matches simulator config — real sensors use same IDs)
KNOWN_NODES = [
    {"id": "node-rpi-01",  "type": "rpi",    "zone": "Habitat Zone A"},
    {"id": "node-rpi-02",  "type": "rpi",    "zone": "Habitat Zone B"},
    {"id": "node-jetson",  "type": "jetson", "zone": "Compute / Power"},
]

MEASUREMENTS_BY_TYPE = {
    "rpi":    ["temperature", "humidity", "pressure", "co2", "o2"],
    "jetson": ["cpu_temp", "gpu_temp", "power_draw", "battery_level", "solar_input"],
}


def _get_client() -> InfluxDBClient:
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/nodes")
async def list_nodes():
    """List all known sensor nodes (static registry).
    
    When real sensors arrive, add their node IDs here — no other changes needed.
    """
    return {"nodes": KNOWN_NODES}


@router.get("/latest")
async def get_latest():
    """Return the most recent reading for every measurement across all nodes.
    
    This is the primary endpoint used by the mission dashboard.
    Falls back to mock data if InfluxDB is unreachable (dev convenience).
    """
    try:
        return _query_latest_from_influx()
    except Exception as exc:
        log.warning("InfluxDB unreachable, returning mock data: %s", exc)
        return _mock_latest()


@router.get("/{node_id}/latest")
async def get_node_latest(node_id: str):
    """Return the most recent readings for a single node."""
    node = next((n for n in KNOWN_NODES if n["id"] == node_id), None)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")

    try:
        data = _query_latest_from_influx(node_id=node_id)
        return {"node": node, "readings": data.get("readings", {})}
    except Exception as exc:
        log.warning("InfluxDB unreachable for %s, returning mock: %s", node_id, exc)
        mock = _mock_latest()
        readings = mock["readings"].get(node_id, {})
        return {"node": node, "readings": readings}


@router.get("/{node_id}/history")
async def get_node_history(
    node_id: str,
    measurement: str = Query(..., description="e.g. temperature"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Return last N readings for a specific measurement on a node."""
    node = next((n for n in KNOWN_NODES if n["id"] == node_id), None)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")

    try:
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -24h)
          |> filter(fn: (r) => r["node_id"] == "{node_id}")
          |> filter(fn: (r) => r["_measurement"] == "{measurement}")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: {limit})
        """
        with _get_client() as client:
            tables = client.query_api().query(query)
            results = []
            for table in tables:
                for record in table.records:
                    results.append({
                        "timestamp": record.get_time().isoformat(),
                        "value": record.get_value(),
                        "unit": record.values.get("unit", ""),
                        "simulated": record.values.get("simulated", True),
                    })
            return {"node_id": node_id, "measurement": measurement, "data": results}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"InfluxDB unavailable: {exc}")


# ── Internal helpers ──────────────────────────────────────────────────────────

def _query_latest_from_influx(node_id: Optional[str] = None) -> dict:
    """Query InfluxDB for the latest reading per field per node."""
    node_filter = f'|> filter(fn: (r) => r["node_id"] == "{node_id}")' if node_id else ""
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -5m)
      {node_filter}
      |> last()
      |> group(columns: ["node_id", "_measurement"])
    """
    readings: dict = {}
    with _get_client() as client:
        tables = client.query_api().query(query)
        for table in tables:
            for record in table.records:
                nid = record.values.get("node_id", "unknown")
                meas = record.get_measurement()
                readings.setdefault(nid, {})[meas] = {
                    "value": record.get_value(),
                    "unit": record.values.get("unit", ""),
                    "timestamp": record.get_time().isoformat(),
                    "simulated": record.values.get("simulated", True),
                }
    return {"readings": readings}


def _mock_latest() -> dict:
    """Fallback mock data when InfluxDB is not yet running (local dev)."""
    import time, random, math
    t = time.time()
    def sw(base, amp, period): return round(base + amp * math.sin(2 * math.pi * t / period), 2)
    return {
        "readings": {
            "node-rpi-01": {
                "temperature": {"value": sw(22, 2, 3600), "unit": "celsius",  "simulated": True},
                "humidity":    {"value": sw(55, 5, 7200), "unit": "percent",  "simulated": True},
                "pressure":    {"value": sw(1013, 1.5, 5400), "unit": "hPa", "simulated": True},
                "co2":         {"value": round(400 + random.uniform(-5, 5), 1), "unit": "ppm", "simulated": True},
                "o2":          {"value": round(20.9 + random.uniform(-0.05, 0.05), 2), "unit": "percent", "simulated": True},
            },
            "node-rpi-02": {
                "temperature": {"value": sw(23, 2, 3600), "unit": "celsius",  "simulated": True},
                "humidity":    {"value": sw(52, 5, 7200), "unit": "percent",  "simulated": True},
                "pressure":    {"value": sw(1012, 1.5, 5400), "unit": "hPa", "simulated": True},
                "co2":         {"value": round(420 + random.uniform(-5, 5), 1), "unit": "ppm", "simulated": True},
                "o2":          {"value": round(20.8 + random.uniform(-0.05, 0.05), 2), "unit": "percent", "simulated": True},
            },
            "node-jetson": {
                "cpu_temp":     {"value": sw(55, 5, 1800), "unit": "celsius", "simulated": True},
                "gpu_temp":     {"value": sw(60, 8, 1800), "unit": "celsius", "simulated": True},
                "power_draw":   {"value": round(12 + random.uniform(-1, 1), 1), "unit": "watts", "simulated": True},
                "battery_level":{"value": round(85 + random.uniform(-0.5, 0.5), 1), "unit": "percent", "simulated": True},
                "solar_input":  {"value": max(0, sw(8, 8, 86400)), "unit": "watts", "simulated": True},
            },
        },
        "_meta": {"source": "mock", "reason": "InfluxDB not reachable"}
    }
