"""
IMM-OS Backend — Telemetry API Router
Provides REST endpoints that query InfluxDB for live + historical sensor data.

Readings come from the telemetry pipeline (edge drivers and the simulator publish on
habitat/sensors/<sensor>/<zone> → Kafka → validator → processor → InfluxDB
"habitat_sensors"). Sensor metrics are mapped to dashboard measurement names
(bme280.temp → temperature, …) by services/telemetry_schema.py.

The older per-node topic format (imm/habitat/{node_id}/telemetry/{measurement},
written by telemetry_worker into the "telemetry" bucket) is still read as a
fallback, so nodes using the real-sensors/ templates keep showing up.

These routes are mounted at /api so nginx proxies /api/ → this service.
"""

import os
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError

from services.auth import current_user
from services.telemetry_schema import DASHBOARD_MEASUREMENTS, SENSOR_PRIORITY

log = logging.getLogger("telemetry_api")

# ── InfluxDB connection (injected via env vars) ───────────────────────────────

INFLUX_URL    = os.getenv("INFLUX_URL",    "http://localhost:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "imm-super-secret-token")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "imm_org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "telemetry")                      # legacy per-node bucket
PIPELINE_BUCKET = os.getenv("INFLUX_PIPELINE_BUCKET", "habitat_sensors")   # telemetry-processor output

router = APIRouter(prefix="/api/telemetry", tags=["Telemetry"], dependencies=[Depends(current_user)])

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

    if measurement not in KNOWN_MEASUREMENTS:
        raise HTTPException(status_code=422, detail=f"Unknown measurement '{measurement}'")
    try:
        with _get_client() as client:
            results = _history_from_pipeline(client, node_id, measurement, limit) \
                or _history_from_legacy(client, node_id, measurement, limit)
        return {"node_id": node_id, "measurement": measurement, "data": results}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"InfluxDB unavailable: {exc}")


# ── Internal helpers ──────────────────────────────────────────────────────────

KNOWN_MEASUREMENTS = {m for m, _ in DASHBOARD_MEASUREMENTS.values()} | {
    m for ms in MEASUREMENTS_BY_TYPE.values() for m in ms}


def _as_bool(v) -> bool:
    return v is True or str(v).lower() == "true"


def merge_pipeline_records(records: list) -> dict:
    """
    Turn latest pipeline points into {node_id: {measurement: reading}}.

    Each record is a dict with the point's tags (``_measurement`` = sensor, ``metric``,
    ``node_id``, ``simulated``) plus ``value`` and ``timestamp``. When several sensors on
    a node report the same measurement, SENSOR_PRIORITY decides which one is shown.
    """
    out: dict = {}
    rank: dict = {}
    for r in records:
        mapped = DASHBOARD_MEASUREMENTS.get((r.get("_measurement"), r.get("metric")))
        if not mapped:
            continue
        name, unit = mapped
        node = r.get("node_id") or "unknown"
        sensor = r["_measurement"]
        pr = SENSOR_PRIORITY.index(sensor) if sensor in SENSOR_PRIORITY else len(SENSOR_PRIORITY)
        if (node, name) in rank and rank[(node, name)] <= pr:
            continue
        rank[(node, name)] = pr
        out.setdefault(node, {})[name] = {
            "value": r["value"], "unit": unit, "timestamp": r.get("timestamp"),
            "simulated": _as_bool(r.get("simulated")), "sensor": sensor, "zone": r.get("zone"),
        }
    return out


def _records(tables) -> list:
    rows = []
    for table in tables:
        for record in table.records:
            row = {k: v for k, v in record.values.items() if not k.startswith("_") or k == "_measurement"}
            row["value"] = record.get_value()
            row["timestamp"] = record.get_time().isoformat()
            rows.append(row)
    return rows


def _latest_from_pipeline(client, node_id: Optional[str] = None) -> dict:
    node_filter = f'|> filter(fn: (r) => r["node_id"] == "{node_id}")' if node_id else ""
    query = f"""
    from(bucket: "{PIPELINE_BUCKET}")
      |> range(start: -5m)
      |> filter(fn: (r) => r["_field"] == "value")
      {node_filter}
      |> last()
    """
    return merge_pipeline_records(_records(client.query_api().query(query)))


def _latest_from_legacy(client, node_id: Optional[str] = None) -> dict:
    node_filter = f'|> filter(fn: (r) => r["node_id"] == "{node_id}")' if node_id else ""
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -5m)
      {node_filter}
      |> last()
      |> group(columns: ["node_id", "_measurement"])
    """
    readings: dict = {}
    for table in client.query_api().query(query):
        for record in table.records:
            nid = record.values.get("node_id", "unknown")
            readings.setdefault(nid, {})[record.get_measurement()] = {
                "value": record.get_value(),
                "unit": record.values.get("unit", ""),
                "timestamp": record.get_time().isoformat(),
                "simulated": _as_bool(record.values.get("simulated", True)),
            }
    return readings


def _query_latest_from_influx(node_id: Optional[str] = None) -> dict:
    """Latest reading per measurement per node; pipeline data wins over legacy data."""
    with _get_client() as client:
        readings = {}
        for source in (_latest_from_legacy, _latest_from_pipeline):
            try:
                for nid, ms in source(client, node_id).items():
                    readings.setdefault(nid, {}).update(ms)
            except Exception as exc:
                if source is _latest_from_pipeline and not readings:
                    raise
                log.debug("%s unavailable: %s", source.__name__, exc)
    return {"readings": readings}


def _history_from_pipeline(client, node_id: str, measurement: str, limit: int) -> list:
    candidates = sorted((k for k, v in DASHBOARD_MEASUREMENTS.items() if v[0] == measurement),
                        key=lambda k: SENSOR_PRIORITY.index(k[0]) if k[0] in SENSOR_PRIORITY else 99)
    for sensor, metric in candidates:
        unit = DASHBOARD_MEASUREMENTS[(sensor, metric)][1]
        query = f"""
        from(bucket: "{PIPELINE_BUCKET}")
          |> range(start: -24h)
          |> filter(fn: (r) => r["_measurement"] == "{sensor}" and r["metric"] == "{metric}")
          |> filter(fn: (r) => r["node_id"] == "{node_id}" and r["_field"] == "value")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: {limit})
        """
        rows = _records(client.query_api().query(query))
        if rows:
            return [{"timestamp": r["timestamp"], "value": r["value"], "unit": unit,
                     "simulated": _as_bool(r.get("simulated"))} for r in rows]
    return []


def _history_from_legacy(client, node_id: str, measurement: str, limit: int) -> list:
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r["node_id"] == "{node_id}")
      |> filter(fn: (r) => r["_measurement"] == "{measurement}")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: {limit})
    """
    return [{"timestamp": r["timestamp"], "value": r["value"], "unit": r.get("unit", ""),
             "simulated": _as_bool(r.get("simulated", True))}
            for r in _records(client.query_api().query(query))]


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
