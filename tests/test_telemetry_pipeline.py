"""
Telemetry pipeline: shared schema, raw → validated routing, dashboard mapping.
"""
import json
import time

import pytest

from services.telemetry_schema import InvalidTelemetry, SENSOR_METRICS, SensorType, normalise
from services.telemetry_validator import DEADLETTER_TOPIC, VALIDATED_TOPIC, route
from services.telemetry_api import merge_pipeline_records

NOW = int(time.time())


def bme(**kw):
    return {"sensor": "bme280", "temp": 22.5, "hum": 48.0, "pres": 1012.3, "timestamp": NOW, **kw}


# ── schema ─────────────────────────────────────────────────────────

def test_every_sensor_type_has_metrics():
    assert {s.value for s in SensorType} == set(SENSOR_METRICS)


def test_normalise_bare_reading_takes_zone_from_topic():
    out = normalise(bme(node_id="node-rpi-01"), "habitat/sensors/bme280/zone_a")
    assert out["zone"] == "zone_a"
    assert out["node_id"] == "node-rpi-01"
    assert out["simulated"] is False
    assert out["temp"] == 22.5


def test_normalise_sensor_from_topic_when_missing():
    msg = {"o2_pct": 20.9, "timestamp": NOW}
    out = normalise(msg, "habitat/sensors/o2/zone_b")
    assert out["sensor"] == "o2" and out["zone"] == "zone_b"


def test_normalise_accepts_envelope_and_keeps_sig():
    out = normalise({"data": bme(), "sig": "abc"}, "habitat/sensors/bme280/zone1")
    assert out["sig"] == "abc"


def test_payload_zone_wins_over_topic():
    assert normalise(bme(zone="lab"), "habitat/sensors/bme280/zone1")["zone"] == "lab"


@pytest.mark.parametrize("msg,topic,reason", [
    (bme(), "habitat/sensors/scd40/zone1", "does not match topic"),     # spoofed sensor
    ({"sensor": "toaster", "timestamp": NOW, "temp": 1}, None, "sensor"),
    ({"sensor": "bme280", "timestamp": NOW}, None, "no bme280 metrics"),
    (bme(timestamp=NOW - 30 * 86400), None, "Timestamp too far"),
    (bme(temp="hot"), None, "temp"),
    (["not", "an", "object"], None, "not a JSON object"),
])
def test_normalise_rejects(msg, topic, reason):
    with pytest.raises(InvalidTelemetry, match=reason):
        normalise(msg, topic)


def test_simulated_flag_round_trips():
    assert normalise(bme(simulated=True))["simulated"] is True


# ── validator routing ──────────────────────────────────────────────

def test_route_valid_message_to_validated():
    topic, key, value = route(b"habitat/sensors/bme280/zone_a", json.dumps(bme(node_id="n1")).encode())
    assert topic == VALIDATED_TOPIC
    assert key == b"bme280"
    env = json.loads(value)
    assert env["data"]["zone"] == "zone_a" and env["data"]["node_id"] == "n1"
    assert "sig" not in env["data"]


def test_route_bad_json_to_deadletter():
    topic, key, value = route(b"habitat/sensors/bme280/zone1", b"{not json")
    assert topic == DEADLETTER_TOPIC
    body = json.loads(value)
    assert body["reason"] == "invalid JSON" and body["mqtt_topic"] == "habitat/sensors/bme280/zone1"


def test_route_invalid_reading_to_deadletter_with_reason():
    topic, _, value = route(b"habitat/sensors/scd40/zone1", json.dumps(bme()).encode())
    assert topic == DEADLETTER_TOPIC
    assert "does not match topic" in json.loads(value)["reason"]


def test_route_handles_missing_key():
    topic, _, _ = route(None, json.dumps(bme()).encode())
    assert topic == VALIDATED_TOPIC


def test_validated_envelope_matches_processor_expectations():
    """The processor reads data.sensor/zone/node_id/simulated and one value per metric."""
    _, _, value = route(b"habitat/sensors/scd40/zone1",
                        json.dumps({"sensor": "scd40", "co2_ppm": 612, "timestamp": NOW, "simulated": True}).encode())
    data = json.loads(value)["data"]
    assert data["sensor"] == "scd40" and data["simulated"] is True and data["co2_ppm"] == 612


# ── dashboard mapping ──────────────────────────────────────────────

def rec(sensor, metric, value, node="node-rpi-01", simulated="false"):
    return {"_measurement": sensor, "metric": metric, "value": value, "node_id": node,
            "simulated": simulated, "zone": "zone_a", "timestamp": "2026-09-25T10:00:00+00:00"}


def test_merge_maps_sensor_metrics_to_dashboard_names():
    out = merge_pipeline_records([rec("scd40", "co2_ppm", 640), rec("o2", "o2_pct", 20.9),
                                  rec("bms", "battery_pct", 84, node="node-jetson")])
    assert out["node-rpi-01"]["co2"]["value"] == 640
    assert out["node-rpi-01"]["o2"]["unit"] == "percent"
    assert out["node-jetson"]["battery_level"]["value"] == 84


def test_merge_prefers_bme280_temperature_over_scd40_in_any_order():
    for records in ([rec("scd40", "temp", 25.0), rec("bme280", "temp", 22.0)],
                    [rec("bme280", "temp", 22.0), rec("scd40", "temp", 25.0)]):
        assert merge_pipeline_records(records)["node-rpi-01"]["temperature"]["value"] == 22.0


def test_merge_reports_simulated_flag():
    out = merge_pipeline_records([rec("bme280", "hum", 50, simulated="true"), rec("bme280", "pres", 1000)])
    assert out["node-rpi-01"]["humidity"]["simulated"] is True
    assert out["node-rpi-01"]["pressure"]["simulated"] is False


def test_merge_ignores_metrics_without_dashboard_name():
    assert merge_pipeline_records([rec("ecg_ad8232", "voltage", 1.2)]) == {}


# ── /history input checks (values are interpolated into Flux) ─────

def test_history_rejects_unknown_names_before_querying(monkeypatch):
    from fastapi.testclient import TestClient
    from services import telemetry_ingest
    from services.auth import User, authenticated

    queried = []
    monkeypatch.setattr(telemetry_ingest, "InfluxDBClient", lambda **kw: queried.append(kw) or (_ for _ in ()).throw(RuntimeError))
    telemetry_ingest.app.dependency_overrides[authenticated] = lambda: User("ev1", frozenset({"crew"}))
    try:
        c = TestClient(telemetry_ingest.app)
        assert c.get('/history?start=0&end=1&sensor=bme280") |> drop()&metric=temp').status_code == 422
        assert c.get("/history?start=0&end=1&sensor=bme280&metric=co2_ppm").status_code == 422
        assert c.get('/history?start=0&end=1&sensor=bme280&metric=temp&zone=a"b').status_code == 422
        assert queried == []
    finally:
        telemetry_ingest.app.dependency_overrides.clear()


def test_processors_use_valid_influx_write_precision():
    """WritePrecision.SECONDS doesn't exist; it crashed both processors on their first point."""
    import re
    from pathlib import Path
    from influxdb_client import WritePrecision
    for name in ("telemetry_processor.py", "ai_processor.py"):
        src = (Path(__file__).parent.parent / "services" / name).read_text()
        for attr in re.findall(r"WritePrecision\.(\w+)", src):
            assert hasattr(WritePrecision, attr), f"{name}: WritePrecision.{attr}"


# ── EVA streams (eva.raw) ──────────────────────────────────────────

def test_eva_biosensor_frame_is_validated_with_crew_from_topic():
    msg = {"crew_id": "ev1", "sensor": "eva_biosensor", "hr_bpm": 88.0, "spo2_pct": 97.5,
           "skin_temp_c": 36.4, "ecg_mv": 1650.0, "timestamp": NOW}
    topic, key, value = route(b"habitat/eva/biosensors/ev1", json.dumps(msg).encode())
    assert topic == VALIDATED_TOPIC and key == b"eva_biosensor"
    data = json.loads(value)["data"]
    assert data["crew_id"] == "ev1" and data["zone"] == "eva" and data["skin_temp_c"] == 36.4


def test_eva_biosensor_crew_mismatch_rejected():
    msg = {"crew_id": "ev2", "hr_bpm": 90, "timestamp": NOW}
    assert route(b"habitat/eva/biosensors/ev1", json.dumps(msg).encode())[0] == DEADLETTER_TOPIC


def test_eva_biosensor_sensor_only_on_eva_topic():
    msg = {"sensor": "eva_biosensor", "hr_bpm": 90, "timestamp": NOW}
    assert route(b"habitat/sensors/eva_biosensor/zone1", json.dumps(msg).encode())[0] == DEADLETTER_TOPIC


def test_eva_position_frames_forwarded_for_openmct():
    msg = {"crew_id": "ev1", "mode": "uwb", "x_m": 3.2, "y_m": 4.1, "z_m": 1.2, "quality": 90, "timestamp": NOW}
    topic, key, value = route(b"habitat/eva/position/ev1", json.dumps(msg).encode())
    data = json.loads(value)["data"]
    assert topic == VALIDATED_TOPIC and key == b"eva_position/ev1"
    assert data == {"crew_id": "ev1", "mode": "uwb", "x_m": 3.2, "y_m": 4.1, "z_m": 1.2, "quality": 90.0, "timestamp": NOW}
    assert "sensor" not in data   # OpenMCT's EVA tracker treats sensor-less frames as positions
    bad = dict(msg, mode="gps")   # gps mode without lat/lon
    assert route(b"habitat/eva/position/ev1", json.dumps(bad).encode())[0] == DEADLETTER_TOPIC


def test_raw_gps_and_uwb_are_fusion_inputs_not_telemetry():
    assert route(b"habitat/eva/gps", b'{"crew_id":"ev1","lat":1,"lon":2}') is None
    assert route(b"habitat/eva/uwb", b'{"crew_id":"ev1","x_m":1,"y_m":2}') is None
