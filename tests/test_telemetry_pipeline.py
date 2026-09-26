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
                                  rec("bms", "battery_pct", 84, node="node-compute")])
    assert out["node-rpi-01"]["co2"]["value"] == 640
    assert out["node-rpi-01"]["o2"]["unit"] == "percent"
    assert out["node-compute"]["battery_level"]["value"] == 84


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


# ── node health (sysmon_driver.py; Raspberry Pi 5 compute node) ────

def sysmon(**kw):
    return {"sensor": "sysmon", "cpu_temp": 51.9, "cpu_load": 14.8, "mem_pct": 35.0, "disk_pct": 22.0,
            "fan_rpm": 2579, "supply_v": 5.1, "power_w": 6.86, "undervolt": 0, "throttled": 0,
            "undervolt_boot": 0, "timestamp": NOW, "node_id": "node-compute", "zone": "compute", **kw}


def test_sysmon_reading_from_a_pi5_validates():
    out = normalise(sysmon(), "habitat/sensors/sysmon/compute")
    assert out["power_w"] == 6.86 and out["undervolt"] == 0 and out["fan_rpm"] == 2579


def test_sysmon_flags_must_be_0_or_1():
    with pytest.raises(InvalidTelemetry):
        normalise(sysmon(undervolt=5), "habitat/sensors/sysmon/compute")


def test_sysmon_on_a_pi4_without_pmic_metrics_validates():
    msg = {"sensor": "sysmon", "cpu_temp": 47.0, "cpu_load": 5.0, "timestamp": NOW}
    assert normalise(msg, "habitat/sensors/sysmon/zone_a")["zone"] == "zone_a"


def test_merge_maps_compute_node_health():
    out = merge_pipeline_records([rec("sysmon", "power_w", 6.9, node="node-compute"),
                                  rec("sysmon", "cpu_temp", 52.0, node="node-compute"),
                                  rec("sysmon", "undervolt", 1, node="node-rpi-02")])
    assert out["node-compute"]["power_draw"] == {**out["node-compute"]["power_draw"], "value": 6.9, "unit": "watts"}
    assert out["node-compute"]["cpu_temp"]["value"] == 52.0
    assert out["node-rpi-02"]["undervoltage"]["value"] == 1


# ── realtime / high-rate data ──────────────────────────────────────

def test_subsecond_timestamps_survive_validation():
    """ECG at 100 Hz: samples in the same second must keep distinct timestamps."""
    t = NOW + 0.01
    out = normalise({"sensor": "ecg_ad8232", "voltage": 1.62, "timestamp": t}, "habitat/sensors/ecg_ad8232/zone_a")
    assert out["timestamp"] == round(t, 3) and out["timestamp"] != int(t)
    assert isinstance(normalise(bme(), "habitat/sensors/bme280/zone_a")["timestamp"], int)


def test_processor_keeps_milliseconds_and_skips_ecg_zscore(monkeypatch):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent / "services"))   # as in its container
    from services import telemetry_processor as tp
    written = []
    monkeypatch.setattr(tp.write_api, "write", lambda bucket, record: written.append((bucket, record)))
    base = float(int(time.time()))
    for i in range(40):   # spiky waveform: would trip a 3-sigma detector
        v = 3.0 if i % 10 == 0 else 1.5
        tp.process_message(json.dumps({"sensor": "ecg_ad8232", "voltage": v, "timestamp": base + i * 0.01,
                                        "zone": "zone_a", "node_id": "node-rpi-01"}).encode())
    lines = [rec.to_line_protocol() for bucket, rec in written]
    assert all(b == "habitat_sensors" for b, _ in written) and len(lines) == 40
    stamps = {l.rsplit(" ", 1)[1] for l in lines}
    assert len(stamps) == 40                                   # no two samples collapse onto one point


def test_sensor_snapshot_groups_every_metric():
    from services.telemetry_api import sensor_snapshot
    recs = [
        {"_measurement": "ina219", "metric": "voltage_v", "value": 12.1, "node_id": "node-rpi-01", "zone": "zone_a",
         "simulated": "false", "timestamp": "2026-09-26T10:00:01+00:00"},
        {"_measurement": "ina219", "metric": "current_ma", "value": 850, "node_id": "node-rpi-01", "zone": "zone_a",
         "simulated": "false", "timestamp": "2026-09-26T10:00:02+00:00"},
        {"_measurement": "sysmon", "metric": "cpu_temp", "value": 51, "node_id": "node-compute", "zone": "compute",
         "simulated": "true", "timestamp": "2026-09-26T10:00:00+00:00", "crew_id": "-"},
    ]
    snap = sensor_snapshot(recs)
    ina = next(s for s in snap if s["sensor"] == "ina219")
    assert ina["metrics"] == {"voltage_v": 12.1, "current_ma": 850} and ina["simulated"] is False
    assert ina["timestamp"] == "2026-09-26T10:00:02+00:00"
    assert next(s for s in snap if s["sensor"] == "sysmon")["crew_id"] is None


# ── ESP32 sensor board: BNO055 IMU and MQ-4 methane (esp32_bridge.py) ──

def test_bno055_and_mq4_readings_validate():
    imu = {"sensor": "bno055", "heading_deg": 182.3, "roll_deg": -5.0, "pitch_deg": 2.0, "lin_acc_ms2": 0.04,
           "imu_calib": 3, "timestamp": NOW}
    assert normalise(imu, "habitat/sensors/bno055/zone_a")["imu_calib"] == 3
    gas = {"sensor": "mq4", "vout_mv": 930.0, "rs_r0": 4.4, "ch4_ppm": 16.5, "timestamp": NOW}
    assert normalise(gas, "habitat/sensors/mq4/zone_a")["ch4_ppm"] == 16.5
    # before warm-up/calibration the board sends only the voltage
    assert "ch4_ppm" not in normalise({"sensor": "mq4", "vout_mv": 930.0, "timestamp": NOW}, "habitat/sensors/mq4/zone_a")


@pytest.mark.parametrize("bad", [{"heading_deg": 400}, {"roll_deg": -200}, {"imu_calib": 4}])
def test_bno055_out_of_range_is_rejected(bad):
    with pytest.raises(InvalidTelemetry):
        normalise({"sensor": "bno055", "heading_deg": 10.0, "timestamp": NOW, **bad}, "habitat/sensors/bno055/zone_a")


@pytest.mark.parametrize("bad", [{"ch4_ppm": -1}, {"vout_mv": 9000}])
def test_mq4_out_of_range_is_rejected(bad):
    with pytest.raises(InvalidTelemetry):
        normalise({"sensor": "mq4", "vout_mv": 900.0, "timestamp": NOW, **bad}, "habitat/sensors/mq4/zone_a")


def test_merge_maps_methane():
    out = merge_pipeline_records([rec("mq4", "ch4_ppm", 12.5)])
    assert out["node-rpi-01"]["methane"]["value"] == 12.5 and out["node-rpi-01"]["methane"]["unit"] == "ppm"


def test_methane_alarm_and_no_imu_zscore(monkeypatch):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent / "services"))
    from services import telemetry_processor as tp
    monkeypatch.setattr(tp.write_api, "write", lambda bucket, record: written.append(bucket))
    alarms, written = [], []

    class FakeConn:
        async def execute(self, sql, *args):
            alarms.append(args[:3])

        async def close(self):
            pass

    async def connect(**kw):
        return FakeConn()
    monkeypatch.setattr(tp.asyncpg, "connect", connect)
    base = float(int(time.time()))
    for i in range(31):   # steady, then someone turns the board: a 30-sigma jump that is not an anomaly
        tp.process_message(json.dumps({"sensor": "bno055", "heading_deg": 180.0 if i == 30 else 10.0 + (i % 2) * 0.5,
                                       "timestamp": base + i, "zone": "zone_x", "node_id": "node-rpi-01"}).encode())
    assert "habitat_alerts" not in written
    for ppm in (800.0, 6200.0):
        tp.process_message(json.dumps({"sensor": "mq4", "ch4_ppm": ppm, "timestamp": base + 50,
                                       "zone": "zone_a", "node_id": "node-rpi-01"}).encode())
    assert alarms == [("mq4", "ch4_ppm", 6200.0)]
