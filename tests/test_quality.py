"""Telemetry quality flags (services/quality.py) and how normalise() applies them."""
import time

import pytest

from services.quality import BAD, GOOD, SUSPECT, assess, level_for, worst
from services.telemetry_schema import InvalidTelemetry, normalise

NOW = time.time()


def test_levels_and_worst():
    assert worst([GOOD, SUSPECT, GOOD]) == SUSPECT
    assert worst([SUSPECT, BAD]) == BAD
    assert level_for([]) == GOOD and level_for(["delayed", "seq_gap"]) == GOOD
    assert level_for(["uncalibrated"]) == SUSPECT and level_for(["warming", "source_fault"]) == BAD
    assert level_for(["made_up"]) == SUSPECT      # unknown reasons never pass as good


def test_good_reading():
    out = normalise({"sensor": "bme280", "temp": 22.1, "hum": 45, "pres": 1008, "timestamp": NOW})
    assert (out["q"], out["qf"], out["delayed"]) == (GOOD, [], False)


def test_sensor_state_fields_become_flags():
    mq4 = normalise({"sensor": "mq4", "vout_mv": 1080, "warming": 1, "calibrated": 0, "timestamp": NOW})
    assert mq4["q"] == SUSPECT and set(mq4["qf"]) == {"warming", "uncalibrated"}
    o2 = normalise({"sensor": "o2", "o2_pct": 18.3, "calibrated": 0, "timestamp": NOW})
    assert o2["q"] == SUSPECT and o2["qf"] == ["uncalibrated"]       # the real board's O₂ before CAL_O2
    assert normalise({"sensor": "o2", "o2_pct": 20.9, "calibrated": 1, "timestamp": NOW})["q"] == GOOD


def test_soft_range_is_suspect_hard_range_is_rejected():
    out = normalise({"sensor": "scd40", "co2_ppm": 250, "timestamp": NOW})
    assert out["q"] == SUSPECT and out["qf"] == ["soft_range"]
    for bad in ({"co2_ppm": -5}, {"hum": 140}, {"o2_pct": 120}):
        with pytest.raises(InvalidTelemetry):
            normalise({"sensor": "scd40" if "o2_pct" not in bad else "o2", "timestamp": NOW, **bad})


def test_late_and_future_timestamps():
    late = normalise({"sensor": "bme280", "temp": 22, "timestamp": NOW - 600})
    assert late["delayed"] is True and late["q"] == GOOD and "delayed" in late["qf"]
    replay = normalise({"sensor": "bme280", "temp": 22, "timestamp": NOW, "delayed": True})
    assert replay["delayed"] is True
    future = normalise({"sensor": "bme280", "temp": 22, "timestamp": NOW + 120})
    assert future["q"] == SUSPECT and "clock" in future["qf"]


def test_edge_declared_quality_is_kept_and_never_improved():
    out = normalise({"sensor": "bme280", "temp": 22, "timestamp": NOW, "q": "bad", "qf": ["source_fault"]})
    assert out["q"] == BAD and out["qf"] == ["source_fault"]
    # a node can't claim good while its own flags say otherwise
    assert normalise({"sensor": "mq4", "vout_mv": 900, "warming": 1, "q": "good", "timestamp": NOW})["q"] == SUSPECT
    # unknown flags from a node are dropped rather than trusted
    assert assess({"sensor": "bme280", "qf": ["nonsense", "sensor_reset"], "timestamp": NOW}, ())["qf"] == ["sensor_reset"]


def test_integrity_fields_pass_through():
    out = normalise({"sensor": "bme280", "temp": 22, "timestamp": NOW, "seq": 41, "run": "a1b2c3"})
    assert out["seq"] == 41 and out["run"] == "a1b2c3"
    with pytest.raises(InvalidTelemetry):
        normalise({"sensor": "bme280", "temp": 22, "timestamp": NOW, "seq": -1})
    with pytest.raises(InvalidTelemetry):
        normalise({"sensor": "bme280", "temp": 22, "timestamp": NOW, "q": "excellent"})


def test_board_health_and_node_service_metrics():
    board = normalise({"sensor": "board", "uptime_s": 1200, "reset_reason": 9, "boot_count": 7, "i2c_err": 3,
                       "bme_resets": 51, "timestamp": NOW}, "habitat/sensors/board/zone1")
    assert board["reset_reason"] == 9 and board["q"] == GOOD
    sysmon = normalise({"sensor": "sysmon", "cpu_temp": 50, "svc_failed": 0, "svc_restarts": 2, "mcc_link": 1,
                        "mqtt_backlog": 0, "timestamp": NOW})
    assert sysmon["svc_restarts"] == 2 and sysmon["mcc_link"] == 1
