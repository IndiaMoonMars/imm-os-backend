"""
IMM-OS telemetry quality: one flag vocabulary shared by the edge, the validator, the
health monitor, the APIs and the UIs (imm-os-docs/telemetry-quality.md).

Every validated reading carries
  q      "good" | "suspect" | "bad"   usable for display, alarms and control?
  qf     reason codes, e.g. ["uncalibrated", "warming"]
  delayed  true when the reading arrived late (store-and-forward backlog, blackbox
           replay): valid data, but not live, so it never raises live alarms.

Rules
  good     use it.
  suspect  show it and store it; it may raise limit alarms, marked "unverified";
           a controller should prefer a good redundant source.
  bad      store it tagged, never use it for alarms or control.
Stream-level states (stale, offline, seq gaps, stuck values) are the health
monitor's (services/health/streams.py); they are not written into readings.

No imports from other IMM-OS modules (used as services.quality and as quality).
"""
import time
from typing import Dict, Iterable, List, Optional, Tuple

GOOD, SUSPECT, BAD = "good", "suspect", "bad"
LEVELS = (GOOD, SUSPECT, BAD)
_ORDER = {GOOD: 0, SUSPECT: 1, BAD: 2}

# reason code → the quality it implies (the worst flag wins)
FLAGS: Dict[str, str] = {
    # declared by the edge (driver, bridge, firmware)
    "uncalibrated": SUSPECT,   # calibration never done (MQ-4 R0, O₂ key, BNO055 system 0)
    "warming": SUSPECT,        # heated sensor not at temperature yet (MQ-4 first 3 min)
    "sensor_reset": SUSPECT,   # the chip reset since its previous reading (supply dip)
    "source_fault": BAD,       # the driver or board says this value is wrong
    "saturated": SUSPECT,      # at the end of the sensor's measuring range
    # added by the validator (one reading at a time)
    "soft_range": SUSPECT,     # physically possible but implausible for a habitat
    "clock": SUSPECT,          # timestamp from the future: the node's clock is off
    "delayed": GOOD,           # arrived late; says nothing about the value itself
    # added by the health monitor (needs history); reported per stream, not stored
    "stuck": SUSPECT,          # the same value far longer than a live sensor allows
    "rate": SUSPECT,           # changed faster than physics allows
    "cross_check": SUSPECT,    # disagrees with another sensor measuring the same air
    "seq_gap": GOOD,           # readings were lost on the way (integrity, not value)
}

# Arrived more than this after its timestamp: not live (store-and-forward, replay)
LATE_S = 60.0
# Timestamp this far in the future: the node's clock is wrong
FUTURE_S = 5.0

# Plausible for a crewed habitat or suit (soft limits). Outside → "soft_range".
# Hard, physically impossible values are rejected by the schema (dead letter).
SOFT_RANGES: Dict[Tuple[str, str], Tuple[float, float]] = {
    ("bme280", "temp"): (0.0, 50.0),
    ("bme280", "hum"): (1.0, 99.0),
    ("bme280", "pres"): (600.0, 1100.0),
    ("scd40", "co2_ppm"): (300.0, 10000.0),
    ("scd40", "temp"): (0.0, 60.0),
    ("scd40", "hum"): (1.0, 99.0),
    ("o2", "o2_pct"): (15.0, 25.0),
    ("mq7", "co_ppm"): (0.0, 1000.0),
    ("mq4", "ch4_ppm"): (0.0, 10000.0),
    ("eva_biosensor", "hr_bpm"): (30.0, 220.0),
    ("eva_biosensor", "spo2_pct"): (70.0, 100.0),
    ("eva_biosensor", "skin_temp_c"): (25.0, 42.0),
    ("max30100", "hr_bpm"): (30.0, 220.0),
    ("max30100", "spo2_pct"): (70.0, 100.0),
    ("sysmon", "cpu_temp"): (0.0, 95.0),
    ("sysmon", "supply_v"): (4.5, 5.6),
    ("bno055", "grav_ms2"): (8.8, 10.8),
    ("bno055", "mag_ut"): (5.0, 150.0),
}

# Sensor-declared state fields → flags (the reading says it itself)
_STATE_FLAGS = (
    ("mq4", "warming", 1, "warming"),
    ("mq4", "calibrated", 0, "uncalibrated"),
    ("o2", "calibrated", 0, "uncalibrated"),
    ("bno055", "imu_calib", 0, "uncalibrated"),
)


def worst(levels: Iterable[str]) -> str:
    out = GOOD
    for lv in levels:
        if _ORDER.get(lv, 0) > _ORDER[out]:
            out = lv
    return out


def level_for(flags: Iterable[str]) -> str:
    return worst(FLAGS.get(f, SUSPECT) for f in flags)


def _clean_flags(raw) -> List[str]:
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, (list, tuple)):
        return []
    out = []
    for f in raw:
        if isinstance(f, str) and f in FLAGS and f not in out:
            out.append(f)
    return out


def assess(reading: dict, metrics: Iterable[str], now: Optional[float] = None) -> dict:
    """
    One reading's quality from what it says about itself and one-reading checks.
    Returns {"q", "qf", "delayed"}; the edge's own q/qf are kept (only made worse).
    """
    now = time.time() if now is None else now
    sensor = reading.get("sensor")
    flags = _clean_flags(reading.get("qf"))
    for s, field, value, flag in _STATE_FLAGS:
        if sensor == s and reading.get(field) == value and flag not in flags:
            flags.append(flag)
    for metric in metrics:
        lim = SOFT_RANGES.get((sensor, metric))
        v = reading.get(metric)
        if lim and isinstance(v, (int, float)) and not (lim[0] <= v <= lim[1]):
            if "soft_range" not in flags:
                flags.append("soft_range")
    ts = reading.get("timestamp")
    delayed = bool(reading.get("delayed"))
    if isinstance(ts, (int, float)):
        if ts - now > FUTURE_S and "clock" not in flags:
            flags.append("clock")
        if now - ts > LATE_S:
            delayed = True
    if delayed and "delayed" not in flags:
        flags.append("delayed")
    declared = reading.get("q") if reading.get("q") in LEVELS else GOOD
    return {"q": worst((declared, level_for(flags))), "qf": flags, "delayed": delayed}
