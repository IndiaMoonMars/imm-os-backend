"""
Habitat measurements, their redundant sources, and cross-checks between sensors.

A measurement (CO₂, O₂, temperature...) in one zone of one node can come from several
sensors. The best available source is used: good before suspect, then the listed
priority, real hardware before the simulator. Bad, stale or offline sources are
never used. Status:
  NOMINAL   the primary source is good
  DEGRADED  running on a backup source, or on a suspect one
  LOST      no usable source (for CO₂ / O₂: loss of critical monitoring)

Cross-checks flag sensors that disagree with physics or each other (the same checks
the edge's tools/verify_esp32.py and the frontend's sensors/checks.ts run):
  dew point   the BME280's and SCD40's dew points differ by > 3 °C: one humidity sensor is wrong
  O₂ vs CO₂   O₂ is more than 1 % from what the CO₂ level implies: the O₂ cell is off
"""
import math
from typing import Dict, List, Optional, Tuple

from services.health.streams import StreamKey, StreamTracker

MEASUREMENTS: Dict[str, dict] = {
    "co2": {"critical": True, "unit": "ppm", "sources": [("scd40", "co2_ppm")]},
    "o2": {"critical": True, "unit": "%", "sources": [("o2", "o2_pct")]},
    "temperature": {"critical": False, "unit": "°C", "sources": [("bme280", "temp"), ("scd40", "temp")]},
    "humidity": {"critical": False, "unit": "%RH", "sources": [("bme280", "hum"), ("scd40", "hum")]},
    "pressure": {"critical": False, "unit": "hPa", "sources": [("bme280", "pres")]},
    "methane": {"critical": False, "unit": "ppm", "sources": [("mq4", "ch4_ppm")]},
    "co": {"critical": False, "unit": "ppm", "sources": [("mq7", "co_ppm")]},
}
USABLE = {"ok": 0, "suspect": 1}
DEW_POINT_MAX_DIFF_C = 3.0
O2_MAX_DIFF_PCT = 1.0


def dew_point(temp_c: float, rh_pct: float) -> Optional[float]:
    if not rh_pct or rh_pct <= 0:
        return None
    g = math.log(min(rh_pct, 100.0) / 100.0) + 17.62 * temp_c / (243.12 + temp_c)
    return 243.12 * g / (17.62 - g)


def expected_o2(co2_ppm: float) -> float:
    """O₂ % implied by CO₂: breathing turns O₂ into CO₂ about 1.2 : 1."""
    return 20.95 - 1.2 * max(0.0, co2_ppm - 420.0) / 1e4


def _groups(tracker: StreamTracker) -> Dict[Tuple[str, str], List]:
    out: Dict[Tuple[str, str], List] = {}
    for s in tracker.streams.values():
        if s.key.crew:            # EVA streams are per crew member, not habitat air
            continue
        out.setdefault((s.key.node, s.key.zone), []).append(s)
    return out


def cross_check(tracker: StreamTracker, now: float) -> List[dict]:
    """Set each stream's cross-check disagreements; returns the active ones."""
    found = []
    for s in tracker.streams.values():
        s.cross = {}
    for (node, zone), streams in _groups(tracker).items():
        live = {}
        for s in streams:
            status, _ = s.assess(now)
            if status in ("ok", "suspect") and not s.key.simulated:
                live[s.key.sensor] = s
        bme, scd, o2 = live.get("bme280"), live.get("scd40"), live.get("o2")
        if bme and scd and {"temp", "hum"} <= set(bme.last) and {"temp", "hum"} <= set(scd.last):
            a = dew_point(bme.last["temp"], bme.last["hum"])
            b = dew_point(scd.last["temp"], scd.last["hum"])
            if a is not None and b is not None and abs(a - b) > DEW_POINT_MAX_DIFF_C:
                why = f"dew point {a:.1f} °C (BME280) vs {b:.1f} °C (SCD40)"
                bme.cross["dew_point"] = scd.cross["dew_point"] = why
                found.append({"node": node, "zone": zone, "check": "dew_point", "detail": why})
        if o2 and scd and "o2_pct" in o2.last and "co2_ppm" in scd.last:
            exp = expected_o2(scd.last["co2_ppm"])
            if abs(o2.last["o2_pct"] - exp) > O2_MAX_DIFF_PCT:
                why = f"O₂ {o2.last['o2_pct']:.2f} % but CO₂ {scd.last['co2_ppm']:.0f} ppm implies ~{exp:.1f} %"
                o2.cross["o2_vs_co2"] = why
                found.append({"node": node, "zone": zone, "check": "o2_vs_co2", "detail": why})
    return found


def evaluate(tracker: StreamTracker, now: float) -> List[dict]:
    """Every measurement each node/zone has (or had) a source for."""
    out = []
    for (node, zone), streams in _groups(tracker).items():
        by_source = {}
        for s in streams:
            by_source.setdefault(s.key.sensor, []).append(s)
        for name, spec in MEASUREMENTS.items():
            candidates = []
            for prio, (sensor, metric) in enumerate(spec["sources"]):
                for s in by_source.get(sensor, []):
                    if metric not in s.last:
                        continue
                    status, reasons = s.assess(now)
                    candidates.append((prio, s, metric, status, reasons))
            if not candidates:
                continue          # this node never had a source for it
            usable = [c for c in candidates if c[3] in USABLE]
            usable.sort(key=lambda c: (USABLE[c[3]], c[1].key.simulated, c[0]))
            m = {"node_id": node, "zone": zone, "measurement": name, "unit": spec["unit"],
                 "critical": spec["critical"],
                 "sources": [{"stream": c[1].key.id(), "sensor": c[1].key.sensor, "status": c[3],
                              "reasons": c[4], "simulated": c[1].key.simulated} for c in candidates]}
            if not usable:
                m.update(status="LOST", value=None, source=None, simulated=None,
                         reason="no usable source: " + ", ".join(f"{c[1].key.sensor} {c[3]}" for c in candidates))
            else:
                prio, s, metric, status, reasons = usable[0]
                primary_ok = prio == 0 and status == "ok"
                m.update(status="NOMINAL" if primary_ok else "DEGRADED", value=s.last.get(metric),
                         source=s.key.id(), source_sensor=s.key.sensor, simulated=s.key.simulated,
                         quality=status, reason=None if primary_ok else (
                             f"using backup {s.key.sensor}" if prio else f"{s.key.sensor} is {status}: {', '.join(reasons)}"))
            out.append(m)
    return out


def primary_key(node: str, zone: str, measurement: str) -> Optional[StreamKey]:
    spec = MEASUREMENTS.get(measurement)
    if not spec:
        return None
    return StreamKey(node, spec["sources"][0][0], zone, "", False)
