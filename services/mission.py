"""
IMM-OS mission record: the mission clock, sols and the per-sol numbers.

A mission starts at T0 (the moment it is started, or a time given in IST) and runs for N sols
of exactly 24 h: Sol n covers [T0 + (n-1)·24 h, T0 + n·24 h). Readings are not tagged with a
sol when stored; the sol comes from the reading's time and T0, so correcting T0 rewrites
nothing. (The telemetry processor's own "sol" tag is the Mars Sol Date, unrelated.)

Everything here is pure: the API (mission_api.py) queries InfluxDB for 1-minute rollups
and hands them to these functions.

Rollups, per sol:
    values:  {(sensor, metric): {minute_start_epoch: [mean, min, max, n]}}   good-quality readings only
    counts:  {(sensor, node_id, zone): {minute_start_epoch: n}}              every reading (coverage)
"""
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

IST = timezone(timedelta(hours=5, minutes=30))
SOL_S = 86400

Values = Dict[Tuple[str, str], Dict[int, List[float]]]
Counts = Dict[Tuple[str, str, str], Dict[int, int]]

# (key, label, unit, decimals, sources in order of preference)
MEASUREMENTS: List[Tuple[str, str, str, int, List[Tuple[str, str]]]] = [
    ("co2", "CO₂", "ppm", 0, [("scd40", "co2_ppm")]),
    ("o2", "O₂", "%", 2, [("o2", "o2_pct")]),
    ("temperature", "Temperature", "°C", 1, [("bme280", "temp"), ("scd40", "temp")]),
    ("humidity", "Humidity", "%RH", 0, [("bme280", "hum"), ("scd40", "hum")]),
    ("pressure", "Pressure", "hPa", 1, [("bme280", "pres")]),
    ("radiation", "Radiation", "µSv/h", 3, [("geiger", "usv_h")]),
    ("methane", "Methane", "ppm", 1, [("mq4", "ch4_ppm")]),
    ("co", "CO", "ppm", 1, [("mq7", "co_ppm")]),
]
MEASUREMENT_KEYS = [m[0] for m in MEASUREMENTS]
VALUE_PAIRS = sorted({p for m in MEASUREMENTS for p in m[4]})

# Sensors that report a "warming" flag: their warm-up minutes are left out of the statistics
WARMING_SENSORS = ("mq4", "geiger")

# The metric counted to see whether a sensor was reporting (present in every reading,
# also while warming up: the MQ-4 has no ch4_ppm then, the SCD40 no co2_ppm at first)
PRESENCE_METRIC = {
    "bme280": "temp", "scd40": "temp", "o2": "o2_pct", "mq4": "vout_mv", "bno055": "heading_deg",
    "geiger": "cpm", "gnss": "sats", "board": "uptime_s", "sysmon": "cpu_temp", "mq7": "co_ppm",
    "tsl2561": "lux", "ina219": "voltage_v", "bms": "battery_pct", "max30100": "hr_bpm",
    "ecg_ad8232": "voltage", "jetson": "cpu_temp",
}


@dataclass
class Mission:
    id: int
    name: str
    start: float                      # T0, epoch seconds
    sols: int = 7
    crew: Optional[int] = None
    notes: str = ""
    ended_at: Optional[float] = None  # ended early (or closed after the last sol)
    created_by: Optional[str] = None
    label: str = ""                   # "" | "restarted" | "test" | "aborted" (see LABELS)

    @property
    def end(self) -> float:
        return self.start + self.sols * SOL_S

    def to_json(self) -> dict:
        d = asdict(self)
        d.update(end=self.end, start_ist=ist(self.start), end_ist=ist(self.end))
        return d


# Mission labels. Nothing is ever deleted: a label only changes where a mission is listed.
#   restarted  replaced by a new mission with the same settings (kept in History, downloadable)
#   test       a dry run: kept, left out of History's default list
#   aborted    a false start (only within ABORT_WINDOW_S of Sol 1): no longer the current mission
LABELS = ("", "restarted", "test", "aborted")
ABORT_WINDOW_S = 3600


def status(m: Mission, now: float) -> str:
    """One word for History: the label if it has one, else where the clock is."""
    if m.label:
        return m.label
    phase = clock(m, now)["phase"]
    if phase == "pre":
        return "upcoming"
    if phase == "active":
        return "running"
    return "ended early" if m.ended_at and m.ended_at < m.end else "complete"


def abortable(m: Mission, now: float) -> bool:
    """A false start can be aborted until an hour into Sol 1 (not after it ended or was relabelled)."""
    return not m.label and (m.ended_at is None or m.ended_at > now) and now < m.start + ABORT_WINDOW_S


def ist(ts: Optional[float], fmt: str = "%a %d %b %Y, %H:%M:%S IST") -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, IST).strftime(fmt)


def parse_ist(text: str) -> float:
    """'2026-10-01 06:00' or '2026-10-01T06:00:00' (IST, unless an offset is given) → epoch."""
    t = text.strip().replace(" ", "T", 1)
    d = datetime.fromisoformat(t)
    if d.tzinfo is None:
        d = d.replace(tzinfo=IST)
    return d.timestamp()


def sol_of(m: Mission, t: float) -> int:
    """0 before T0; 1 … sols during the mission; sols + 1 after it."""
    if t < m.start:
        return 0
    return min(int((t - m.start) // SOL_S) + 1, m.sols + 1)


def sol_bounds(m: Mission, n: int) -> Tuple[float, float]:
    s = m.start + (n - 1) * SOL_S
    return s, s + SOL_S


def clock(m: Optional[Mission], now: float) -> dict:
    """The mission clock for the top bar: phase, sol, time into the sol, progress."""
    if m is None:
        return {"phase": "none"}
    if m.ended_at is not None and m.ended_at <= m.start and now >= m.ended_at:      # ended before Sol 1
        return {"phase": "complete", "sol": 0, "met_s": 0.0, "progress": 0.0}
    if now < m.start:
        return {"phase": "pre", "sol": 0, "t_minus_s": m.start - now, "progress": 0.0}
    end = min(m.end, m.ended_at) if m.ended_at else m.end
    if now >= end:
        return {"phase": "complete", "sol": min(sol_of(m, end - 1), m.sols), "met_s": end - m.start, "progress": 1.0}
    met = now - m.start
    sol = sol_of(m, now)
    into = met - (sol - 1) * SOL_S
    return {"phase": "active", "sol": sol, "met_s": met, "sol_elapsed_s": into, "sol_left_s": SOL_S - into,
            "progress": met / (m.sols * SOL_S)}


def sol_states(m: Mission, now: float) -> List[dict]:
    out = []
    for n in range(1, m.sols + 1):
        s, e = sol_bounds(m, n)
        end = min(e, m.ended_at) if m.ended_at else e
        state = "upcoming" if now < s or (m.ended_at and s >= m.ended_at) else ("done" if now >= end else "live")
        out.append({"sol": n, "start": s, "end": e, "state": state, "start_ist": ist(s, "%d %b %H:%M"),
                    "end_ist": ist(e, "%d %b %H:%M")})
    return out


# ── per-sol numbers ────────────────────────────────────────────────

def pick_source(values: Values, sources: List[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    for p in sources:
        if values.get(p):
            return p
    return None


def stats(series: Dict[int, List[float]]) -> Optional[dict]:
    """Mean (weighted by readings), min and max with the minute they happened."""
    if not series:
        return None
    n = sum(v[3] for v in series.values())
    mean = sum(v[0] * v[3] for v in series.values()) / n if n else None
    t_min = min(series, key=lambda t: series[t][1])
    t_max = max(series, key=lambda t: series[t][2])
    last = series[max(series)]
    return {"mean": mean, "min": series[t_min][1], "min_t": t_min, "max": series[t_max][2], "max_t": t_max,
            "last": last[0], "readings": int(n)}


def buckets(series: Dict[int, List[float]], origin: float, width_s: int) -> List[List[float]]:
    """[[hours from origin, mean, min, max], …] in buckets of width_s (gaps stay gaps)."""
    acc: Dict[int, List[float]] = {}
    for t, (mean, lo, hi, n) in series.items():
        b = int((t - origin) // width_s)
        a = acc.setdefault(b, [0.0, math.inf, -math.inf, 0.0])
        a[0] += mean * n
        a[1] = min(a[1], lo)
        a[2] = max(a[2], hi)
        a[3] += n
    return [[round((b * width_s + width_s / 2) / 3600, 4), a[0] / a[3] if a[3] else None, a[1], a[2]]
            for b, a in sorted(acc.items())]


def coverage(series: Dict[int, int], start: float, end: float, period_s: float) -> Optional[float]:
    """% of time slots in [start, end) with at least one reading. Slots are ≥ 1 min and ≥ 2
    reporting periods, so a sensor that reports every 150 s isn't counted short."""
    if end <= start:
        return None
    slot = max(60, int(math.ceil(2 * period_s / 60)) * 60)
    total = int(math.ceil((end - start) / slot))
    have = {int((t - start) // slot) for t, n in series.items() if n and start <= t < end}
    return round(100.0 * len(have) / total, 1) if total else None


def dose_usv(series: Dict[int, List[float]]) -> float:
    """µSv from 1-minute mean dose rates (µSv/h). Minutes without data add nothing."""
    return sum(v[0] for v in series.values()) / 60.0


def summary(m: Mission, n: int, values: Values, counts: Counts, now: float,
            periods: Dict[str, float]) -> dict:
    s, e = sol_bounds(m, n)
    end = min(e, now, m.ended_at or e)
    meas = {}
    for key, label, unit, dp, sources in MEASUREMENTS:
        src = pick_source(values, sources)
        st = stats(values[src]) if src else None
        meas[key] = {"label": label, "unit": unit, "dp": dp, "source": ".".join(src) if src else None, **(st or {})}
    cov = []
    for (sensor, node, zone), series in sorted(counts.items()):
        pct = coverage(series, s, end, periods.get(sensor, 5.0))
        cov.append({"sensor": sensor, "node_id": node, "zone": zone, "pct": pct,
                    "readings": int(sum(series.values()))})
    geiger = values.get(("geiger", "usv_h"), {})
    pcts = [c["pct"] for c in cov if c["pct"] is not None and c["pct"] >= 10]   # not a test node's minute
    return {"sol": n, "start": s, "end": e, "live": s <= now < e and not m.ended_at,
            "start_ist": ist(s), "end_ist": ist(e), "measurements": meas, "coverage": cov,
            "coverage_pct": round(sum(pcts) / len(pcts), 1) if pcts else None,
            "dose_usv": round(dose_usv(geiger), 3) if geiger else None,
            "readings": int(sum(sum(c.values()) for c in counts.values()))}


def timeline(m: Mission, per_sol: Dict[int, Values], measurement: str, width_s: int = 600) -> dict:
    """The whole mission for one measurement: [[hours since T0, mean, min, max], …]."""
    spec = next(x for x in MEASUREMENTS if x[0] == measurement)
    pts: List[List[float]] = []
    source = None
    for n in sorted(per_sol):
        src = pick_source(per_sol[n], spec[4])
        if src:
            source = source or ".".join(src)
            pts += buckets(per_sol[n][src], m.start, width_s)
    return {"measurement": measurement, "label": spec[1], "unit": spec[2], "dp": spec[3], "source": source,
            "points": pts}


def overlay(m: Mission, per_sol: Dict[int, Values], measurement: str, width_s: int = 600) -> dict:
    """Each sol's profile against hours into the sol."""
    spec = next(x for x in MEASUREMENTS if x[0] == measurement)
    sols = {}
    for n in sorted(per_sol):
        src = pick_source(per_sol[n], spec[4])
        if src:
            sols[n] = [[p[0], p[1]] for p in buckets(per_sol[n][src], sol_bounds(m, n)[0], width_s)]
    return {"measurement": measurement, "label": spec[1], "unit": spec[2], "dp": spec[3], "sols": sols}


def dose_series(m: Mission, per_sol: Dict[int, Values], width_s: int = 1800) -> dict:
    """Cumulative mission dose: per sol, and [[hours since T0, µSv so far], …]."""
    total, pts, per = 0.0, [], {}
    for n in sorted(per_sol):
        series = per_sol[n].get(("geiger", "usv_h"), {})
        per[n] = round(dose_usv(series), 3) if series else None
        acc: Dict[int, float] = {}
        for t, v in series.items():
            acc[int((t - m.start) // width_s)] = acc.get(int((t - m.start) // width_s), 0.0) + v[0] / 60.0
        for b in sorted(acc):
            total += acc[b]
            pts.append([round((b + 1) * width_s / 3600, 3), round(total, 4)])
    return {"per_sol": per, "total_usv": round(total, 3), "points": pts}


# ── rollups: InfluxDB rows ↔ dicts, and JSON for the archive ───────

def values_from_series_rows(rows: List[dict], warming: set = frozenset()) -> Values:
    """Per-series 1-minute aggregates (one series per node / zone / tag set) → Values, merged per
    (sensor, metric): count-weighted mean, lowest min, highest max. Minutes in `warming`
    ({(sensor, minute)}) are dropped."""
    cells: Dict[tuple, dict] = {}
    for r in rows:
        if r["value"] is None or (r["sensor"], int(r["t"])) in warming:
            continue
        key = (r["sensor"], r["metric"], r.get("node_id"), r.get("zone"), r.get("series"), int(r["t"]))
        cells.setdefault(key, {})[r["result"]] = float(r["value"])
    out: Values = {}
    for (sensor, metric, _n, _z, _s, t), c in cells.items():
        n = c.get("count", 0.0)
        if not n or "mean" not in c:
            continue
        cell = out.setdefault((sensor, metric), {}).get(t)
        if cell is None:
            out[(sensor, metric)][t] = [c["mean"], c.get("min", c["mean"]), c.get("max", c["mean"]), n]
        else:
            total = cell[3] + n
            cell[0] = (cell[0] * cell[3] + c["mean"] * n) / total
            cell[1] = min(cell[1], c.get("min", c["mean"]))
            cell[2] = max(cell[2], c.get("max", c["mean"]))
            cell[3] = total
    return out


def counts_from_rows(rows: List[dict]) -> Counts:
    """Per-series 1-minute counts → per (sensor, node, zone), summed over series."""
    out: Counts = {}
    for r in rows:
        if r["value"]:
            s = out.setdefault((r["sensor"], r["node_id"], r["zone"]), {})
            s[int(r["t"])] = s.get(int(r["t"]), 0) + int(r["value"])
    return out


def mission_coverage(cov_by_sol: Dict[int, List[dict]], min_pct: float = 10.0) -> Tuple[List[dict], Dict[int, Optional[float]]]:
    """Which sensors belong to the mission, and each sol's data coverage.

    A sensor belongs if it reached min_pct in some sol (a test node that reported for a minute
    does not). From the first sol it appears in, a sol without it counts 0 % (a dead sensor
    must not simply vanish). → (grid rows, overall % per sol)."""
    pct: Dict[tuple, Dict[int, Optional[float]]] = {}
    for n, rows in cov_by_sol.items():
        for c in rows:
            pct.setdefault((c["sensor"], c["node_id"], c["zone"]), {})[n] = c["pct"]
    keep = {k: v for k, v in pct.items() if max((x or 0) for x in v.values()) >= min_pct}
    rows_out = []
    for (sensor, node, zone), by_sol in sorted(keep.items()):
        first = min(by_sol)
        rows_out.append({"sensor": sensor, "node_id": node, "zone": zone,
                         "sols": {n: by_sol.get(n, 0.0) for n in sorted(cov_by_sol) if n >= first}})
    overall = {}
    for n in cov_by_sol:
        vals = [r["sols"][n] for r in rows_out if n in r["sols"] and r["sols"][n] is not None]
        overall[n] = round(sum(vals) / len(vals), 1) if vals else None
    return rows_out, overall


def rollup_to_json(values: Values, counts: Counts) -> dict:
    return {"values": {f"{s}|{m}": {str(t): v for t, v in ser.items()} for (s, m), ser in values.items()},
            "counts": {f"{s}|{n}|{z}": {str(t): c for t, c in ser.items()} for (s, n, z), ser in counts.items()}}


def rollup_from_json(d: dict) -> Tuple[Values, Counts]:
    values = {tuple(k.split("|", 1)): {int(t): v for t, v in ser.items()} for k, ser in d.get("values", {}).items()}
    counts = {tuple(k.split("|", 2)): {int(t): c for t, c in ser.items()} for k, ser in d.get("counts", {}).items()}
    return values, counts  # type: ignore[return-value]
