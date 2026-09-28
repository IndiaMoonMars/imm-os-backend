"""
Telemetry stream health.

A stream is one sensor on one node (and zone, crew member, real/simulated). For each
the tracker keeps when readings arrive, their sequence numbers, recent values and the
quality the validator gave them, and derives

  status   ok | suspect | bad | stale | offline
  reasons  why (quality flags, stuck, rate, cross_check, invalid, stale...)
  integrity  readings received, lost (sequence gaps), recovered (late fill), duplicates

Detection rules (imm-os-docs/fdir-strategy.md, "Sensor faults"):
  stale    nothing live for max(3 × the stream's period, 15 s)
  offline  nothing live for max(10 × period, 120 s)
  stuck    a measurement identical in ≥ 20 readings spanning ≥ 10 min (live sensors
           always show noise; a frozen driver or chip repeats its last value)
  rate     two readings further apart than physics allows (a glitched I2C read)
  invalid  ≥ 5 dead-lettered (rejected) readings in 5 min
Only live readings count for freshness: late ones (store-and-forward backlog,
blackbox replay) fill gaps but don't make a stream look alive.
"""
import json
import statistics
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, NamedTuple, Optional, Tuple  # noqa: F401

try:
    from services.telemetry_schema import SENSOR_METRICS, STATE_METRICS
except ImportError:  # pragma: no cover - scripts run from services/
    from telemetry_schema import SENSOR_METRICS, STATE_METRICS

# Nominal reporting period per sensor (s), until the stream's own is measured
DEFAULT_PERIOD_S = {
    "bme280": 5, "scd40": 5, "o2": 5, "mq7": 150, "mq4": 5, "bno055": 5, "board": 10,
    "sysmon": 10, "bms": 10, "jetson": 10, "tsl2561": 5, "ina219": 5, "ecg_ad8232": 0.05,
    "max30100": 1, "eva_biosensor": 1, "eva_position": 1,
}
# Silent when there is nothing to measure (no finger on the pulse oximeter): never stale
INTERMITTENT = {"max30100"}
STALE_FACTOR, STALE_MIN_S = 3.0, 15.0
OFFLINE_FACTOR, OFFLINE_MIN_S = 10.0, 120.0

STUCK_MIN_READINGS, STUCK_MIN_SPAN_S = 20, 600.0
# measurements checked for stuck values (not ECG, orientation or counters)
STUCK_METRICS = {
    ("bme280", "temp"), ("bme280", "hum"), ("bme280", "pres"), ("scd40", "co2_ppm"),
    ("scd40", "temp"), ("scd40", "hum"), ("o2", "o2_pct"), ("mq4", "vout_mv"), ("mq7", "co_ppm"),
    ("eva_biosensor", "hr_bpm"), ("eva_biosensor", "skin_temp_c"),
}
# largest believable change per second
RATE_LIMITS = {
    ("bme280", "temp"): 5.0, ("bme280", "hum"): 25.0, ("bme280", "pres"): 20.0,
    ("scd40", "co2_ppm"): 5000.0, ("scd40", "temp"): 5.0, ("scd40", "hum"): 25.0,
    ("o2", "o2_pct"): 2.0, ("eva_biosensor", "skin_temp_c"): 2.0,
}
RATE_HOLD_S = 60.0              # a rate glitch keeps the stream suspect this long
INVALID_WINDOW_S, INVALID_COUNT = 300.0, 5
KEEP_OFFLINE_S = 24 * 3600      # forget streams silent for a day


class StreamKey(NamedTuple):
    node: str
    sensor: str
    zone: str
    crew: str
    simulated: bool

    def id(self) -> str:
        return "|".join((self.node, self.sensor, self.zone, self.crew, "sim" if self.simulated else "live"))

    @classmethod
    def parse(cls, text: str) -> "StreamKey":
        node, sensor, zone, crew, sim = text.split("|")
        return cls(node, sensor, zone, crew, sim == "sim")


def key_for(reading: dict) -> StreamKey:
    sensor = reading.get("sensor") or ("eva_position" if reading.get("mode") in ("uwb", "gps") else "unknown")
    zone = reading.get("zone") or ("eva" if sensor == "eva_position" else "unknown")
    return StreamKey(str(reading.get("node_id") or "unknown"), sensor, str(zone),
                     str(reading.get("crew_id") or ""), bool(reading.get("simulated")))


def metrics_of(reading: dict) -> List[str]:
    if reading.get("mode") in ("uwb", "gps"):
        return [m for m in ("x_m", "y_m", "lat", "lon") if m in reading]
    return [m for m in SENSOR_METRICS.get(reading.get("sensor"), []) if m in reading]


@dataclass
class Integrity:
    received: int = 0
    lost: int = 0                # sequence numbers never seen (so far)
    recovered: int = 0           # arrived late into an earlier gap
    duplicates: int = 0
    last_gap_at: Optional[float] = None


@dataclass
class Stream:
    key: StreamKey
    first_seen: float
    last_live: Optional[float] = None          # arrival of the newest live reading
    last_any: Optional[float] = None
    last_ts: Optional[float] = None            # its timestamp
    intervals: Deque[float] = field(default_factory=lambda: deque(maxlen=21))
    q: str = "good"
    qf: List[str] = field(default_factory=list)
    last: Dict[str, float] = field(default_factory=dict)
    last_at: Dict[str, float] = field(default_factory=dict)         # metric → timestamp of last value
    runs: Dict[str, list] = field(default_factory=dict)             # metric → [value, since_ts, count]
    rate_until: float = 0.0
    rate_metric: Optional[str] = None
    invalid: Deque[float] = field(default_factory=lambda: deque(maxlen=50))
    invalid_reason: Optional[str] = None
    cross: Dict[str, str] = field(default_factory=dict)   # cross-check name → reason
    integrity: Integrity = field(default_factory=Integrity)
    _seq: Dict[str, int] = field(default_factory=dict)             # run → highest seq
    _missing: Dict[str, List[List[int]]] = field(default_factory=dict)  # run → [start, end] gaps
    delayed_count: int = 0

    # ── feeding ────────────────────────────────────────────────────
    def feed(self, reading: dict, now: float) -> List[dict]:
        """Take one validated reading; returns integrity events (gaps)."""
        events = []
        delayed = bool(reading.get("delayed"))
        self.integrity.received += 1
        self.last_any = now
        if isinstance(reading.get("seq"), int):
            events += self._sequence(str(reading.get("run") or ""), reading["seq"], now)
        if delayed:
            self.delayed_count += 1
            return events
        if self.last_live is not None:
            gap = now - self.last_live
            if gap > 0:
                self.intervals.append(gap)
        self.last_live = now
        ts = reading.get("timestamp")
        self.last_ts = float(ts) if isinstance(ts, (int, float)) else now
        self.q = reading.get("q") or "good"
        self.qf = list(reading.get("qf") or [])
        for m in metrics_of(reading):
            v = reading.get(m)
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                continue
            v = float(v)
            prev, prev_ts = self.last.get(m), self.last_at.get(m)
            self.last[m], self.last_at[m] = v, self.last_ts
            if m in STATE_METRICS:
                continue
            run = self.runs.get(m)
            if run and run[0] == v:
                run[2] += 1
            else:
                self.runs[m] = [v, self.last_ts, 1]
            lim = RATE_LIMITS.get((self.key.sensor, m))
            if lim and prev is not None and prev_ts is not None:
                dt = self.last_ts - prev_ts
                if dt >= 0.5 and abs(v - prev) / dt > lim:
                    self.rate_until, self.rate_metric = now + RATE_HOLD_S, m
        return events

    def _sequence(self, run: str, seq: int, now: float) -> List[dict]:
        last = self._seq.get(run)
        if last is None:
            self._seq[run] = seq
            if len(self._seq) > 20:                      # old runs: keep the newest few
                for r in list(self._seq)[:-10]:
                    self._seq.pop(r, None)
                    self._missing.pop(r, None)
            return []
        if seq == last + 1:
            self._seq[run] = seq
            return []
        if seq > last + 1:
            n = seq - last - 1
            self._seq[run] = seq
            self.integrity.lost += n
            self.integrity.last_gap_at = now
            gaps = self._missing.setdefault(run, [])
            gaps.append([last + 1, seq - 1])
            del gaps[:-50]
            return [{"type": "gap", "stream": self.key.id(), "run": run, "from": last + 1, "to": seq - 1, "lost": n}]
        for g in self._missing.get(run, []):             # late arrival into a known gap
            if g[0] <= seq <= g[1]:
                self.integrity.recovered += 1
                self.integrity.lost -= 1
                if g[0] == g[1]:
                    self._missing[run].remove(g)
                elif seq == g[0]:
                    g[0] += 1
                elif seq == g[1]:
                    g[1] -= 1
                else:
                    self._missing[run].remove(g)
                    self._missing[run] += [[g[0], seq - 1], [seq + 1, g[1]]]
                return []
        self.integrity.duplicates += 1
        return []

    def reject(self, reason: str, now: float) -> None:
        self.invalid.append(now)
        self.invalid_reason = reason
        self.last_any = now

    # ── assessment ─────────────────────────────────────────────────
    def period(self) -> float:
        default = DEFAULT_PERIOD_S.get(self.key.sensor, 5.0)
        if len(self.intervals) >= 5:
            return min(max(statistics.median(self.intervals), 0.01), 3600.0)
        return float(default)

    def stale_after(self) -> float:
        return max(STALE_FACTOR * self.period(), STALE_MIN_S)

    def offline_after(self) -> float:
        return max(OFFLINE_FACTOR * self.period(), OFFLINE_MIN_S)

    def stuck_metric(self) -> Optional[str]:
        """A measurement repeating the exact same value for too long."""
        for m, (value, since, count) in self.runs.items():
            if (self.key.sensor, m) not in STUCK_METRICS:
                continue
            if count >= STUCK_MIN_READINGS and self.last_at.get(m, since) - since >= STUCK_MIN_SPAN_S:
                return m
        return None

    def assess(self, now: float) -> Tuple[str, List[str]]:
        """(status, reasons) at time `now`."""
        reasons: List[str] = []
        recent_invalid = [t for t in self.invalid if now - t <= INVALID_WINDOW_S]
        if self.last_live is None:
            if len(recent_invalid) >= 1:
                return "bad", ["invalid"]
            return "offline", ["never_live"]
        age = now - self.last_live
        if self.key.sensor not in INTERMITTENT:
            if age > self.offline_after():
                return "offline", ["offline"]
            if age > self.stale_after():
                return "stale", ["stale"]
        status = self.q if self.q in ("good", "suspect", "bad") else "good"
        reasons += [f for f in self.qf if f not in ("delayed",)]
        if len(recent_invalid) >= INVALID_COUNT:
            status = "bad"
            reasons.append("invalid")
        stuck = self.stuck_metric()
        if stuck:
            reasons.append("stuck")
            status = "bad" if status == "bad" else "suspect"
        if now < self.rate_until:
            reasons.append("rate")
            status = "bad" if status == "bad" else "suspect"
        if self.cross:
            reasons.append("cross_check")
            status = "bad" if status == "bad" else "suspect"
        return ("ok" if status == "good" else status), sorted(set(reasons))

    def snapshot(self, now: float) -> dict:
        status, reasons = self.assess(now)
        return {
            "key": self.key.id(), "node_id": self.key.node, "sensor": self.key.sensor, "zone": self.key.zone,
            "crew_id": self.key.crew or None, "simulated": self.key.simulated,
            "status": status, "reasons": reasons, "q": self.q, "qf": self.qf,
            "age_s": None if self.last_live is None else round(now - self.last_live, 1),
            "period_s": round(self.period(), 3), "stale_after_s": round(self.stale_after(), 1),
            "stuck_metric": self.stuck_metric(), "rate_metric": self.rate_metric if now < self.rate_until else None,
            "cross_checks": dict(self.cross), "invalid_5min": sum(1 for t in self.invalid if now - t <= INVALID_WINDOW_S),
            "invalid_reason": self.invalid_reason,
            "integrity": {"received": self.integrity.received, "lost": self.integrity.lost,
                          "recovered": self.integrity.recovered, "duplicates": self.integrity.duplicates,
                          "delayed": self.delayed_count},
            "values": dict(self.last),
        }


class StreamTracker:
    """All streams, fed by the validated and dead-letter topics."""

    def __init__(self):
        self.streams: Dict[StreamKey, Stream] = {}

    def feed(self, reading: dict, now: float) -> Tuple[Stream, List[dict]]:
        key = key_for(reading)
        s = self.streams.get(key)
        if s is None:
            s = self.streams[key] = Stream(key=key, first_seen=now)
        return s, s.feed(reading, now)

    def reject(self, deadletter: dict, now: float) -> Optional[Stream]:
        """A dead-lettered message: attribute it to its stream when the topic/payload tell which."""
        topic = deadletter.get("mqtt_topic") or ""
        parts = topic.split("/")
        try:
            raw = json.loads(deadletter.get("raw") or "{}")
            raw = raw.get("data", raw) if isinstance(raw, dict) else {}
        except ValueError:
            raw = {}
        if len(parts) == 4 and parts[:2] == ["habitat", "sensors"]:
            sensor, zone = parts[2], raw.get("zone") or parts[3]
        elif len(parts) >= 4 and parts[:3] == ["habitat", "eva", "biosensors"]:
            sensor, zone = "eva_biosensor", "eva"
        else:
            return None
        key = StreamKey(str(raw.get("node_id") or "unknown"), sensor, str(zone),
                        str(raw.get("crew_id") or (parts[3] if sensor == "eva_biosensor" else "")),
                        bool(raw.get("simulated")))
        s = self.streams.get(key)
        if s is None:
            s = self.streams[key] = Stream(key=key, first_seen=now)
        s.reject(str(deadletter.get("reason") or "invalid"), now)
        return s

    def register(self, key: StreamKey, first_seen: float, last_live: float, period: Optional[float]) -> None:
        """Seed a stream known from before a restart, so its silence is noticed."""
        if key in self.streams:
            return
        s = Stream(key=key, first_seen=first_seen, last_live=last_live)
        if period:
            s.intervals.extend([period] * 5)
        self.streams[key] = s

    def prune(self, now: float) -> None:
        for k, s in list(self.streams.items()):
            ref = s.last_any or s.first_seen
            if now - ref > KEEP_OFFLINE_S:
                del self.streams[k]

    def live(self, now: float):
        """Streams with their current status."""
        for s in self.streams.values():
            yield s, s.assess(now)
