"""
EVA loss of signal (LOS) monitoring, one state machine per crew member.

Inputs: the suit's vitals (eva_biosensor, 5 Hz) and fused position (habitat/eva/position,
5 Hz). Contact = the newest live frame of either. States, by time since contact
(configurable; defaults from imm-os-docs/eva-contingency.md):

  DISARMED     not on EVA: no monitoring
  NOMINAL      frames arriving
  LOS_WARN     no contact for ≥ 10 s        caution   "check comms, try voice"
  LOS          no contact for ≥ 30 s        warning   "LOS: initiate comm-loss procedure"
  CONTINGENCY  no contact for ≥ 120 s       emergency "EVA contingency: dispatch buddy/IV crew"
Partial loss while in contact: vitals missing ≥ 15 s (position fine) or position
missing ≥ 15 s (vitals fine) → caution.

Arming: automatically on the first live, real frame of a crew member, or when an EVA
plan naming them is IN_PROGRESS; disarmed by the MCC (EVA complete) or when the plan
is COMPLETE/ABORTED. State survives a health-monitor restart (eva_crew_status table).

While out of contact the monitor keeps the last known position and vitals, the speed
seen just before (from the last positions) and a search radius that grows with time
(walking at up to 1.4 m/s, capped). After contact returns it reports how long the
outage was and how much of it the suit's store-and-forward backlog filled in.
Thresholds count on arrival at the monitor: with a simulated Mars delay the data is
older still (reported as data_age_s); with minutes of delay the crew in the habitat,
not the MCC, owns the response (see the doc).
"""
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

WARN_S, LOS_S, CONTINGENCY_S = 10.0, 30.0, 120.0
PARTIAL_S = 15.0
WALK_SPEED_MS, MAX_RADIUS_M = 1.4, 2000.0
STATES = ("DISARMED", "NOMINAL", "LOS_WARN", "LOS", "CONTINGENCY")


def _dist_m(a: dict, b: dict) -> Optional[float]:
    if "x_m" in a and "x_m" in b:
        return math.hypot(a["x_m"] - b["x_m"], a["y_m"] - b["y_m"])
    if "lat" in a and "lat" in b:
        r = 6371000.0
        p1, p2 = math.radians(a["lat"]), math.radians(b["lat"])
        dp, dl = p2 - p1, math.radians(b["lon"] - a["lon"])
        h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * r * math.asin(math.sqrt(h))
    return None


@dataclass
class CrewTrack:
    crew: str
    armed: bool = False
    armed_at: Optional[float] = None
    armed_by: str = "auto"
    state: str = "DISARMED"
    last_contact: Optional[float] = None
    last_vitals_at: Optional[float] = None
    last_position_at: Optional[float] = None
    last_vitals: dict = field(default_factory=dict)
    last_position: dict = field(default_factory=dict)
    last_data_ts: Optional[float] = None
    positions: Deque[Tuple[float, dict]] = field(default_factory=lambda: deque(maxlen=50))
    los_started: Optional[float] = None
    backfill_pending: int = 0              # delayed readings from the current outage, before contact returned
    outages: List[dict] = field(default_factory=list)     # completed LOS periods
    simulated: bool = False

    def speed_ms(self) -> Optional[float]:
        pts = [p for p in self.positions if self.positions and p[0] >= self.positions[-1][0] - 10.0]
        if len(pts) < 2:
            return None
        d = _dist_m(pts[0][1], pts[-1][1])
        dt = pts[-1][0] - pts[0][0]
        return None if d is None or dt <= 0 else d / dt

    def snapshot(self, now: float) -> dict:
        since = None if self.last_contact is None else now - self.last_contact
        radius = None
        if since is not None and self.state in ("LOS_WARN", "LOS", "CONTINGENCY"):
            radius = min(MAX_RADIUS_M, max(self.speed_ms() or 0.0, WALK_SPEED_MS) * since)
        return {
            "crew_id": self.crew, "armed": self.armed, "armed_by": self.armed_by, "armed_at": self.armed_at,
            "state": self.state, "simulated": self.simulated,
            "since_contact_s": None if since is None else round(since, 1),
            "vitals_age_s": None if self.last_vitals_at is None else round(now - self.last_vitals_at, 1),
            "position_age_s": None if self.last_position_at is None else round(now - self.last_position_at, 1),
            "data_age_s": None if self.last_data_ts is None else round(now - self.last_data_ts, 1),
            "last_vitals": self.last_vitals, "last_position": self.last_position,
            "speed_ms": None if self.speed_ms() is None else round(self.speed_ms(), 2),
            "search_radius_m": None if radius is None else round(radius, 1),
            "los_started": self.los_started, "outages": self.outages[-10:],
        }


class EvaMonitor:
    def __init__(self, warn_s=WARN_S, los_s=LOS_S, contingency_s=CONTINGENCY_S, partial_s=PARTIAL_S):
        self.warn_s, self.los_s, self.contingency_s, self.partial_s = warn_s, los_s, contingency_s, partial_s
        self.crews: Dict[str, CrewTrack] = {}

    def track(self, crew: str) -> CrewTrack:
        crew = crew.lower()
        t = self.crews.get(crew)
        if t is None:
            t = self.crews[crew] = CrewTrack(crew=crew)
        return t

    # ── inputs ─────────────────────────────────────────────────────
    def feed(self, reading: dict, now: float) -> List[dict]:
        crew = str(reading.get("crew_id") or "").lower()
        if not crew:
            return []
        t = self.track(crew)
        events = []
        is_pos = reading.get("mode") in ("uwb", "gps")
        if reading.get("delayed"):
            ts = reading.get("timestamp")
            if isinstance(ts, (int, float)):
                if t.los_started is not None and ts >= t.los_started:
                    t.backfill_pending += 1     # the suit replays its log before live data resumes
                for o in t.outages:     # backfill: data from inside an outage arrived after all
                    if o["from_ts"] <= ts <= o["to_ts"]:
                        o["backfilled"] = o.get("backfilled", 0) + 1
            return events
        if not t.armed and not reading.get("simulated"):
            events += self.arm(crew, now, by="auto")
        t.simulated = bool(reading.get("simulated"))
        t.last_contact = now
        ts = reading.get("timestamp")
        t.last_data_ts = float(ts) if isinstance(ts, (int, float)) else now
        if is_pos:
            t.last_position_at = now
            t.last_position = {k: reading[k] for k in ("mode", "x_m", "y_m", "z_m", "lat", "lon", "quality", "timestamp")
                               if k in reading}
            t.positions.append((t.last_data_ts, t.last_position))
        else:
            t.last_vitals_at = now
            t.last_vitals = {k: reading[k] for k in ("hr_bpm", "spo2_pct", "skin_temp_c", "timestamp") if k in reading}
        return events

    def arm(self, crew: str, now: float, by: str = "mcc") -> List[dict]:
        t = self.track(crew)
        if t.armed:
            return []
        t.armed, t.armed_at, t.armed_by, t.state = True, now, by, "NOMINAL"
        t.los_started = None
        if t.last_contact is None:
            t.last_contact = now      # the clock starts now: a suit that never reports reaches LOS
        return [{"type": "eva", "event": "armed", "crew_id": t.crew, "by": by, "at": now}]

    def disarm(self, crew: str, now: float, by: str = "mcc") -> List[dict]:
        t = self.track(crew)
        if not t.armed:
            return []
        t.armed, t.state, t.los_started = False, "DISARMED", None
        return [{"type": "eva", "event": "disarmed", "crew_id": t.crew, "by": by, "at": now}]

    # ── evaluation ─────────────────────────────────────────────────
    def tick(self, now: float) -> List[dict]:
        events = []
        for t in self.crews.values():
            if not t.armed:
                continue
            since = now - (t.last_contact or now)
            if since >= self.contingency_s:
                new = "CONTINGENCY"
            elif since >= self.los_s:
                new = "LOS"
            elif since >= self.warn_s:
                new = "LOS_WARN"
            else:
                new = "NOMINAL"
            if new != t.state:
                if t.state == "NOMINAL" and new != "NOMINAL":
                    t.los_started = t.last_contact
                if new == "NOMINAL" and t.los_started is not None:
                    outage = {"from": t.los_started, "to": t.last_contact, "duration_s": round(t.last_contact - t.los_started, 1),
                              "worst": t.state, "from_ts": t.los_started, "to_ts": t.last_contact,
                              "backfilled": t.backfill_pending}
                    t.backfill_pending = 0
                    # data timestamps of the gap (arrival ≈ timestamp for live data)
                    t.outages.append(outage)
                    del t.outages[:-20]
                    events.append({"type": "eva", "event": "recovered", "crew_id": t.crew, "at": now,
                                   "outage_s": outage["duration_s"], "worst": outage["worst"]})
                    t.los_started = None
                events.append({"type": "eva", "event": "state", "crew_id": t.crew, "from": t.state, "to": new, "at": now})
                t.state = new
        return events

    def conditions(self, now: float) -> Dict[str, dict]:
        """Alarm conditions for the rules: key → (severity, message, details)."""
        out = {}
        for t in self.crews.values():
            if not t.armed:
                continue
            snap = t.snapshot(now)
            where = ""
            if t.last_position:
                p = t.last_position
                where = (f" Last position {p.get('x_m', 0):.1f}, {p.get('y_m', 0):.1f} m (UWB)" if p.get("mode") == "uwb"
                         else f" Last position {p.get('lat', 0):.5f}, {p.get('lon', 0):.5f} (GPS)")
                if snap["search_radius_m"] is not None:
                    where += f", search radius {snap['search_radius_m']:.0f} m."
            since = snap["since_contact_s"] or 0
            if t.state == "LOS_WARN":
                out[f"eva.los.{t.crew}"] = ("caution", f"EVA {t.crew}: no telemetry for {since:.0f} s: check comms, try voice.{where}", snap)
            elif t.state == "LOS":
                out[f"eva.los.{t.crew}"] = ("warning", f"EVA {t.crew}: LOSS OF SIGNAL {since:.0f} s: start the comm-loss procedure.{where}", snap)
            elif t.state == "CONTINGENCY":
                out[f"eva.los.{t.crew}"] = ("emergency", f"EVA {t.crew}: LOS {since:.0f} s: EVA CONTINGENCY: dispatch buddy/IV crew to the last position.{where}", snap)
            elif t.state == "NOMINAL":
                va = snap["vitals_age_s"]
                pa = snap["position_age_s"]
                if t.last_vitals_at is not None and va is not None and va >= self.partial_s:
                    out[f"eva.vitals_lost.{t.crew}"] = ("caution", f"EVA {t.crew}: no suit vitals for {va:.0f} s (position still arriving)", snap)
                if t.last_position_at is not None and pa is not None and pa >= self.partial_s:
                    out[f"eva.position_lost.{t.crew}"] = ("caution", f"EVA {t.crew}: no position for {pa:.0f} s (vitals still arriving)", snap)
        return out
