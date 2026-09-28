"""
The health monitor's core: ingest messages, tick every second, report.

  ingest_reading(envelope)     telemetry.validated (sensor readings, EVA positions)
  ingest_deadletter(message)   telemetry.deadletter (rejected readings)
  ingest_component(status)     habitat/health/<node>/<component> (via Kafka health.raw)
  set_services(status)         MCC service checks (health_monitor.py)
  tick(now) → events           alarm, EVA and integrity events since the last tick
  summary / streams / measurements / eva_status   the current picture
"""
import time
from typing import Dict, List, Optional

from services.health import measurements as meas
from services.health.alarms import AlarmManager
from services.health.eva import EvaMonitor
from services.health.rules import Rules
from services.health.status import mission_mode, subsystems
from services.health.streams import StreamTracker

COMPONENT_STATES = {"NOMINAL", "DEGRADED", "SAFE", "ISOLATED", "FAULT", "STARTING", "STOPPED"}


def normalise_component(msg: dict, topic: Optional[str] = None) -> Optional[dict]:
    """habitat/health/<node>/<component> status → dict, or None if malformed."""
    if not isinstance(msg, dict):
        return None
    parts = (topic or "").split("/")
    node = msg.get("node_id") or (parts[2] if len(parts) >= 4 else None)
    comp = msg.get("component") or (parts[3] if len(parts) >= 4 else None)
    state = str(msg.get("state") or "").upper()
    if not node or not comp or state not in COMPONENT_STATES:
        return None
    out = {"node_id": str(node)[:64], "component": str(comp)[:64], "state": state,
           "reason": str(msg.get("reason") or "")[:300], "timestamp": msg.get("timestamp"),
           "interval_s": float(msg.get("interval_s") or 60), "details": msg.get("details") if isinstance(msg.get("details"), dict) else {}}
    return out


class HealthMonitor:
    def __init__(self, eva: Optional[EvaMonitor] = None):
        self.tracker = StreamTracker()
        self.alarms = AlarmManager()
        self.eva = eva or EvaMonitor()
        self.rules = Rules()
        self.components: Dict[str, dict] = {}
        self.services: Dict[str, dict] = {}
        self.extra_conditions: Dict = {}       # conditions from outside the rules (reported recoveries)
        self._pending: List[dict] = []
        self._measurements: List[dict] = []
        self._subsystems: List[dict] = []
        self._mode: dict = {"mode": "NOMINAL"}
        self.last_tick: Optional[float] = None
        self.started = time.time()
        self.counts = {"readings": 0, "deadletters": 0, "components": 0}

    # ── inputs ─────────────────────────────────────────────────────
    def ingest_reading(self, envelope: dict, now: float) -> None:
        data = envelope.get("data", envelope) if isinstance(envelope, dict) else None
        if not isinstance(data, dict):
            return
        self.counts["readings"] += 1
        _, events = self.tracker.feed(data, now)
        self._pending += events
        if data.get("crew_id") and (data.get("sensor") == "eva_biosensor" or data.get("mode") in ("uwb", "gps")):
            self._pending += self.eva.feed(data, now)

    def ingest_deadletter(self, msg: dict, now: float) -> None:
        if isinstance(msg, dict):
            self.counts["deadletters"] += 1
            self.tracker.reject(msg, now)

    def ingest_component(self, msg: dict, now: float, topic: Optional[str] = None) -> Optional[dict]:
        comp = normalise_component(msg, topic)
        if comp is None:
            return None
        self.counts["components"] += 1
        comp["_received"] = now
        cid = f"{comp['node_id']}/{comp['component']}"
        prev = self.components.get(cid)
        self.components[cid] = comp
        if prev is None or prev.get("state") != comp["state"]:
            self._pending.append({"type": "component", "event": "state", "id": cid, "from": prev and prev.get("state"),
                                  "to": comp["state"], "reason": comp["reason"], "at": now})
        return comp

    def set_services(self, services: Dict[str, dict]) -> None:
        self.services = services

    # ── the tick ───────────────────────────────────────────────────
    def tick(self, now: float) -> List[dict]:
        events, self._pending = self._pending, []
        events += self.eva.tick(now)
        meas.cross_check(self.tracker, now)
        self._measurements = meas.evaluate(self.tracker, now)
        conditions = self.rules.evaluate(now, self.tracker, self._measurements, self.eva, self.services, self.components)
        conditions.update(self.extra_conditions)
        events += self.alarms.evaluate(conditions, now)
        alarms = self.alarms.open()
        self._subsystems = subsystems(now, self.tracker, self._measurements, self.eva, self.services,
                                      self.components, alarms)
        self._mode = mission_mode(self._subsystems, alarms)
        self.last_tick = now
        return events

    # ── views ──────────────────────────────────────────────────────
    def summary(self, now: float) -> dict:
        return {"at": now, **self._mode, "subsystems": self._subsystems,
                "alarms": [a.to_dict() for a in self.alarms.open()],
                "eva": [t.snapshot(now) for t in self.eva.crews.values() if t.armed],
                "counts": dict(self.counts), "last_tick": self.last_tick}

    def streams(self, now: float) -> List[dict]:
        return sorted((s.snapshot(now) for s in self.tracker.streams.values()), key=lambda d: d["key"])

    def measurements(self) -> List[dict]:
        return self._measurements

    def eva_status(self, now: float) -> List[dict]:
        return [t.snapshot(now) for t in sorted(self.eva.crews.values(), key=lambda t: t.crew)]
