"""
Alarm lifecycle (ISA-18.2 style, simplified).

The rules produce, every tick, the set of alarm conditions that are currently true,
each under a stable key ("limit.co2.node-rpi-01.zone_a", "eva.los.ev1"...). The manager
turns that into alarm state:

  (condition true for on_delay_s)          → ACTIVE, unacknowledged   event "raised"
  severity changes while active            → severity updated         event "escalated"/"deescalated"
                                             (an escalation needs a new acknowledgement)
  (condition false for off_delay_s)        → cleared:
        acknowledged           → CLOSED                               event "cleared" + "closed"
        not acknowledged       → RTN (returned to normal, still shown) event "cleared"
  RTN and the condition comes back         → ACTIVE again                event "reraised"
  acknowledge                              → ACTIVE+acked, or RTN → CLOSED

One open alarm per key, so a condition that stays true is one alarm, not one per
reading. Severities: advisory < caution < warning < emergency.
"""
import itertools
from dataclasses import dataclass, field
from typing import Dict, List, Optional

SEVERITIES = ("advisory", "caution", "warning", "emergency")
RANK = {s: i for i, s in enumerate(SEVERITIES)}
CATEGORIES = ("atmosphere", "eva", "sensor", "node", "pipeline", "power", "data")


@dataclass
class Condition:
    severity: str
    category: str
    source: str
    message: str
    value: Optional[float] = None
    unverified: bool = False          # raised on suspect data
    simulated: bool = False           # raised on simulator data (never changes the mission mode)
    details: dict = field(default_factory=dict)
    on_delay_s: float = 0.0           # must stay true this long before it is raised
    off_delay_s: float = 0.0          # must stay false this long before it clears


@dataclass
class Alarm:
    key: str
    severity: str
    category: str
    source: str
    message: str
    raised_at: float
    state: str = "active"             # active | rtn | closed
    acked: bool = False
    acked_at: Optional[float] = None
    acked_by: Optional[str] = None
    cleared_at: Optional[float] = None
    value: Optional[float] = None
    unverified: bool = False
    simulated: bool = False
    raise_count: int = 1
    details: dict = field(default_factory=dict)
    off_delay_s: float = 0.0
    false_since: Optional[float] = None
    db_id: Optional[int] = None
    local_id: int = 0

    def to_dict(self) -> dict:
        return {
            "id": self.db_id or self.local_id, "key": self.key, "severity": self.severity,
            "category": self.category, "source": self.source, "message": self.message, "state": self.state,
            "acked": self.acked, "acked_at": self.acked_at, "acked_by": self.acked_by,
            "raised_at": self.raised_at, "cleared_at": self.cleared_at, "value": self.value,
            "unverified": self.unverified, "simulated": self.simulated, "raise_count": self.raise_count,
            "details": self.details,
        }


class AlarmManager:
    def __init__(self):
        self.alarms: Dict[str, Alarm] = {}       # open alarms (active or rtn) by key
        self._pending: Dict[str, float] = {}     # key → since when the condition has been true
        self._ids = itertools.count(1)

    # ── queries ────────────────────────────────────────────────────
    def is_active(self, key: str) -> bool:
        a = self.alarms.get(key)
        return a is not None and a.state == "active"

    def open(self) -> List[Alarm]:
        return sorted(self.alarms.values(), key=lambda a: (-RANK[a.severity], a.acked, -a.raised_at))

    def find(self, alarm_id: int) -> Optional[Alarm]:
        return next((a for a in self.alarms.values() if alarm_id in (a.db_id, a.local_id)), None)

    # ── restore after a restart ────────────────────────────────────
    def restore(self, alarm: Alarm) -> None:
        alarm.local_id = next(self._ids)
        self.alarms[alarm.key] = alarm

    # ── evaluation ─────────────────────────────────────────────────
    def evaluate(self, conditions: Dict[str, Condition], now: float) -> List[dict]:
        events: List[dict] = []
        for key, c in conditions.items():
            a = self.alarms.get(key)
            if a is None:
                since = self._pending.setdefault(key, now)
                if now - since < c.on_delay_s:
                    continue
                self._pending.pop(key, None)
                a = Alarm(key=key, severity=c.severity, category=c.category, source=c.source, message=c.message,
                          raised_at=now, value=c.value, unverified=c.unverified, simulated=c.simulated,
                          details=dict(c.details), off_delay_s=c.off_delay_s, local_id=next(self._ids))
                self.alarms[key] = a
                events.append(self._event("raised", a, now))
                continue
            a.false_since = None
            a.value, a.details, a.off_delay_s = c.value, dict(c.details), c.off_delay_s
            if a.state == "rtn":
                a.state, a.acked, a.acked_at, a.acked_by, a.cleared_at = "active", False, None, None, None
                a.raise_count += 1
                a.severity, a.message, a.unverified = c.severity, c.message, c.unverified
                events.append(self._event("reraised", a, now))
                continue
            if c.severity != a.severity:
                up = RANK[c.severity] > RANK[a.severity]
                a.severity, a.message, a.unverified = c.severity, c.message, c.unverified
                if up:
                    a.acked, a.acked_at, a.acked_by = False, None, None
                events.append(self._event("escalated" if up else "deescalated", a, now))
            elif c.message != a.message or c.unverified != a.unverified:
                a.message, a.unverified = c.message, c.unverified
        for key in list(self._pending):
            if key not in conditions:
                del self._pending[key]
        for key, a in list(self.alarms.items()):
            if key in conditions or a.state != "active":
                continue
            if a.false_since is None:
                a.false_since = now
            if now - a.false_since < a.off_delay_s:
                continue
            a.cleared_at = now
            if a.acked:
                a.state = "closed"
                del self.alarms[key]
                events.append(self._event("cleared", a, now))
                events.append(self._event("closed", a, now))
            else:
                a.state = "rtn"
                events.append(self._event("cleared", a, now))
        return events

    def acknowledge(self, alarm: Alarm, user: str, now: float) -> List[dict]:
        if alarm.acked and alarm.state == "active":
            return []
        alarm.acked, alarm.acked_at, alarm.acked_by = True, now, user
        events = [self._event("acked", alarm, now, actor=user)]
        if alarm.state == "rtn":
            alarm.state = "closed"
            self.alarms.pop(alarm.key, None)
            events.append(self._event("closed", alarm, now, actor=user))
        return events

    @staticmethod
    def _event(kind: str, a: Alarm, now: float, actor: Optional[str] = None) -> dict:
        return {"type": "alarm", "event": kind, "at": now, "actor": actor, "alarm": a.to_dict(), "_alarm": a}
