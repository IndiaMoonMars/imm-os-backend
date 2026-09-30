"""
Subsystem status and the mission mode.

Each subsystem is GO, DEGRADED or NO_GO, from what the monitoring can still do
(capability) and the alarms that are open:

  atmosphere:<node>/<zone>  NO_GO when CO₂ or O₂ monitoring is lost; DEGRADED when any
                            measurement is degraded or lost, or an atmosphere caution/warning
                            is active; NO_GO when an atmosphere emergency is active
  node:<node>               NO_GO when the node is offline; DEGRADED on weak power,
                            heat, failed services, a component in SAFE/DEGRADED mode
  eva                       NO_GO when a crew member is in LOS or CONTINGENCY or has an
                            emergency vitals alarm; DEGRADED on LOS_WARN, partial loss or warnings
  pipeline                  NO_GO when a critical MCC service is down or processing is
                            stalled; DEGRADED when any other service is unhealthy
Mission mode:
  EMERGENCY   an emergency alarm is active (real data, not the simulator)
  DEGRADED    a subsystem is not GO, or a warning is active
  NOMINAL     everything GO
Simulated data and simulated alarms never change the mode.
"""
from typing import Dict, List

from services.health.alarms import RANK

ORDER = {"GO": 0, "DEGRADED": 1, "NO_GO": 2}


def _worse(a: str, b: str) -> str:
    return a if ORDER[a] >= ORDER[b] else b


def subsystems(now: float, tracker, measurements: List[dict], eva, services: dict,
               components: Dict[str, dict], alarms: List) -> List[dict]:
    subs: Dict[str, dict] = {}

    def sub(sid: str, name: str, category: str) -> dict:
        return subs.setdefault(sid, {"id": sid, "name": name, "category": category, "status": "GO", "reasons": []})

    def mark(s: dict, status: str, reason: str):
        s["status"] = _worse(s["status"], status)
        if reason and reason not in s["reasons"]:
            s["reasons"].append(reason)

    for m in measurements:
        if m.get("simulated") and m["status"] != "LOST":
            continue
        if all(src["simulated"] for src in m["sources"]):
            continue
        s = sub(f"atmosphere:{m['node_id']}/{m['zone']}", f"Atmosphere {m['zone']} ({m['node_id']})", "atmosphere")
        if m["status"] == "LOST":
            mark(s, "NO_GO" if m["critical"] else "DEGRADED", f"{m['measurement']} monitoring lost")
        elif m["status"] == "DEGRADED":
            mark(s, "DEGRADED", f"{m['measurement']}: {m['reason']}")

    for st, (status, reasons) in tracker.live(now):
        if st.key.simulated or st.key.sensor != "sysmon":
            continue
        s = sub(f"node:{st.key.node}", f"Node {st.key.node}", "node")
        if status == "offline":
            mark(s, "NO_GO", "not reporting")
        elif status == "stale":
            mark(s, "DEGRADED", "late")
    for comp in components.values():
        s = sub(f"node:{comp.get('node_id')}", f"Node {comp.get('node_id')}", "node")
        if comp.get("state") in ("SAFE", "FAULT"):
            mark(s, "DEGRADED", f"{comp.get('component')} {comp.get('state')}")
        elif comp.get("state") in ("DEGRADED", "ISOLATED"):
            mark(s, "DEGRADED", f"{comp.get('component')} {comp.get('state')}")

    armed = [t for t in eva.crews.values() if t.armed]
    if armed:
        s = sub("eva", "EVA", "eva")
        for t in armed:
            if t.state in ("LOS", "CONTINGENCY"):
                mark(s, "NO_GO", f"{t.crew} {t.state}")
            elif t.state == "LOS_WARN":
                mark(s, "DEGRADED", f"{t.crew} LOS warning")

    if services:
        s = sub("pipeline", "MCC data pipeline", "pipeline")
        for name, st in services.items():
            if st.get("ok") is False and st.get("failures", 0) >= 2:
                mark(s, "NO_GO" if st.get("critical") else "DEGRADED", f"{name} down")
            if st.get("stalled"):
                mark(s, "NO_GO", f"{name} stalled")

    for a in alarms:
        if a.simulated or a.state != "active":
            continue
        target = None
        if a.category == "atmosphere" and "/" in a.source:
            node, zone = a.source.split("/", 1)
            target = subs.get(f"atmosphere:{node}/{zone}")
        elif a.category == "eva":
            target = subs.get("eva")
        elif a.category in ("node", "power") and a.source:
            target = subs.get(f"node:{a.source.split('|')[0]}")
        if target is None:
            continue
        if a.severity == "emergency":
            mark(target, "NO_GO", a.message)
        elif a.severity in ("warning", "caution"):
            mark(target, "DEGRADED", a.message)
    return sorted(subs.values(), key=lambda s: (-ORDER[s["status"]], s["id"]))


def mission_mode(subs: List[dict], alarms: List) -> dict:
    real_active = [a for a in alarms if a.state == "active" and not a.simulated]
    worst = max((RANK[a.severity] for a in real_active), default=-1)
    if worst >= RANK["emergency"]:
        mode = "EMERGENCY"
    elif worst >= RANK["warning"] or any(s["status"] != "GO" for s in subs):
        mode = "DEGRADED"
    else:
        mode = "NOMINAL"
    return {
        "mode": mode,
        "subsystems_go": sum(1 for s in subs if s["status"] == "GO"),
        "subsystems_total": len(subs),
        "alarms_active": len(real_active),
        "alarms_unacked": sum(1 for a in alarms if not a.acked and not a.simulated),
        "worst_severity": None if worst < 0 else ("advisory", "caution", "warning", "emergency")[worst],
    }
