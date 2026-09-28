"""
Alarm conditions, evaluated every tick from the current health picture.

  limits      environmental and crew limits on the MEASUREMENT (the best available
              source, see measurements.py), so a failed sensor with a working backup
              neither hides a real limit nor raises a false one. Hysteresis: once
              raised, an alarm clears only when the value is back inside the limit by
              the deadband. Suspect data still alarms, marked "unverified" and capped
              at warning; bad or late data never raises a limit alarm.
  sensors     live sensor offline / bad / stuck / rate glitch / invalid data / cross-check
  monitoring  loss of a critical measurement (CO₂, O₂) in a zone
  nodes       node offline, weak power supply, overheating, failed services, disk,
              store-and-forward backlog, ESP32 board brownouts and BME280 resets,
              components in SAFE or DEGRADED mode
  pipeline    MCC services unhealthy, telemetry processing stalled, stores unreachable
  eva         loss of signal, partial loss, suit vitals limits
  integrity   readings lost between node and MCC (sequence gaps)

Numbers are the defaults in imm-os-docs/fdir-strategy.md (FDIR table).
"""
from typing import Dict, List, Optional

from services.health.alarms import Condition

# measurement → [(severity, direction, limit, deadband)]; direction "hi" raises above, "lo" below
LIMITS = {
    "co2": [("caution", "hi", 1000.0, 50.0), ("warning", "hi", 5000.0, 200.0), ("emergency", "hi", 20000.0, 500.0)],
    "o2": [("warning", "lo", 19.5, 0.2), ("emergency", "lo", 17.0, 0.2), ("warning", "hi", 23.5, 0.2)],
    "co": [("warning", "hi", 35.0, 5.0), ("emergency", "hi", 200.0, 10.0)],
    "methane": [("warning", "hi", 5000.0, 250.0), ("emergency", "hi", 12500.0, 500.0)],
    "temperature": [("caution", "lo", 10.0, 0.5), ("caution", "hi", 35.0, 0.5),
                    ("warning", "lo", 5.0, 0.5), ("warning", "hi", 40.0, 0.5)],
    "humidity": [("caution", "hi", 75.0, 3.0), ("caution", "lo", 15.0, 2.0)],
}
LABEL = {"co2": "CO₂", "o2": "O₂", "co": "CO", "methane": "Methane", "temperature": "Temperature",
         "humidity": "Humidity", "pressure": "Pressure"}
UNIT = {"co2": "ppm", "o2": "%", "co": "ppm", "methane": "ppm", "temperature": "°C", "humidity": "%RH"}

# EVA suit vitals (eva_biosensor per crew member)
VITALS = {
    "hr_bpm": [("warning", "hi", 160.0, 5.0), ("emergency", "hi", 185.0, 5.0), ("emergency", "lo", 40.0, 3.0)],
    "spo2_pct": [("warning", "lo", 94.0, 1.0), ("emergency", "lo", 90.0, 1.0)],
    "skin_temp_c": [("warning", "hi", 38.5, 0.3), ("emergency", "hi", 39.5, 0.3), ("caution", "lo", 30.0, 0.5)],
}
VITAL_LABEL = {"hr_bpm": ("heart rate", "bpm"), "spo2_pct": ("SpO₂", "%"), "skin_temp_c": ("skin temperature", "°C")}

# ESP32 reset reasons (esp_reset_reason_t) worth a word
RESET_REASONS = {1: "power-on", 3: "software", 4: "crash", 5: "interrupt watchdog", 6: "task watchdog",
                 7: "watchdog", 9: "brownout (supply dipped)"}

CRITICAL_SOURCES = {"scd40", "o2"}


def _limit_state(value: float, rules, active_sev) -> Optional[tuple]:
    """Highest limit breached, with hysteresis for the ones already active."""
    hit = None
    for sev, direction, limit, deadband in rules:
        key = (sev, direction, limit)
        held = key in active_sev
        edge = (limit - deadband if direction == "hi" else limit + deadband) if held else limit
        breached = value > edge if direction == "hi" else value < edge
        if breached and (hit is None or _rank(sev) > _rank(hit[0])):
            hit = (sev, direction, limit)
    return hit


def _rank(sev: str) -> int:
    return ("advisory", "caution", "warning", "emergency").index(sev)


class Rules:
    def __init__(self):
        self._limit_held: Dict[str, set] = {}       # alarm key → {(sev, dir, limit)} currently held
        self._board_last: Dict[str, dict] = {}      # stream → last counters seen
        self._board_events: Dict[str, List[tuple]] = {}
        self._svc_restarts: Dict[str, List[tuple]] = {}

    def _limits(self, key: str, value: float, rules) -> Optional[tuple]:
        held = self._limit_held.get(key, set())
        hit = _limit_state(value, rules, held)
        # hold every limit at or below the hit (so de-escalating also has hysteresis)
        self._limit_held[key] = set() if hit is None else {
            (s, d, lim) for s, d, lim, _ in rules if d == hit[1] and _rank(s) <= _rank(hit[0])}
        return hit

    def evaluate(self, now: float, tracker, measurements: List[dict], eva, services: dict,
                 components: Dict[str, dict]) -> Dict[str, Condition]:
        c: Dict[str, Condition] = {}
        self._measurement_rules(c, measurements)
        self._sensor_rules(c, tracker, now)
        self._node_rules(c, tracker, components, now)
        self._pipeline_rules(c, services)
        self._eva_rules(c, eva, tracker, now)
        return c

    # ── habitat air ────────────────────────────────────────────────
    def _measurement_rules(self, c, measurements):
        for m in measurements:
            name, node, zone = m["measurement"], m["node_id"], m["zone"]
            where = f"{node} {zone}"
            if m["status"] == "LOST" and m["critical"] and not all(s["simulated"] for s in m["sources"]):
                c[f"monitoring.{name}.{node}.{zone}"] = Condition(
                    "warning", "atmosphere", f"{node}/{zone}",
                    f"Loss of {LABEL[name]} monitoring in {where} ({m['reason']}): use a portable monitor",
                    details={"sources": m["sources"]}, on_delay_s=10)
            elif m["status"] == "DEGRADED" and m["critical"] and not m.get("simulated"):
                c[f"monitoring.{name}.{node}.{zone}"] = Condition(
                    "advisory", "atmosphere", f"{node}/{zone}",
                    f"{LABEL[name]} monitoring degraded in {where}: {m['reason']}", on_delay_s=30, off_delay_s=30)
            rules = LIMITS.get(name)
            if not rules or m.get("value") is None:
                continue
            key = f"limit.{name}.{node}.{zone}"
            hit = self._limits(key, float(m["value"]), rules)
            if hit is None:
                continue
            sev, direction, limit = hit
            unverified = m.get("quality") == "suspect"
            if unverified and _rank(sev) > _rank("warning"):
                sev = "warning"
            word = "high" if direction == "hi" else "low"
            msg = f"{LABEL[name]} {word}: {m['value']:g} {UNIT.get(name, '')} in {where} (limit {limit:g})"
            if unverified:
                msg += f" UNVERIFIED: {m['reason'] or 'suspect sensor'}"
            c[key] = Condition(sev, "atmosphere", f"{node}/{zone}", msg, value=m["value"], unverified=unverified,
                               simulated=bool(m.get("simulated")), details={"source": m.get("source"), "limit": limit},
                               on_delay_s=5, off_delay_s=10)

    # ── sensors ────────────────────────────────────────────────────
    def _sensor_rules(self, c, tracker, now):
        for s, (status, reasons) in tracker.live(now):
            k = s.key
            if k.simulated or k.sensor in ("eva_position",) or k.crew:
                continue
            name = f"{k.sensor} on {k.node} ({k.zone})"
            base = f"sensor.{k.node}.{k.sensor}.{k.zone}"
            critical = k.sensor in CRITICAL_SOURCES
            if status == "offline" and s.last_live is not None:
                if k.sensor != "sysmon":         # a node's own health stream: see node rules
                    c[base + ".offline"] = Condition("warning" if critical else "caution", "sensor", k.id(),
                                                     f"Sensor offline: {name}, no data for {now - s.last_live:.0f} s")
            elif status == "bad":
                why = s.invalid_reason if "invalid" in reasons else ", ".join(reasons) or "bad quality"
                c[base + ".bad"] = Condition("caution", "sensor", k.id(), f"Sensor fault: {name}: {why}",
                                             on_delay_s=10, off_delay_s=30)
            if status in ("ok", "suspect", "bad"):
                if "stuck" in reasons:
                    c[base + ".stuck"] = Condition("caution", "sensor", k.id(),
                                                   f"Sensor stuck: {name}: {s.stuck_metric()} unchanged for over 10 min")
                if "rate" in reasons:
                    c[base + ".rate"] = Condition("advisory", "sensor", k.id(),
                                                  f"Implausible jump from {name} ({s.rate_metric}): check wiring", off_delay_s=0)
                for check, why in s.cross.items():
                    c[base + ".cross." + check] = Condition("caution", "sensor", k.id(), f"Cross-check failed: {why}",
                                                            on_delay_s=60, off_delay_s=60)
            # integrity: readings lost between node and MCC in the last 10 min
            if s.integrity.lost > 0 and s.integrity.last_gap_at and now - s.integrity.last_gap_at < 600:
                c[base + ".gap"] = Condition("advisory", "data", k.id(),
                                             f"Data loss: {s.integrity.lost} reading(s) from {name} never arrived",
                                             value=s.integrity.lost)

    # ── nodes ──────────────────────────────────────────────────────
    def _node_rules(self, c, tracker, components, now):
        for s, (status, _) in tracker.live(now):
            k = s.key
            if k.simulated:
                continue
            if k.sensor == "sysmon":
                node = k.node
                if status == "offline" and s.last_live is not None:
                    c[f"node.{node}.offline"] = Condition(
                        "warning", "node", node, f"Node {node} not reporting for {now - s.last_live:.0f} s (down, or its link to the MCC)")
                if status not in ("ok", "suspect"):
                    continue
                v = s.last
                if v.get("undervolt") == 1:
                    c[f"node.{node}.undervolt"] = Condition("caution", "power", node,
                                                            f"Node {node}: power supply too weak (under-voltage)", off_delay_s=30)
                if v.get("cpu_temp", 0) > 85:
                    c[f"node.{node}.hot"] = Condition("warning", "node", node, f"Node {node} overheating: {v['cpu_temp']:.0f} °C")
                elif v.get("cpu_temp", 0) > 80:
                    c[f"node.{node}.hot"] = Condition("caution", "node", node, f"Node {node} hot: {v['cpu_temp']:.0f} °C")
                if v.get("svc_failed", 0) > 0:
                    c[f"node.{node}.svc_failed"] = Condition("caution", "node", node,
                                                             f"Node {node}: {int(v['svc_failed'])} IMM-OS service(s) failed")
                if v.get("disk_pct", 0) > 97:
                    c[f"node.{node}.disk"] = Condition("warning", "node", node, f"Node {node} disk {v['disk_pct']:.0f} % full")
                elif v.get("disk_pct", 0) > 90:
                    c[f"node.{node}.disk"] = Condition("caution", "node", node, f"Node {node} disk {v['disk_pct']:.0f} % full")
                if v.get("mqtt_backlog", 0) > 10000:
                    c[f"node.{node}.backlog"] = Condition("caution", "data", node,
                                                          f"Node {node}: {int(v['mqtt_backlog'])} readings waiting to reach the MCC")
                # restarts: automatic recovery happened (the watchdog / systemd restarted something)
                r = v.get("svc_restarts")
                if r is not None:
                    hist = self._svc_restarts.setdefault(node, [])
                    if not hist or hist[-1][1] != r:
                        hist.append((now, r))
                    hist[:] = [h for h in hist if now - h[0] <= 900]
                    if len(hist) >= 2 and hist[-1][1] > hist[0][1]:
                        n = int(hist[-1][1] - hist[0][1])
                        c[f"node.{node}.restarts"] = Condition(
                            "caution" if n >= 3 else "advisory", "node", node,
                            f"Node {node}: {n} automatic service restart(s) in the last 15 min", value=n)
            elif k.sensor == "board" and status in ("ok", "suspect"):
                self._board_rules(c, s, now)
        for cid, comp in components.items():
            node, name, state = comp.get("node_id"), comp.get("component"), comp.get("state")
            age = now - comp.get("_received", now)
            if age > max(3 * comp.get("interval_s", 60), 180):
                c[f"component.{cid}.silent"] = Condition("caution", "node", cid,
                                                         f"{name} on {node}: no status for {age:.0f} s")
                continue
            if state in ("SAFE", "FAULT"):
                c[f"component.{cid}.state"] = Condition("warning", "node", cid,
                                                        f"{name} on {node} in {state} mode: {comp.get('reason') or ''}".strip())
            elif state in ("DEGRADED", "ISOLATED"):
                c[f"component.{cid}.state"] = Condition("caution" if state == "DEGRADED" else "advisory", "node", cid,
                                                        f"{name} on {node} {state}: {comp.get('reason') or ''}".strip())

    def _board_rules(self, c, s, now):
        k = s.key
        last = self._board_last.get(k.id(), {})
        cur = {m: s.last.get(m) for m in ("bme_resets", "boot_count", "reset_reason", "i2c_err")}
        ev = self._board_events.setdefault(k.id(), [])
        if last:
            if cur["bme_resets"] is not None and last.get("bme_resets") is not None and cur["bme_resets"] > last["bme_resets"]:
                ev.append((now, "bme", cur["bme_resets"] - last["bme_resets"]))
            if cur["boot_count"] is not None and last.get("boot_count") is not None and cur["boot_count"] > last["boot_count"]:
                ev.append((now, "boot", int(cur["reset_reason"] or 0)))
        self._board_last[k.id()] = cur
        ev[:] = [e for e in ev if now - e[0] <= 900]
        bme = int(sum(e[2] for e in ev if e[1] == "bme"))
        if bme >= 3:
            c[f"board.{k.node}.{k.zone}.bme_resets"] = Condition(
                "caution", "power", k.id(),
                f"ESP32 board on {k.node}: BME280 lost power {bme} times in 15 min: check the board's supply and the BME280's VIN/GND")
        boots = [e for e in ev if e[1] == "boot"]
        if boots:
            why = RESET_REASONS.get(boots[-1][2], f"reason {boots[-1][2]}")
            c[f"board.{k.node}.{k.zone}.reboot"] = Condition(
                "caution" if boots[-1][2] in (4, 5, 6, 7, 9) else "advisory", "node", k.id(),
                f"ESP32 board on {k.node} restarted ({why}), {len(boots)} time(s) in 15 min")

    # ── MCC services ───────────────────────────────────────────────
    def _pipeline_rules(self, c, services):
        for name, st in (services or {}).items():
            if st.get("ok") is False and st.get("failures", 0) >= 2:
                sev = "warning" if st.get("critical") else "caution"
                c[f"pipeline.{name}"] = Condition(sev, "pipeline", name, f"MCC service {name}: {st.get('error') or 'unhealthy'}")
            if st.get("stalled"):
                c[f"pipeline.{name}.stalled"] = Condition(
                    "warning", "pipeline", name, f"Telemetry processing stalled: {name} has {st.get('lag', 0)} unprocessed readings")

    # ── EVA ────────────────────────────────────────────────────────
    def _eva_rules(self, c, eva, tracker, now):
        for key, (sev, msg, snap) in eva.conditions(now).items():
            c[key] = Condition(sev, "eva", snap["crew_id"], msg, details={k: snap[k] for k in (
                "since_contact_s", "last_position", "last_vitals", "search_radius_m", "state")},
                simulated=snap["simulated"])
        for s, (status, _) in tracker.live(now):
            if s.key.sensor != "eva_biosensor" or status not in ("ok", "suspect"):
                continue
            crew = s.key.crew
            t = eva.crews.get(crew)
            if t is None or not t.armed:
                continue
            for metric, rules in VITALS.items():
                v = s.last.get(metric)
                if v is None:
                    continue
                key = f"eva.vitals.{crew}.{metric}"
                hit = self._limits(key, float(v), rules)
                if hit is None:
                    continue
                label, unit = VITAL_LABEL[metric]
                sev = hit[0]
                unverified = status == "suspect"
                if unverified and _rank(sev) > _rank("warning"):
                    sev = "warning"
                c[key] = Condition(sev, "eva", crew, f"EVA {crew}: {label} {v:g} {unit} (limit {hit[2]:g})"
                                   + (" UNVERIFIED" if unverified else ""), value=v, unverified=unverified,
                                   simulated=s.key.simulated, on_delay_s=3, off_delay_s=10)
