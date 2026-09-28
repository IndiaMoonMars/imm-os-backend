"""
Health monitor logic (services/health): FDIR detection, isolation, alarms, EVA LOS, modes.
Time is driven explicitly, so every threshold in imm-os-docs/fdir-strategy.md is checked.
"""
import pytest

from services.health.alarms import AlarmManager, Condition
from services.health.eva import EvaMonitor
from services.health.monitor import HealthMonitor
from services.health.streams import StreamTracker

T0 = 1_800_000_000.0


def reading(sensor="bme280", node="node-rpi-01", zone="zone_a", t=T0, sim=False, q="good", qf=None, **metrics):
    base = {"bme280": {"temp": 22.0, "hum": 45.0, "pres": 1008.0},
            "scd40": {"co2_ppm": 600.0, "temp": 23.5, "hum": 42.0},
            "o2": {"o2_pct": 20.9},
            "sysmon": {"cpu_temp": 50.0, "undervolt": 0, "svc_failed": 0, "svc_restarts": 0}}.get(sensor, {})
    return {"sensor": sensor, "node_id": node, "zone": zone, "timestamp": t, "simulated": sim,
            "q": q, "qf": qf or [], "delayed": False, **base, **metrics}


def feed_series(mon, sensor, start, end, period=5.0, **kw):
    t = start
    while t <= end:
        mon.ingest_reading({"data": reading(sensor, t=t, **kw)}, t)
        t += period
    return t


# ── streams ─────────────────────────────────────────────────────────

def test_stream_goes_stale_then_offline():
    tr = StreamTracker()
    for i in range(10):
        tr.feed(reading(t=T0 + 5 * i), T0 + 5 * i)
    s = next(iter(tr.streams.values()))
    last = T0 + 45
    assert s.assess(last + 10)[0] == "ok"
    assert s.period() == 5.0 and s.stale_after() == 15.0 and s.offline_after() == 120.0
    assert s.assess(last + 16)[0] == "stale"
    assert s.assess(last + 121)[0] == "offline"


def test_sequence_gaps_are_counted_and_late_arrivals_recover_them():
    tr = StreamTracker()
    events = []
    for seq in (0, 1, 2, 6, 7):                        # 3, 4, 5 lost on the way
        _, ev = tr.feed(reading(t=T0 + seq, seq=seq, run="r1"), T0 + seq)
        events += ev
    s = next(iter(tr.streams.values()))
    assert s.integrity.lost == 3 and events == [
        {"type": "gap", "stream": s.key.id(), "run": "r1", "from": 3, "to": 5, "lost": 3}]
    tr.feed(reading(t=T0 + 4, seq=4, run="r1", delayed=True), T0 + 20)   # blackbox replay of #4
    assert (s.integrity.lost, s.integrity.recovered) == (2, 1)
    tr.feed(reading(t=T0 + 7, seq=7, run="r1"), T0 + 21)                  # duplicate
    assert s.integrity.duplicates == 1
    tr.feed(reading(t=T0 + 30, seq=0, run="r2"), T0 + 30)                 # driver restarted: new run, no gap
    assert s.integrity.lost == 2


def test_late_data_does_not_make_a_dead_stream_look_alive():
    tr = StreamTracker()
    tr.feed(reading(t=T0), T0)
    s = next(iter(tr.streams.values()))
    tr.feed(reading(t=T0 + 1, delayed=True), T0 + 200)
    assert s.assess(T0 + 200)[0] == "offline" and s.delayed_count == 1


def test_stuck_value_detected_after_ten_minutes():
    tr = StreamTracker()
    t = T0
    while t <= T0 + 590:
        tr.feed(reading(t=t, temp=22.0), t)
        t += 10
    s = next(iter(tr.streams.values()))
    assert s.stuck_metric() is None                    # 9 min 50 s
    tr.feed(reading(t=T0 + 600, temp=22.0), T0 + 600)
    assert s.stuck_metric() in ("temp", "hum", "pres")
    assert s.assess(T0 + 600) == ("suspect", ["stuck"])
    tr.feed(reading(t=T0 + 610, temp=22.01, hum=45.1, pres=1008.1), T0 + 610)   # noise again
    assert s.stuck_metric() is None


def test_rate_glitch_marks_suspect_for_a_minute():
    tr = StreamTracker()
    tr.feed(reading(t=T0, temp=22.0), T0)
    tr.feed(reading(t=T0 + 1, temp=85.0), T0 + 1)      # +63 °C in 1 s: a corrupted read
    s = next(iter(tr.streams.values()))
    assert s.assess(T0 + 2) == ("suspect", ["rate"])
    tr.feed(reading(t=T0 + 70, temp=22.1), T0 + 70)
    assert s.assess(T0 + 70)[0] == "ok"


def test_dead_letters_make_a_stream_bad():
    tr = StreamTracker()
    tr.feed(reading("scd40", t=T0), T0)
    for i in range(5):
        tr.reject({"mqtt_topic": "habitat/sensors/scd40/zone_a", "reason": "co2_ppm: ensure this value is >= 0",
                   "raw": '{"sensor":"scd40","node_id":"node-rpi-01","co2_ppm":-5}'}, T0 + 1 + i)
    s = next(iter(tr.streams.values()))
    assert s.assess(T0 + 6) == ("bad", ["invalid"])
    assert s.invalid_reason.startswith("co2_ppm")


# ── redundancy, cross-checks ───────────────────────────────────────

def test_failover_to_backup_temperature_source():
    mon = HealthMonitor()
    feed_series(mon, "bme280", T0, T0 + 60)
    feed_series(mon, "scd40", T0, T0 + 300)
    mon.tick(T0 + 60)
    temp = next(m for m in mon.measurements() if m["measurement"] == "temperature")
    assert (temp["status"], temp["source_sensor"], temp["value"]) == ("NOMINAL", "bme280", 22.0)
    mon.tick(T0 + 300)                                  # BME280 silent for 4 min
    temp = next(m for m in mon.measurements() if m["measurement"] == "temperature")
    assert (temp["status"], temp["source_sensor"], temp["value"]) == ("DEGRADED", "scd40", 23.5)
    assert temp["reason"] == "using backup scd40"
    keys = {a.key for a in mon.alarms.open()}
    assert "sensor.node-rpi-01.bme280.zone_a.offline" in keys
    assert not any(k.startswith("limit.temperature") for k in keys)


def test_loss_of_critical_monitoring_is_no_go():
    mon = HealthMonitor()
    feed_series(mon, "o2", T0, T0 + 30)
    feed_series(mon, "bme280", T0, T0 + 400)
    mon.tick(T0 + 400)
    o2 = next(m for m in mon.measurements() if m["measurement"] == "o2")
    assert o2["status"] == "LOST"
    for t in range(0, 12):
        mon.tick(T0 + 400 + t)
    assert "monitoring.o2.node-rpi-01.zone_a" in {a.key for a in mon.alarms.open()}
    atm = next(s for s in mon.summary(T0 + 412)["subsystems"] if s["id"] == "atmosphere:node-rpi-01/zone_a")
    assert atm["status"] == "NO_GO"


def test_uncalibrated_o2_is_caught_by_the_co2_cross_check():
    """The real board: O₂ reads 18.3 % while CO₂ 543 ppm says the air is normal."""
    mon = HealthMonitor()
    t = T0
    while t <= T0 + 120:
        mon.ingest_reading({"data": reading("o2", t=t, o2_pct=18.3, q="suspect", qf=["uncalibrated"])}, t)
        mon.ingest_reading({"data": reading("scd40", t=t, co2_ppm=543.0)}, t)
        mon.tick(t)
        t += 5
    o2 = next(m for m in mon.measurements() if m["measurement"] == "o2")
    assert o2["status"] == "DEGRADED" and o2["quality"] == "suspect"
    alarms = {a.key: a for a in mon.alarms.open()}
    low = alarms["limit.o2.node-rpi-01.zone_a"]
    assert low.severity == "warning" and low.unverified and "UNVERIFIED" in low.message
    assert "sensor.node-rpi-01.o2.zone_a.cross.o2_vs_co2" in alarms
    assert mon.summary(t)["mode"] == "DEGRADED"          # an unverified reading never declares an emergency


# ── alarm lifecycle ────────────────────────────────────────────────

def test_alarm_on_delay_dedup_ack_and_return_to_normal():
    am = AlarmManager()
    cond = {"k": Condition("caution", "atmosphere", "n/z", "CO₂ high", on_delay_s=5)}
    assert am.evaluate(cond, T0) == []                  # waiting out the on-delay
    ev = am.evaluate(cond, T0 + 5)
    assert [e["event"] for e in ev] == ["raised"]
    for t in range(6, 60):
        assert am.evaluate(cond, T0 + t) == []           # one alarm, not one per reading
    ev = am.evaluate({}, T0 + 61)
    assert [e["event"] for e in ev] == ["cleared"] and am.alarms["k"].state == "rtn"
    ev = am.evaluate(cond, T0 + 62)
    assert [e["event"] for e in ev] == ["reraised"] and am.alarms["k"].raise_count == 2
    ev = am.acknowledge(am.alarms["k"], "ev1", T0 + 63)
    assert [e["event"] for e in ev] == ["acked"] and am.alarms["k"].acked
    ev = am.evaluate({}, T0 + 64)
    assert [e["event"] for e in ev] == ["cleared", "closed"] and "k" not in am.alarms


def test_escalation_needs_a_new_acknowledgement_and_off_delay_debounces():
    am = AlarmManager()
    am.evaluate({"k": Condition("caution", "eva", "ev1", "LOS warn", off_delay_s=10)}, T0)
    am.acknowledge(am.alarms["k"], "mcc", T0 + 1)
    ev = am.evaluate({"k": Condition("warning", "eva", "ev1", "LOS", off_delay_s=10)}, T0 + 2)
    assert [e["event"] for e in ev] == ["escalated"] and not am.alarms["k"].acked
    assert am.evaluate({}, T0 + 3) == [] and am.evaluate({}, T0 + 12) == []   # chatter filtered
    assert [e["event"] for e in am.evaluate({}, T0 + 13)] == ["cleared"]


# ── limits with hysteresis ─────────────────────────────────────────

def co2_run(mon, values, start):
    t = start
    for v in values:
        mon.ingest_reading({"data": reading("scd40", t=t, co2_ppm=v)}, t)
        mon.tick(t)
        t += 5
    return t


def test_co2_limit_hysteresis_and_escalation():
    mon = HealthMonitor()
    t = co2_run(mon, [600] * 3 + [1100] * 3, T0)
    a = mon.alarms.alarms["limit.co2.node-rpi-01.zone_a"]
    assert a.severity == "caution" and a.value == 1100
    t = co2_run(mon, [980] * 4, t)                       # inside the limit but within the 50 ppm deadband
    assert mon.alarms.alarms["limit.co2.node-rpi-01.zone_a"].state == "active"
    t = co2_run(mon, [5600] * 2, t)
    assert mon.alarms.alarms["limit.co2.node-rpi-01.zone_a"].severity == "warning"
    t = co2_run(mon, [900] * 4, t)                       # below 950: clears after the off-delay
    assert mon.alarms.alarms["limit.co2.node-rpi-01.zone_a"].state == "rtn"


def test_bad_data_never_raises_limit_alarms_and_simulated_alarms_do_not_change_the_mode():
    mon = HealthMonitor()
    t = T0
    for _ in range(6):
        mon.ingest_reading({"data": reading("scd40", t=t, co2_ppm=30000, q="bad", qf=["source_fault"])}, t)
        mon.tick(t)
        t += 5
    assert not any(k.startswith("limit.co2") for k in mon.alarms.alarms)
    sim = HealthMonitor()
    t = T0
    for _ in range(6):
        sim.ingest_reading({"data": reading("scd40", node="node-rpi-02", t=t, sim=True, co2_ppm=25000)}, t)
        sim.tick(t)
        t += 5
    a = sim.alarms.alarms["limit.co2.node-rpi-02.zone_a"]
    assert a.severity == "emergency" and a.simulated
    assert sim.summary(t)["mode"] == "NOMINAL"


# ── nodes, boards, components ──────────────────────────────────────

def test_node_offline_and_automatic_restarts_are_reported():
    mon = HealthMonitor()
    t = T0
    for r in (0, 0, 1, 2, 3):
        mon.ingest_reading({"data": reading("sysmon", t=t, svc_restarts=r)}, t)
        mon.tick(t)
        t += 10
    a = mon.alarms.alarms["node.node-rpi-01.restarts"]
    assert a.severity == "caution" and a.value == 3
    mon.tick(t + 200)
    assert "node.node-rpi-01.offline" in mon.alarms.alarms
    node = next(s for s in mon.summary(t + 200)["subsystems"] if s["id"] == "node:node-rpi-01")
    assert node["status"] == "NO_GO"


def test_board_brownouts_and_bme280_resets():
    mon = HealthMonitor()
    t = T0
    for resets, boots, reason in ((51, 3, 1), (60, 3, 1), (70, 4, 9)):
        mon.ingest_reading({"data": {"sensor": "board", "node_id": "node-rpi-01", "zone": "zone1", "timestamp": t,
                                     "q": "good", "qf": [], "uptime_s": 100, "bme_resets": resets,
                                     "boot_count": boots, "reset_reason": reason, "i2c_err": 0}}, t)
        mon.tick(t)
        t += 10
    keys = {a.key: a for a in mon.alarms.open()}
    assert "BME280 lost power 19 times" in keys["board.node-rpi-01.zone1.bme_resets"].message
    assert "brownout" in keys["board.node-rpi-01.zone1.reboot"].message


def test_component_safe_mode_and_silence():
    mon = HealthMonitor()
    mon.ingest_component({"state": "SAFE", "reason": "no fresh temperature/humidity: relays OFF", "interval_s": 60},
                         T0, "habitat/health/node-rpi-01/eclss_pid")
    mon.tick(T0)
    a = mon.alarms.alarms["component.node-rpi-01/eclss_pid.state"]
    assert a.severity == "warning" and "relays OFF" in a.message
    mon.tick(T0 + 200)
    assert "component.node-rpi-01/eclss_pid.silent" in mon.alarms.alarms


def test_component_status_is_validated():
    mon = HealthMonitor()
    assert mon.ingest_component({"state": "EXPLODED"}, T0, "habitat/health/n/c") is None
    assert mon.ingest_component(["x"], T0, "habitat/health/n/c") is None
    assert mon.ingest_component({"state": "nominal"}, T0, "habitat/health/n/c")["state"] == "NOMINAL"


# ── EVA loss of signal ─────────────────────────────────────────────

def vitals(crew="ev1", t=T0, **kw):
    return {"sensor": "eva_biosensor", "crew_id": crew, "zone": "eva", "node_id": "suit-ev1", "timestamp": t,
            "hr_bpm": 92.0, "spo2_pct": 98.0, "skin_temp_c": 33.5, "q": "good", "qf": [], **kw}


def position(crew="ev1", t=T0, x=10.0, y=5.0, **kw):
    return {"crew_id": crew, "mode": "uwb", "x_m": x, "y_m": y, "quality": 90.0, "timestamp": t, "q": "good", **kw}


def test_eva_auto_arm_and_los_escalation_timeline():
    mon = HealthMonitor()
    for i in range(10):
        t = T0 + i
        mon.ingest_reading({"data": vitals(t=t)}, t)
        mon.ingest_reading({"data": position(t=t, x=10 + i, y=5)}, t)
        mon.tick(t)
    track = mon.eva.crews["ev1"]
    assert track.armed and track.armed_by == "auto" and track.state == "NOMINAL"
    last = T0 + 9
    states = {}
    for dt in range(1, 131):
        mon.tick(last + dt)
        states.setdefault(track.state, dt)
    assert states == {"NOMINAL": 1, "LOS_WARN": 10, "LOS": 30, "CONTINGENCY": 120}
    alarm = mon.alarms.alarms["eva.los.ev1"]
    assert alarm.severity == "emergency" and "EVA CONTINGENCY" in alarm.message
    snap = track.snapshot(last + 130)
    assert snap["speed_ms"] == pytest.approx(1.0, abs=0.01)          # it was walking 1 m/s
    assert snap["search_radius_m"] == pytest.approx(1.4 * 130, abs=1)  # at least walking pace × time
    assert mon.summary(last + 130)["mode"] == "EMERGENCY"
    # contact returns
    mon.ingest_reading({"data": vitals(t=last + 131)}, last + 131)
    events = mon.tick(last + 131)
    rec = next(e for e in events if e.get("event") == "recovered")
    assert rec["outage_s"] == 131 and rec["worst"] == "CONTINGENCY"
    assert mon.alarms.alarms["eva.los.ev1"].state == "rtn"          # cleared, waiting for acknowledgement
    assert track.outages[-1]["worst"] == "CONTINGENCY"


def test_eva_partial_loss_and_vitals_limits():
    mon = HealthMonitor()
    for i in range(25):
        t = T0 + i
        mon.ingest_reading({"data": position(t=t)}, t)
        if i < 5:
            mon.ingest_reading({"data": vitals(t=t, spo2_pct=89.0)}, t)
        mon.tick(t)
    keys = {a.key: a for a in mon.alarms.open()}
    assert keys["eva.vitals.ev1.spo2_pct"].severity == "emergency"
    assert "eva.vitals_lost.ev1" in keys and mon.eva.crews["ev1"].state == "NOMINAL"


def test_eva_disarm_stops_monitoring_and_simulated_suits_do_not_arm():
    mon = HealthMonitor()
    mon.ingest_reading({"data": vitals(t=T0)}, T0)
    mon.eva.disarm("ev1", T0 + 1)
    mon.tick(T0 + 500)
    assert mon.eva.crews["ev1"].state == "DISARMED" and "eva.los.ev1" not in mon.alarms.alarms
    mon.ingest_reading({"data": vitals(crew="ev9", t=T0, simulated=True)}, T0)
    assert not mon.eva.crews["ev9"].armed


def test_eva_armed_by_mcc_reaches_los_even_if_the_suit_never_reports():
    ev = EvaMonitor()
    ev.arm("ev2", T0, by="plan")
    for dt in range(0, 31):
        ev.tick(T0 + dt)
    assert ev.crews["ev2"].state == "LOS"


def test_eva_store_and_forward_backfill_is_counted():
    mon = HealthMonitor()
    for i in range(5):
        mon.ingest_reading({"data": vitals(t=T0 + i)}, T0 + i)
        mon.tick(T0 + i)
    for dt in range(1, 40):
        mon.tick(T0 + 4 + dt)                              # 39 s outage → LOS
    mon.ingest_reading({"data": vitals(t=T0 + 44)}, T0 + 44)
    mon.tick(T0 + 44)
    for i in range(5, 44, 1):                             # the suit's backlog arrives late
        mon.ingest_reading({"data": vitals(t=T0 + i, delayed=True)}, T0 + 45)
    outage = mon.eva.crews["ev1"].outages[-1]
    assert outage["worst"] == "LOS" and outage["backfilled"] >= 30


def test_methane_limits_and_no_alarm_from_late_data():
    mon = HealthMonitor()
    t = T0
    for v in (800, 800, 6200, 6200, 6200, 13000, 13000, 13000):
        mon.ingest_reading({"data": {"sensor": "mq4", "node_id": "node-rpi-01", "zone": "zone_a", "timestamp": t,
                                     "ch4_ppm": float(v), "q": "good", "qf": []}}, t)
        mon.tick(t)
        t += 5
    a = mon.alarms.alarms["limit.methane.node-rpi-01.zone_a"]
    assert a.severity == "emergency" and a.value == 13000
    late = HealthMonitor()
    for i in range(6):
        late.ingest_reading({"data": {"sensor": "mq4", "node_id": "n", "zone": "z", "timestamp": T0 - 900 + i,
                                      "ch4_ppm": 13000.0, "q": "good", "delayed": True}}, T0 + i)
        late.tick(T0 + i)
    assert not late.alarms.alarms                      # backlog from an outage: history, not a live alarm
