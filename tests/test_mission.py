"""Mission record: sols, IST, per-sol statistics, the API, downloads and the sol archiver."""
import asyncio
import csv
import json
import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from main import app
from services import mission as M
from services import mission_api as api
from services.auth import User, current_user

T0 = 1790457496.0            # Sun 27 Sep 2026, 02:48:16 IST


# ── pure logic ─────────────────────────────────────────────────────

def test_sols_are_24_h_from_t0_and_ist_is_shown():
    m = M.Mission(1, "Alpha", T0)
    assert M.ist(T0) == "Sun 27 Sep 2026, 02:48:16 IST"
    assert M.parse_ist("2026-09-27 02:48:16") == T0 and M.parse_ist("2026-09-27T02:48:16+05:30") == T0
    assert [M.sol_of(m, t) for t in (T0 - 1, T0, T0 + 86399, T0 + 86400, T0 + 7 * 86400)] == [0, 1, 1, 2, 8]
    assert M.sol_bounds(m, 3) == (T0 + 2 * 86400, T0 + 3 * 86400)
    now = T0 + 2 * 86400 + 14 * 3600 + 22 * 60 + 5                 # SOL 3 · 14:22:05
    c = M.clock(m, now)
    assert (c["phase"], c["sol"], c["sol_elapsed_s"]) == ("active", 3, 14 * 3600 + 22 * 60 + 5)
    assert c["progress"] == pytest.approx((2 + 14.3681 / 24) / 7, abs=1e-4)
    assert M.clock(m, T0 - 90)["phase"] == "pre" and M.clock(m, T0 - 90)["t_minus_s"] == 90
    assert M.clock(m, T0 + 8 * 86400)["phase"] == "complete"
    assert M.clock(None, now) == {"phase": "none"}
    states = [s["state"] for s in M.sol_states(m, now)]
    assert states == ["done", "done", "live", "upcoming", "upcoming", "upcoming", "upcoming"]


def test_ended_early_mission():
    m = M.Mission(1, "Alpha", T0, ended_at=T0 + 86400 + 3600)
    assert M.clock(m, T0 + 3 * 86400)["phase"] == "complete"
    assert [s["state"] for s in M.sol_states(m, T0 + 3 * 86400)][:3] == ["done", "done", "upcoming"]


def minutes(start, n, value=lambda i: 20.0, step=60, count=60):
    return {int(start + i * step): [value(i), value(i) - 0.5, value(i) + 0.5, count] for i in range(n)}


def test_stats_buckets_coverage_and_dose():
    s = minutes(T0, 120, lambda i: 20 + i / 10)
    st = M.stats(s)
    assert st["min"] == 19.5 and st["min_t"] == int(T0) and st["max"] == pytest.approx(32.4)
    assert st["mean"] == pytest.approx(20 + 119 / 20) and st["readings"] == 7200
    b = M.buckets(s, T0, 600)
    assert len(b) == 12 and b[0][0] == pytest.approx(5 / 60, abs=1e-3) and b[0][1] == pytest.approx(20.45)
    # 1 Hz sensor, readings in 90 of 120 minutes → 75 %; a 150 s sensor uses 5-min slots
    counts = {int(T0 + i * 60): 60 for i in range(120) if not 30 <= i < 60}
    assert M.coverage(counts, T0, T0 + 7200, 1) == 75.0
    slow = {int(T0 + i * 150): 1 for i in range(48)}                     # every 150 s for 2 h
    assert M.coverage(slow, T0, T0 + 7200, 150) == 100.0
    assert M.dose_usv(minutes(T0, 60, lambda i: 0.12)) == pytest.approx(0.12)     # 1 h at 0.12 µSv/h


def test_summary_prefers_bme280_and_counts_every_sensor():
    m = M.Mission(1, "Alpha", T0)
    values = {("bme280", "temp"): minutes(T0, 60, lambda i: 22.0), ("scd40", "temp"): minutes(T0, 60, lambda i: 25.0),
              ("scd40", "co2_ppm"): minutes(T0, 60, lambda i: 600 + i), ("geiger", "usv_h"): minutes(T0, 60, lambda i: 0.2)}
    counts = {("bme280", "node-rpi-01", "zone_a"): {int(T0 + i * 60): 60 for i in range(60)},
              ("geiger", "node-rpi-01", "exterior"): {int(T0 + i * 60): 60 for i in range(30)},
              ("o2", "vv-node", "test"): {int(T0): 60}}                   # a test node's minute: not averaged
    s = M.summary(m, 1, values, counts, T0 + 3600, {"bme280": 1, "geiger": 1})
    assert s["measurements"]["temperature"]["source"] == "bme280.temp" and s["measurements"]["temperature"]["mean"] == 22
    assert s["measurements"]["co2"]["max"] == 659.5 and s["measurements"]["o2"]["source"] is None
    assert {c["sensor"]: c["pct"] for c in s["coverage"]} == {"bme280": 100.0, "geiger": 50.0, "o2": 1.7}
    assert s["coverage_pct"] == 75.0 and s["dose_usv"] == pytest.approx(0.2) and s["live"]


def test_rollup_rows_and_archive_json_round_trip():
    rows = [{"result": r, "sensor": "bme280", "metric": "temp", "t": T0, "value": v, "series": ser}
            for ser, vals in (("a", (22.0, 21.5, 22.5, 60)), ("b", (24.0, 23.0, 25.0, 20)))
            for r, v in zip(("mean", "min", "max", "count"), vals)]
    values = M.values_from_series_rows(rows)                           # two series of one sensor, merged
    assert values == {("bme280", "temp"): {int(T0): [22.5, 21.5, 25.0, 80.0]}}
    # a warm-up minute is left out
    assert M.values_from_series_rows(rows, {("bme280", int(T0))}) == {}
    counts = M.counts_from_rows([{"sensor": "bme280", "node_id": "n1", "zone": "z", "t": T0, "value": 40},
                                 {"sensor": "bme280", "node_id": "n1", "zone": "z", "t": T0, "value": 20}])
    assert counts == {("bme280", "n1", "z"): {int(T0): 60}}
    assert M.rollup_from_json(json.loads(json.dumps(M.rollup_to_json(values, counts)))) == (values, counts)


def test_mission_coverage_ignores_test_nodes_and_counts_dead_sensors():
    cov = {1: [{"sensor": "bme280", "node_id": "n", "zone": "a", "pct": 100.0},
               {"sensor": "o2", "node_id": "vv", "zone": "t", "pct": 0.2}],            # a test node, a minute
           2: [{"sensor": "bme280", "node_id": "n", "zone": "a", "pct": 90.0},
               {"sensor": "geiger", "node_id": "n", "zone": "ext", "pct": 100.0}],     # added on Sol 2
           3: [{"sensor": "geiger", "node_id": "n", "zone": "ext", "pct": 50.0}]}      # the BME280 died
    rows, overall = M.mission_coverage(cov)
    assert [(r["sensor"], r["sols"]) for r in rows] == [("bme280", {1: 100.0, 2: 90.0, 3: 0.0}),
                                                       ("geiger", {2: 100.0, 3: 50.0})]
    assert overall == {1: 100.0, 2: 95.0, 3: 25.0}


def test_timeline_overlay_and_cumulative_dose():
    m = M.Mission(1, "Alpha", T0)
    per_sol = {n: {("scd40", "co2_ppm"): minutes(T0 + (n - 1) * 86400, 60, lambda i: 600 + 100 * n),
                   ("geiger", "usv_h"): minutes(T0 + (n - 1) * 86400, 60, lambda i: 0.1)} for n in (1, 2)}
    t = M.timeline(m, per_sol, "co2")
    assert t["source"] == "scd40.co2_ppm" and len(t["points"]) == 12
    assert t["points"][6][0] == pytest.approx(24 + 5 / 60, abs=1e-3) and t["points"][6][1] == 800
    o = M.overlay(m, per_sol, "co2")
    assert set(o["sols"]) == {1, 2} and o["sols"][2][0] == [pytest.approx(5 / 60, abs=1e-3), 800]
    d = M.dose_series(m, per_sol)
    assert d["per_sol"] == {1: 0.1, 2: 0.1} and d["total_usv"] == pytest.approx(0.2) and d["points"][-1][1] == 0.2


# ── the API, with an in-memory store and made-up rollups ───────────

class FakeStore:
    def __init__(self):
        self.missions, self.log, self.done = [], [], {}

    async def current(self):
        live = [m for m in self.missions if m.label != "aborted"]
        return live[-1] if live else None

    async def get(self, mid):
        return next((m for m in self.missions if m.id == mid), None)

    async def history(self):
        return [(m, len(self.done.get(m.id, {})), sum(d["readings"] for d in self.done.get(m.id, {}).values()))
                for m in reversed(self.missions)]

    async def recent(self, n=5):
        return [m for m in reversed(self.missions) if m.label != "aborted"][:n]

    async def create(self, name, start, sols, crew, notes, actor):
        m = M.Mission(len(self.missions) + 1, name, start, sols, crew, notes or "", None, actor)
        self.missions.append(m)
        return m

    async def update(self, mid, **f):
        m = await self.get(mid)
        for k, v in f.items():
            setattr(m, k, v)
        return m

    async def event(self, mid, kind, message, actor=None, at=None, details=None):
        self.log.append({"mission": mid, "at": at if at is not None else api.time.time(), "kind": kind,
                         "message": message, "actor": actor, "details": details or {}})

    async def manual_readings(self, mid, start, end):
        out = {}
        for e in sorted((e for e in self.log if e["mission"] == mid and e["kind"] == "reading"
                         and start <= e["at"] < end), key=lambda e: e["at"]):
            d = e["details"]
            out[d["measurement"]] = {"value": d["value"], "unit": d["unit"], "at": e["at"], "by": e["actor"]}
        return out

    async def events(self, mid, start, end):
        return [e for e in self.log if e["mission"] == mid]

    async def archived(self, mid):
        return dict(self.done.get(mid, {}))

    async def mark_archived(self, mid, sol, path, readings, summary):
        self.done.setdefault(mid, {})[sol] = {"path": path, "readings": readings}


BOARD: list = []          # board-health rows the fake InfluxDB holds (board_rows)


def board_health(t0, t1, boot, uptime0, reset_reason=1, heal_cause=None, node="node-rpi-01", zone="zone_a"):
    """The ESP32 board's health rows, every 10 s from t0 to t1 (uptime counting from uptime0)."""
    rows = []
    for t in range(int(t0), int(t1), 10):
        r = {"t": float(t), "node_id": node, "zone": zone, "boot_count": float(boot),
             "uptime_s": float(uptime0 + t - t0), "reset_reason": float(reset_reason)}
        if heal_cause is not None:
            r["heal_cause"] = float(heal_cause)
        rows.append(r)
    return rows


@pytest.fixture
def mission_env(monkeypatch, tmp_path):
    fake = FakeStore()
    monkeypatch.setattr(api, "store", fake)
    monkeypatch.setattr(api, "ARCHIVE_DIR", str(tmp_path / "archive"))
    api._cache.clear()
    clock = {"now": T0 + 2 * 86400 + 14 * 3600}
    monkeypatch.setattr(api.time, "time", lambda: clock["now"])

    def rollup(start, end, real_only=True):
        n = int((start - T0) // 86400) + 1
        mins = int((end - start) // 60)
        values = {("scd40", "co2_ppm"): minutes(start, mins, lambda i: 600 + n * 10 + (i % 120)),
                  ("bme280", "temp"): minutes(start, mins, lambda i: 22 + n),
                  ("geiger", "usv_h"): minutes(start, mins, lambda i: 0.12)}
        counts = {("bme280", "node-rpi-01", "zone_a"): {int(start + i * 60): 60 for i in range(mins) if n != 2 or i % 10},
                  ("geiger", "node-rpi-01", "exterior"): {int(start + i * 60): 60 for i in range(mins)}}
        return values, counts
    monkeypatch.setattr(api, "query_rollup", rollup)
    monkeypatch.setattr(api, "board_rows", lambda start, end, real_only=True: [r for r in BOARD if start <= r["t"] < end])
    BOARD.clear()
    app.dependency_overrides[current_user] = lambda: User("pratham", frozenset({"commander"}))
    yield fake, clock
    app.dependency_overrides.clear()


client = TestClient(app)


def test_start_edit_end_and_roles(mission_env):
    fake, clock = mission_env
    assert client.get("/api/mission").json()["clock"] == {"phase": "none"}
    r = client.post("/api/mission/start", json={"name": "Analog Mission Alpha", "start_ist": "2026-09-27 02:48:16"})
    assert r.status_code == 200 and r.json()["mission"]["start"] == T0
    assert r.json()["mission"]["start_ist"] == "Sun 27 Sep 2026, 02:48:16 IST"
    assert client.post("/api/mission/start", json={"name": "Beta"}).status_code == 409      # one at a time
    g = client.get("/api/mission").json()
    assert g["clock"]["sol"] == 3 and [s["state"] for s in g["sols"]][:4] == ["done", "done", "live", "upcoming"]
    assert client.patch("/api/mission", json={"name": "Alpha"}).json()["mission"]["name"] == "Alpha"
    assert client.patch("/api/mission", json={"start_ist": "tomorrow"}).status_code == 422
    app.dependency_overrides[current_user] = lambda: User("crew1", frozenset({"crew"}))
    assert client.post("/api/mission/end").status_code == 403                           # crew can't end it
    assert client.post("/api/mission/events", json={"message": "Crew wake · lights on"}).status_code == 200
    app.dependency_overrides[current_user] = lambda: User("pratham", frozenset({"commander"}))
    assert client.post("/api/mission/end").json()["clock"]["phase"] == "complete"
    assert any(e["message"] == "Crew wake · lights on" for e in fake.log)


def test_overview_sol_timeline_overlay_health_dose(mission_env):
    client.post("/api/mission/start", json={"name": "Alpha", "start_ist": "2026-09-27 02:48:16"})
    o = client.get("/api/mission/overview").json()
    assert [s["state"] for s in o["sols"]][:3] == ["done", "done", "live"] and "coverage_pct" not in o["sols"][3]
    assert o["sols"][1]["coverage_pct"] == 95.0                           # bme280 missed 1 minute in 10 on Sol 2
    assert o["dose"]["total_usv"] == pytest.approx(0.12 * (48 + 14))
    s = client.get("/api/mission/sol/3").json()
    assert s["live"] and s["measurements"]["temperature"]["mean"] == 25 and s["measurements"]["co2"]["max"] == 749.5
    assert s["series"]["co2"][0][0] == pytest.approx(5 / 60, abs=1e-3)
    assert client.get("/api/mission/sol/9").status_code == 404
    t = client.get("/api/mission/timeline", params={"measurement": "co2"}).json()
    assert t["points"][0][0] == pytest.approx(5 / 60, abs=1e-3) and t["now_h"] == pytest.approx(62)
    assert client.get("/api/mission/timeline", params={"measurement": "x"}).status_code == 422
    assert set(client.get("/api/mission/overlay", params={"measurement": "temperature"}).json()["sols"]) == {"1", "2", "3"}
    h = client.get("/api/mission/health").json()
    bme = next(r for r in h["rows"] if r["sensor"] == "bme280")
    assert bme["sols"] == {"1": 100.0, "2": 90.0, "3": 100.0} and h["current"] == 3
    assert client.get("/api/mission/dose").json()["per_sol"]["1"] == pytest.approx(0.12 * 24)


def test_downloads_need_a_token_and_report_prints(mission_env):
    client.post("/api/mission/start", json={"name": "Alpha", "start_ist": "2026-09-27 02:48:16"})
    app.dependency_overrides.clear()                                       # a browser link: no login header
    assert client.get("/api/mission/report/2").status_code == 401
    assert client.get("/api/mission/report/2", params={"dl": "1.bad.pratham"}).status_code == 401
    token = api.make_token("pratham")
    r = client.get("/api/mission/report/2", params={"dl": token})
    assert r.status_code == 200 and "Alpha · Sol 2" in r.text and "Print / save as PDF" in r.text
    assert api.check_token(token) == "pratham" and api.check_token(token, now=api.time.time() + 600) is None


def test_archiver_writes_each_finished_sol_once(mission_env, monkeypatch):
    fake, clock = mission_env
    client.post("/api/mission/start", json={"name": "Analog Alpha", "start_ist": "2026-09-27 02:48:16"})

    def csvs(m, n, target, real_only=True):
        os.makedirs(target, exist_ok=True)
        with open(os.path.join(target, "bme280.csv"), "w") as f:
            f.write("time_ist,time_utc,sol,node_id,zone,temp,quality\n")
        return {"bme280.csv": 86400}
    monkeypatch.setattr(api, "write_sol_csvs", csvs)
    assert asyncio.run(api.archive_due(clock["now"])) == [1, 2]
    assert asyncio.run(api.archive_due(clock["now"])) == []                # not twice
    d = os.path.join(api.ARCHIVE_DIR, "analog-alpha-1", "sol-02")
    assert sorted(os.listdir(d)) == ["README.txt", "SHA256SUMS", "readings", "rollup.json", "summary.json"]
    sums = open(os.path.join(d, "SHA256SUMS")).read()
    assert "readings/bme280.csv" in sums and "summary.json" in sums
    assert json.load(open(os.path.join(d, "summary.json")))["sol"] == 2
    assert any("Sol 2 archived · 86,400 readings" in e["message"] for e in fake.log)
    # after a restart the finished sol's numbers come from the archive, not InfluxDB
    api._cache.clear()
    monkeypatch.setattr(api, "query_rollup", lambda *a: (_ for _ in ()).throw(RuntimeError("influx down")))
    assert client.get("/api/mission/sol/2").json()["measurements"]["temperature"]["mean"] == 24


def test_flux_rows_reads_annotated_csv(monkeypatch):
    text = ("#group,false,false,true,false,false,true\r\n#datatype,string,long,string,dateTime:RFC3339,double,string\r\n"
            "#default,mean,,,,,\r\n,result,table,_measurement,_time,_value,metric\r\n"
            ",,0,bme280,2026-09-27T22:47:00Z,22.5,temp\r\n,,0,bme280,2026-09-27T22:48:00.123456789Z,,temp\r\n\r\n")

    class Raw:
        data = text.encode()

    class Client:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def query_api(self): return self
        def query_raw(self, flux): return Raw()
    monkeypatch.setattr(api, "_get_client", lambda: Client())
    rows = list(api.flux_rows("x"))
    assert rows[0] == {"result": "mean", "table": 0, "_measurement": "bme280", "_time": "2026-09-27T22:47:00Z",
                       "_value": 22.5, "metric": "temp"}
    assert rows[1]["_value"] is None
    assert api._epoch(rows[1]["_time"]) == pytest.approx(api._epoch("2026-09-27T22:48:00Z") + 0.123456789)


def test_edge_nodes_can_read_the_mission_clock(mission_env):
    from services.auth import authenticated
    client.post("/api/mission/start", json={"name": "Alpha", "start_ist": "2026-09-27 02:48:16"})
    app.dependency_overrides.clear()
    app.dependency_overrides[authenticated] = lambda: User("service-account-imm-edge", frozenset({"edge_device"}))
    assert client.get("/api/mission").status_code == 403                  # the full API stays crew / MCC only
    r = client.get("/api/mission/clock").json()
    assert r["mission"]["start"] == T0 and r["clock"]["sol"] == 3


# ── restart, test runs, abort, history (nothing is ever deleted) ────

def test_portable_manual_readings_are_logged_and_surfaced(mission_env):
    fake, clock = mission_env
    client.post("/api/mission/start", json={"name": "Alpha", "start_ist": "2026-09-27 02:48:16"})
    assert client.post("/api/mission/reading", json={"measurement": "nope", "value": 1}).status_code == 422
    r = client.post("/api/mission/reading", json={"measurement": "co2", "value": 812, "note": "Aranet4"})
    assert r.status_code == 200 and r.json() == {"ok": True, "measurement": "co2", "value": 812.0, "unit": "ppm"}
    assert any(e["kind"] == "reading" and "CO\u2082 812 ppm" in e["message"] and "Aranet4" in e["message"] for e in fake.log)
    clock["now"] += 1                                                         # the view is a moment after logging
    ov = client.get("/api/mission/overview").json()
    assert ov["manual"]["co2"]["value"] == 812.0 and ov["manual"]["co2"]["unit"] == "ppm" and ov["manual"]["co2"]["by"] == "pratham"
    # a later reading wins; it lands in the sol it was taken in
    client.post("/api/mission/reading", json={"measurement": "co2", "value": 845})
    clock["now"] += 1
    assert client.get("/api/mission/overview").json()["manual"]["co2"]["value"] == 845.0
    assert client.get("/api/mission/sol/3").json()["manual"]["co2"]["value"] == 845.0
    assert client.get("/api/mission/sol/1").json()["manual"] == {}            # none taken in Sol 1


def test_restart_keeps_the_old_mission_and_starts_again_with_the_same_settings(mission_env):
    fake, clock = mission_env
    client.post("/api/mission/start", json={"name": "Alpha", "start_ist": "2026-09-27 02:48:16", "sols": 5, "crew": 4})
    assert client.post("/api/mission/restart", json={"confirm": "alpha"}).status_code == 422      # name must match
    assert client.post("/api/mission/restart", json={"confirm": "Alpha", "keep_as": "gone"}).status_code == 422
    app.dependency_overrides[current_user] = lambda: User("crew1", frozenset({"crew"}))
    assert client.post("/api/mission/restart", json={"confirm": "Alpha"}).status_code == 403
    app.dependency_overrides[current_user] = lambda: User("pratham", frozenset({"commander"}))
    r = client.post("/api/mission/restart", json={"confirm": "Alpha"}).json()
    now = clock["now"]
    assert r["previous"]["label"] == "restarted" and r["previous"]["ended_at"] == now
    new = r["mission"]
    assert new["id"] == 2 and new["name"] == "Alpha" and new["sols"] == 5 and new["crew"] == 4 and new["start"] == now
    assert r["clock"]["phase"] == "active" and r["clock"]["sol"] == 1
    assert client.get("/api/mission").json()["mission"]["id"] == 2                    # the new one is current
    assert any("restarted by pratham on Sol 3" in e["message"] and "#2" in e["message"] for e in fake.log)
    # the old one is still there, read-only, with its three sols
    o = client.get("/api/mission/overview", params={"mission": 1}).json()
    assert o["mission"]["label"] == "restarted" and [s["state"] for s in o["sols"]] == ["done", "done", "done", "upcoming", "upcoming"]
    assert client.get("/api/mission/sol/3", params={"mission": 1}).json()["measurements"]["temperature"]["mean"] == 25
    t = client.get("/api/mission/timeline", params={"mission": 1, "measurement": "co2"}).json()
    assert t["ended"] and t["now_h"] == pytest.approx(62)                    # where it stopped, not the planned end
    assert client.get("/api/mission/sol/1", params={"mission": 9}).status_code == 404
    token = api.make_token("pratham")
    assert client.get("/api/mission/report/2", params={"mission": 1, "dl": token}).status_code == 200
    assert client.get("/api/mission/download/sol/4", params={"mission": 1, "dl": token}).status_code == 404  # never ran
    h = client.get("/api/mission/history").json()["missions"]
    assert [(m["id"], m["status"], m["current"]) for m in h] == [(2, "running", True), (1, "restarted", False)]


def test_restart_at_a_chosen_time_and_as_test_run(mission_env):
    client.post("/api/mission/start", json={"name": "Dry run", "start_ist": "2026-09-27 02:48:16"})
    r = client.post("/api/mission/restart", json={"confirm": "Dry run", "keep_as": "test",
                                                  "start_ist": "2026-10-04 10:00"}).json()
    assert r["previous"]["label"] == "test" and r["clock"]["phase"] == "pre"
    assert r["mission"]["start_ist"] == "Sun 04 Oct 2026, 10:00:00 IST"
    assert [m["id"] for m in client.get("/api/mission/history").json()["missions"]] == [2]      # tests hidden
    assert [m["id"] for m in client.get("/api/mission/history", params={"all": True}).json()["missions"]] == [2, 1]
    # restarting a mission that hasn't started yet: it ends before Sol 1 and reads as over
    r = client.post("/api/mission/restart", json={"confirm": "Dry run"}).json()
    old = client.get("/api/mission/overview", params={"mission": 2}).json()
    assert r["previous"]["label"] == "restarted" and old["clock"]["phase"] == "complete"
    assert all(s["state"] == "upcoming" for s in old["sols"])


def test_abort_only_a_false_start_and_label_test_runs(mission_env):
    fake, clock = mission_env
    client.post("/api/mission/start", json={"name": "Alpha", "start_ist": "2026-09-27 02:48:16"})
    assert client.post("/api/mission/abort", json={"confirm": "Alpha"}).status_code == 409     # Sol 3: too late
    clock["now"] = T0 + 86400 * 10
    client.post("/api/mission/start", json={"name": "Beta"})                                   # after Alpha ended
    clock["now"] += 1800
    assert client.post("/api/mission/abort", json={"confirm": "Beta?"}).status_code == 422
    r = client.post("/api/mission/abort", json={"confirm": "Beta"}).json()
    assert r["aborted"] == 2 and r["mission"]["name"] == "Alpha"                  # Alpha is current again
    assert client.get("/api/mission").json()["mission"]["id"] == 1                # the Pi files by Alpha (over)
    assert fake.missions[1].label == "aborted" and fake.missions[1].ended_at == clock["now"]
    assert client.post("/api/mission/label", params={"mission": 2}, json={"test": True}).status_code == 409
    assert client.post("/api/mission/label", json={"test": True}).json()["mission"]["label"] == "test"
    assert client.get("/api/mission/history").json()["missions"] == []
    statuses = [m["status"] for m in client.get("/api/mission/history", params={"all": 1}).json()["missions"]]
    assert statuses == ["aborted", "test"]
    assert client.post("/api/mission/label", json={"test": False}).json()["mission"]["label"] == ""
    assert client.get("/api/mission/history").json()["missions"][0]["status"] == "complete"


def test_archiver_finishes_a_restarted_mission_too(mission_env, monkeypatch):
    fake, clock = mission_env
    client.post("/api/mission/start", json={"name": "Alpha", "start_ist": "2026-09-27 02:48:16"})

    def csvs(m, n, target, real_only=True):
        os.makedirs(target, exist_ok=True)
        return {"bme280.csv": 10}
    monkeypatch.setattr(api, "write_sol_csvs", csvs)
    client.post("/api/mission/restart", json={"confirm": "Alpha"})
    assert asyncio.run(api.archive_due(clock["now"])) == [1, 2]              # Sol 3 is still settling
    clock["now"] += api.SETTLE_S + 1
    assert asyncio.run(api.archive_due(clock["now"])) == [3]                 # the cut-short Sol 3 of mission #1
    assert sorted(fake.done[1]) == [1, 2, 3] and 2 not in fake.done


# ── ESP32 board restarts in the mission data ─────────────────────────

def test_board_restarts_found_from_health_rows_with_cause_and_gap():
    # boot 25292 runs, goes silent (unreachable) at +1000 s, the firmware reboots itself (Wi-Fi lost)
    # and comes back as boot 25293 at +1190 s; the external board (no boot counting) is ignored
    rows = (board_health(T0, T0 + 1000, 25292, 38000)
            + board_health(T0 + 1190, T0 + 2000, 25293, 1, reset_reason=3, heal_cause=2)
            + [{"t": T0 + 5.0, "node_id": "node-rpi-01", "zone": "exterior", "uptime_s": 9.0}])
    segs = api.boot_segments(rows)
    assert list(segs) == [("node-rpi-01", "zone_a")] and [x["boot"] for x in segs[("node-rpi-01", "zone_a")]] == [25292, 25293]
    rb = api.board_reboots(segs, T0, T0 + 86400)
    assert len(rb) == 1                                         # boot 25292 started before the window
    r = rb[0]
    assert r["at"] == pytest.approx(T0 + 1189) and r["boot"] == 25293 and r["gap_s"] == 200
    assert r["why"] == "self-heal reboot: Wi-Fi lost" and r["at_ist"].endswith("IST")
    ev = api.reboot_event(r)
    assert ev["kind"] == "board.reboot" and ev["severity"] == "caution"
    assert ev["message"] == "ESP32 board on node-rpi-01 (zone_a) restarted: self-heal reboot: Wi-Fi lost (boot 25293, no data for ~200 s)"
    # a RESET-button press is told apart from the firmware's own reboot; a power-on is advisory
    assert api.restart_why(2, 0) == "RESET button" and api.restart_why(9, 0) == "brownout (supply dipped)"
    assert api.reboot_event({**r, "heal_cause": 0, "reset_reason": 1, "why": "power-on"})["severity"] == "advisory"


def test_readings_are_labelled_with_the_boot_they_came_from():
    segs = api.boot_segments(board_health(T0, T0 + 1000, 7, 50) + board_health(T0 + 1190, T0 + 2000, 8, 1, 3, 2))[
        ("node-rpi-01", "zone_a")]
    assert api.boot_at(segs, T0 + 500)["boot"] == 7
    assert api.boot_at(segs, T0 + 1189)["boot"] == 8            # the first second of the new boot
    assert api.boot_at(segs, T0 - 40)["boot"] == 7              # started 50 s before its first health row
    assert api.boot_at(segs, T0 - 60) is None                   # before the board started


def test_sol_log_report_and_archive_show_board_restarts(mission_env, monkeypatch):
    fake, clock = mission_env
    monkeypatch.setattr(api, "write_sol_csvs", lambda m, n, target, real_only=True: os.makedirs(target) or {})
    client.post("/api/mission/start", json={"name": "Alpha", "start_ist": "2026-09-27 02:48:16"})
    s3 = T0 + 2 * 86400
    BOARD.extend(board_health(s3 - 600, s3 + 3600, 25292, 38000)
                 + board_health(s3 + 3790, s3 + 7200, 25293, 1, reset_reason=3, heal_cause=2))
    ev = client.get("/api/mission/sol/3").json()["events"]
    rb = [e for e in ev if e["kind"] == "board.reboot"]
    assert len(rb) == 1 and "self-heal reboot: Wi-Fi lost (boot 25293, no data for ~200 s)" in rb[0]["message"]
    token = api.make_token("pratham")
    assert "self-heal reboot: Wi-Fi lost" in client.get("/api/mission/report/3", params={"dl": token}).text
    # boot 25292 had been up 38000 s at the start of Sol 3: it powered on during Sol 2, and is shown there
    rb2 = [e for e in client.get("/api/mission/sol/2").json()["events"] if e["kind"] == "board.reboot"]
    assert [e["message"] for e in rb2] == ["ESP32 board on node-rpi-01 (zone_a) restarted: power-on (boot 25292)"]
    target = os.path.join(api.ARCHIVE_DIR, "t")
    summary = asyncio.run(api.build_sol(asyncio.run(api.mission_or_404()), 3, target))["summary"]
    assert [r["boot"] for r in summary["reboots"]] == [25293] and summary["reboots"][0]["gap_s"] == 200
    assert any(e["kind"] == "board.reboot" for e in summary["events"])
    assert "board_boot" in open(os.path.join(target, "README.txt")).read()


def test_sol_csvs_carry_board_boot_and_time_since_boot(monkeypatch, tmp_path):
    m = M.Mission(id=1, name="Alpha", start=T0, sols=7)
    BOARD[:] = board_health(T0, T0 + 100, 7, 500) + board_health(T0 + 300, T0 + 400, 8, 2, 3, 2)
    monkeypatch.setattr(api, "board_rows", lambda start, end, real_only=True: [r for r in BOARD if start <= r["t"] < end])
    monkeypatch.setattr(api.time, "time", lambda: T0 + 1000)

    def iso(t):
        return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def flux_rows(flux):
        if 'r._field == "q"' in flux:
            return []
        sensor = flux.split('r._measurement == "')[1].split('"')[0]
        zone = "exterior" if sensor == "geiger" else "zone_a"
        return [{"_time": iso(T0 + t), "node_id": "node-rpi-01", "zone": zone, "temp" if sensor == "bme280" else "cpm": 25.0}
                for t in (50, 310, 395)]

    class Rec:
        def __init__(self, v): self.v = v
        def get_value(self): return self.v

    class Client:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def query_api(self): return self
        def query(self, q): return [type("T", (), {"records": [Rec("bme280"), Rec("geiger"), Rec("board")]})()]
    monkeypatch.setattr(api, "_get_client", lambda: Client())
    monkeypatch.setattr(api, "flux_rows", flux_rows)
    api.write_sol_csvs(m, 1, str(tmp_path))
    with open(tmp_path / "bme280.csv") as f:
        rows = list(csv.DictReader(f))
    assert [(r["board_boot"], r["since_boot_s"]) for r in rows] == [("7", "550"), ("8", "12"), ("8", "97")]
    with open(tmp_path / "geiger.csv") as f:
        assert "board_boot" not in f.readline()                  # the external board has no boot counter
