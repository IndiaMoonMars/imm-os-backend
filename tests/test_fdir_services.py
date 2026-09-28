"""Autoheal, the worker heartbeat, bridge routing of health topics and the health store queue."""
import asyncio
import os
import time
from types import SimpleNamespace


from services import autoheal, heartbeat


def container(name, status, state="running"):
    return {"Id": name + "-id", "Names": ["/" + name], "Status": status, "State": state}


def test_health_of_reads_docker_status():
    assert autoheal.health_of(container("a", "Up 3 minutes (unhealthy)")) == "unhealthy"
    assert autoheal.health_of(container("a", "Up 3 minutes (healthy)")) == "healthy"
    assert autoheal.health_of(container("a", "Up 5 seconds (health: starting)")) == "starting"
    assert autoheal.health_of(container("a", "Up 1 hour")) == "none"
    assert autoheal.health_of(container("a", "Up 1 hour (Paused)", state="paused")) == "paused"


def test_autoheal_restarts_then_gives_up_then_retries_after_backoff(monkeypatch):
    monkeypatch.setattr(autoheal, "MAX_RESTARTS", 3)
    monkeypatch.setattr(autoheal, "WINDOW_S", 900)
    monkeypatch.setattr(autoheal, "BACKOFF_S", 1800)
    h = autoheal.Healer()
    sick = [container("imm-validator", "Up 2 minutes (unhealthy)"), container("imm-api", "Up 2 minutes (healthy)")]
    acts = [h.step(t, sick) for t in (0, 60, 120, 180, 240)]
    assert [a[0]["action"] for a in acts[:3]] == ["restart"] * 3
    assert acts[0][0]["container"] == "imm-validator" and all(len(a) <= 1 for a in acts)   # the healthy one is left alone
    assert acts[3][0]["action"] == "give_up" and acts[4] == []                            # needs a person now
    assert h.step(180 + 1800 + 1, sick)[0]["action"] == "restart"                          # tries again after the backoff


def test_autoheal_window_forgets_old_restarts(monkeypatch):
    monkeypatch.setattr(autoheal, "MAX_RESTARTS", 3)
    monkeypatch.setattr(autoheal, "WINDOW_S", 900)
    h = autoheal.Healer()
    sick = [container("imm-bridge", "Up (unhealthy)")]
    for t in (0, 1000, 2000, 3000, 4000):         # one restart every ~17 min is recoverable, not a loop
        assert h.step(t, sick)[0]["action"] == "restart"


def test_autoheal_reports_restart_and_give_up(monkeypatch):
    calls, reports = [], []
    monkeypatch.setattr(autoheal, "docker", lambda m, p, timeout=30: calls.append((m, p)) or (204, b""))
    monkeypatch.setattr(autoheal, "report", lambda *a: reports.append(a))
    h = autoheal.Healer()
    h.apply({"action": "restart", "container": "imm-p", "id": "x1", "health": "paused", "attempt": 1})
    assert calls == [("POST", "/containers/x1/unpause"), ("POST", "/containers/x1/restart?t=10")]
    assert reports[0][0] == "autoheal.imm-p" and reports[0][1] == "advisory"
    h.apply({"action": "give_up", "container": "imm-p", "restarts": 3, "health": "unhealthy"})
    assert reports[1][0] == "autoheal.imm-p.gave_up" and reports[1][1] == "warning"


def test_heartbeat_file_ages_and_fails_the_healthcheck(tmp_path, monkeypatch):
    p = str(tmp_path / "hb")
    assert heartbeat.age(p) == float("inf")                  # never beat: unhealthy
    monkeypatch.setattr(heartbeat, "_last", 0.0)
    heartbeat.beat(p, min_interval_s=0)
    assert heartbeat.age(p) < 2
    os.utime(p, (time.time() - 120, time.time() - 120))       # a hung loop stops touching it
    assert heartbeat.age(p) > 60


def test_bridge_routes_health_topics_to_health_raw(monkeypatch):
    from services import mqtt_to_kafka_bridge as b
    produced = []
    monkeypatch.setattr(b, "producer", SimpleNamespace(produce=lambda t, **k: produced.append((t, k["key"])),
                                                       poll=lambda *a: None))
    for topic in ("habitat/health/node-rpi-01/eclss_pid", "habitat/eva/position/crew-01", "habitat/sensors/scd40/zone_a"):
        b.on_message(None, None, SimpleNamespace(topic=topic, payload=b"{}"))
    assert [t for t, _ in produced] == ["health.raw", "eva.raw", "telemetry.raw"]
    assert produced[0][1] == b"habitat/health/node-rpi-01/eclss_pid"    # the component is in the key


# ── health store: ordered queue, kept on outage, bad rows skipped ──
class FakePool:
    def __init__(self):
        self.sql, self.down, self.bad = [], False, False

    async def _run(self, q, *a):
        if self.down:
            raise ConnectionRefusedError("connection refused")
        if self.bad and "alarm_events" in q:
            raise ValueError("invalid input syntax")
        self.sql.append(q.split()[0] + " " + q.split()[2 if q.startswith("INSERT") else 1])
        return 7

    fetchval = execute = _run

    async def executemany(self, q, rows):
        return await self._run(q)


def test_health_store_keeps_writes_through_an_outage_in_order():
    from services.health.alarms import Alarm
    from services.health_store import HealthStore
    s = HealthStore()
    s.pool = FakePool()
    a = Alarm(key="limit.co2.n.z", severity="warning", category="measurement", source="n", message="CO2 high",
              raised_at=1.0, state="active")
    s.pool.down = True
    s.enqueue("alarm", {"event": "raised", "at": 1.0, "_alarm": a})
    s.enqueue("alarm", {"event": "acked", "at": 2.0, "actor": "capcom", "_alarm": a})
    asyncio.run(s.flush())
    assert len(s.pending) == 2 and not s.ok                     # nothing lost, nothing reordered
    s.pool.down = False
    asyncio.run(s.flush())
    assert not s.pending and s.ok and a.db_id == 7
    assert s.pool.sql == ["INSERT alarms", "INSERT alarm_events", "UPDATE alarms", "INSERT alarm_events"]


def test_health_store_skips_a_bad_row_instead_of_blocking():
    from services.health_store import HealthStore
    s = HealthStore()
    s.pool = FakePool()
    s.pool.bad = True
    s.enqueue("note", {"key": "x", "message": "m", "at": 1.0})
    s.enqueue("streams", [])
    asyncio.run(s.flush())
    assert not s.pending and s.pool.sql == ["INSERT telemetry_streams"]


def test_workers_import_the_way_compose_runs_them():
    """compose runs these as scripts (python services/x.py), not as modules."""
    import subprocess
    import sys
    from pathlib import Path
    root = Path(__file__).parent.parent
    for script in ("services/mqtt_to_kafka_bridge.py", "services/telemetry_processor.py"):
        code = (f"import runpy, sys; sys.argv = ['{script}']; sys.path.insert(0, '{root / 'services'}'); "
                f"runpy.run_path('{root / script}', run_name='not_main')")
        r = subprocess.run([sys.executable, "-c", code], cwd="/", capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, f"{script}: {r.stderr[-600:]}"
