"""
Health store against a real Postgres (skipped unless IMM_TEST_DATABASE_URL is set; CI sets it).
Runs in its own schema, so it never touches a real database's alarm history.
"""
import asyncio
import os
import time
from urllib.parse import urlparse

import pytest

from services.health.alarms import Condition
from services.health.monitor import HealthMonitor
from services.health_store import HealthStore, stream_rows

DSN = os.getenv("IMM_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DSN, reason="IMM_TEST_DATABASE_URL not set")
SCHEMA = "imm_test_health"


@pytest.fixture
def pg(monkeypatch):
    import asyncpg
    u = urlparse(DSN)
    for k, v in (("POSTGRES_HOST", u.hostname), ("POSTGRES_PORT", str(u.port or 5432)), ("POSTGRES_USER", u.username),
                 ("POSTGRES_PASSWORD", u.password or ""), ("POSTGRES_DB", u.path.lstrip("/"))):
        monkeypatch.setenv(k, v)

    async def reset():
        c = await asyncpg.connect(DSN)
        await c.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE; CREATE SCHEMA {SCHEMA}")
        await c.close()
    asyncio.run(reset())
    real = asyncpg.create_pool
    monkeypatch.setattr(asyncpg, "create_pool", lambda **kw: real(server_settings={"search_path": SCHEMA}, **kw))
    yield


def test_alarm_lifecycle_history_and_restore_on_real_postgres(pg):
    async def run():
        s = HealthStore()
        assert await s.connect(), s.error
        assert await HealthStore().connect()                      # schema creation is idempotent
        mon = HealthMonitor()
        t = time.time() - 400
        key = "limit.co2.node-rpi-01.zone_a"
        warn = {key: Condition("warning", "measurement", "node-rpi-01", "CO2 5200 ppm", value=5200.0)}
        emer = {key: Condition("emergency", "measurement", "node-rpi-01", "CO2 21000 ppm", value=21000.0)}
        evs = []
        for i in range(40):
            evs += mon.alarms.evaluate(warn, t + i)
        evs += mon.alarms.acknowledge(mon.alarms.open()[0], "capcom", t + 41)
        for i in range(42, 60):
            evs += mon.alarms.evaluate(emer, t + i)
        for i in range(60, 200):
            evs += mon.alarms.evaluate({}, t + i)
        for i in range(300, 340):                                  # back before anyone acked the RTN
            evs += mon.alarms.evaluate(warn, t + i)
        for ev in evs:
            s.enqueue("alarm", ev)
        mon.ingest_reading({"data": {"sensor": "bme280", "node_id": "n1", "zone": "z", "timestamp": t,
                                     "temp": 22.0, "hum": 40.0, "pres": 1000.0}}, t)
        s.enqueue("streams", stream_rows(mon.tracker))
        s.enqueue("streams", stream_rows(mon.tracker))
        s.enqueue("crew", {"crew_id": "crew-01", "armed": True, "armed_by": "capcom", "state": "LOS",
                           "_last_contact": t, "last_position": {"x_m": 1.5}, "last_vitals": {}})
        s.enqueue("note", {"key": "autoheal.imm-x", "event": "note", "severity": "advisory",
                           "message": "restarted imm-x", "actor": "imm-service", "details": {"n": 1}, "at": t + 1})
        await s.flush()
        assert not s.pending and s.ok, s.error

        rows = await s.pool.fetch("SELECT alarm_key, severity, state, raise_count FROM alarms")
        assert [tuple(r) for r in rows] == [(key, "warning", "active", 2)]      # one row per condition
        hist = [(r["event"], r["severity"]) for r in reversed(await s.history(50)) if r["alarm_key"] == key]
        assert hist == [("raised", "warning"), ("acked", "warning"), ("escalated", "emergency"),
                        ("cleared", "emergency"), ("reraised", "warning")]    # each as it was at the time

        restored = await s.load_open_alarms()
        assert [(a.key, a.state, a.raise_count, a.db_id is not None) for a in restored] == [(key, "active", 2, True)]
        crews = await s.load_crews()
        assert crews[0]["crew_id"] == "crew-01" and crews[0]["state"] == "LOS"
        assert [r["sensor"] for r in await s.load_streams(3600)] == ["bme280"]
        await s.pool.close()
    asyncio.run(run())
