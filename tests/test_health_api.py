"""Health monitor HTTP API (no Kafka/Postgres: the startup hook is not run)."""
import time

import pytest
from fastapi.testclient import TestClient

from services import health_monitor as hm
from services.auth import User, authenticated
from services.health.alarms import Condition
from services.health.monitor import HealthMonitor

client = TestClient(hm.app)
OPERATOR = User("capcom", frozenset({"mcc_operator"}))
CREW = User("crew-01", frozenset({"crew"}))
SERVICE = User("imm-service", frozenset({"service"}))


@pytest.fixture(autouse=True)
def fresh(monkeypatch):
    monkeypatch.setattr(hm, "monitor", HealthMonitor())
    monkeypatch.setattr(hm, "publish", lambda ev: None)
    monkeypatch.setattr(hm.store, "pending", type(hm.store.pending)(maxlen=100))
    hm.state["notes"].clear()
    yield
    hm.app.dependency_overrides.clear()


def as_user(u):
    hm.app.dependency_overrides[authenticated] = lambda: u


def test_everything_needs_login_except_liveness():
    for path in ("/api/health/summary", "/api/health/alarms", "/api/health/streams", "/api/health/eva"):
        assert client.get(path).status_code == 401
    assert client.post("/api/health/alarms/ack-all").status_code == 401


def test_liveness_follows_the_tick_not_kafka(monkeypatch):
    hm.monitor.tick(time.time())
    hm.state["consumer_alive"] = False             # Kafka down: reported, but no restart
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["status"] == "degraded" and r.json()["kafka"] is False
    hm.monitor.last_tick = time.time() - 60        # the loop itself hung
    monkeypatch.setitem(hm.state, "started", time.time() - 600)
    assert client.get("/health").status_code == 503


def test_alarm_raise_ack_and_summary():
    as_user(OPERATOR)
    now = time.time()
    hm.monitor.extra_conditions = {"limit.co2.n.z": Condition("warning", "measurement", "n", "CO2 2100 ppm")}
    hm.monitor.alarms.evaluate(hm.monitor.extra_conditions, now)
    hm.monitor.alarms.evaluate(hm.monitor.extra_conditions, now + 30)     # past any on-delay
    hm.monitor.tick(now + 31)
    alarms = client.get("/api/health/alarms").json()["alarms"]
    assert [a["key"] for a in alarms] == ["limit.co2.n.z"] and not alarms[0]["acked"]
    r = client.post(f"/api/health/alarms/{alarms[0]['id']}/ack")
    assert r.status_code == 200 and r.json()["alarm"]["acked"] and r.json()["alarm"]["acked_by"] == "capcom"
    assert client.post("/api/health/alarms/999999/ack").status_code == 404
    s = client.get("/api/health/summary").json()
    assert s["mode"] in ("NOMINAL", "DEGRADED", "EMERGENCY") and s["alarms"][0]["acked"]
    assert any(k == "alarm" for k, _ in hm.store.pending)                 # the ack is persisted


def test_eva_arm_by_anyone_disarm_only_by_mcc_or_commander():
    as_user(CREW)
    assert client.post("/api/health/eva/CREW-02/arm").json()["crew"]["armed"] is True
    assert client.post("/api/health/eva/crew-02/disarm").status_code == 403       # crew can't silence LOS
    assert client.post("/api/health/eva/crew;drop/arm").status_code == 422
    as_user(OPERATOR)
    assert client.post("/api/health/eva/crew-02/disarm").json()["crew"]["armed"] is False
    assert client.post("/api/health/eva/nobody/disarm").status_code == 404


def test_events_endpoint_is_for_services_only_and_raises_an_advisory():
    body = {"key": "autoheal.imm-validator", "severity": "advisory", "message": "restarted imm-validator"}
    as_user(OPERATOR)
    assert client.post("/api/health/events", json=body).status_code == 403
    as_user(SERVICE)
    assert client.post("/api/health/events", json={**body, "severity": "nonsense"}).status_code == 200
    cond, until = hm.state["notes"]["note.autoheal.imm-validator"]
    assert cond.severity == "advisory" and until > time.time()          # unknown severity → advisory


def test_forget_node_needs_mcc_or_commander():
    as_user(CREW)
    assert client.post("/api/health/nodes/vv-node-01/forget").status_code == 403
    as_user(OPERATOR)
    r = client.post("/api/health/nodes/vv-node-01/forget")
    assert r.status_code == 200 and r.json()["node"] == "vv-node-01"
    assert ("forget_node", "vv-node-01") in list(hm.store.pending)
    assert client.post("/api/health/nodes/bad%20node/forget").status_code == 422


def test_component_state_changes_are_persisted(monkeypatch):
    now = time.time()
    hm.monitor.ingest_component({"state": "SAFE", "reason": "relays off"}, now, "habitat/health/node-rpi-01/eclss_pid")
    hm.emit(hm.monitor.tick(now))
    comps = [p for k, p in hm.store.pending if k == "component"]
    assert comps and comps[-1]["state"] == "SAFE" and "_received" not in comps[-1]
