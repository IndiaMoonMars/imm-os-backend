"""
Machine-auth tests: edge-device role separation, webhook shared secret,
telemetry-ingest WebSocket handshake and command attribution, time-service roles.
"""
import time

import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from services import auth, telemetry_ingest
from services.auth import User, authenticated, current_user, edge_device, require_shared_secret

CREW = User("ev1", frozenset({"crew"}))
EDGE = User("service-account-imm-edge", frozenset({"edge_device"}))
MCC = User("capcom", frozenset({"mcc_operator"}))
SERVICE = User("imm-service", frozenset({"service"}))

# ── edge_device vs current_user ───────────────────────────────────

roles_app = FastAPI()


@roles_app.get("/crew", dependencies=[Depends(current_user)])
def crew_only():
    return {}


@roles_app.post("/device", dependencies=[Depends(edge_device)])
def device_only():
    return {}


@roles_app.post("/hook", dependencies=[Depends(require_shared_secret("TEST_HOOK_TOKEN"))])
def hook():
    return {}


roles_client = TestClient(roles_app)


@pytest.fixture(autouse=True)
def clear_overrides():
    yield
    for app in (roles_app, telemetry_ingest.app):
        app.dependency_overrides.clear()


@pytest.mark.parametrize("user,crew_status,device_status", [
    (CREW, 200, 403),
    (EDGE, 403, 200),     # device credentials can't reach crew APIs
    (SERVICE, 200, 200),
])
def test_edge_and_crew_roles_are_separate(user, crew_status, device_status):
    roles_app.dependency_overrides[authenticated] = lambda: user
    assert roles_client.get("/crew").status_code == crew_status
    assert roles_client.post("/device").status_code == device_status


def test_device_endpoint_needs_credentials():
    assert roles_client.post("/device").status_code == 401


def test_webhook_secret(monkeypatch):
    monkeypatch.delenv("TEST_HOOK_TOKEN", raising=False)
    assert roles_client.post("/hook").status_code == 503          # unconfigured = closed
    monkeypatch.setenv("TEST_HOOK_TOKEN", "s3cret")
    assert roles_client.post("/hook").status_code == 401
    assert roles_client.post("/hook", headers={"X-IMM-Webhook-Token": "nope"}).status_code == 401
    assert roles_client.post("/hook", headers={"X-IMM-Webhook-Token": "s3cret"}).status_code == 200
    assert roles_client.post("/hook?token=s3cret").status_code == 200


# ── telemetry ingest ──────────────────────────────────────────────

ingest_client = TestClient(telemetry_ingest.app)


def fake_user_from_token(token):
    users = {"crew-token": CREW, "edge-token": EDGE}
    if token not in users:
        raise HTTPException(401, "bad token")
    return users[token]


@pytest.fixture
def ws_auth(monkeypatch):
    monkeypatch.setattr(telemetry_ingest, "user_from_token", fake_user_from_token)
    monkeypatch.setattr(telemetry_ingest, "WS_AUTH_TIMEOUT_S", 0.5)
    telemetry_ingest.manager.active_connections.clear()


def test_realtime_accepts_crew_token(ws_auth):
    with ingest_client.websocket_connect("/realtime") as ws:
        ws.send_json({"type": "auth", "token": "crew-token"})
        for _ in range(100):  # server thread registers the socket asynchronously
            if telemetry_ingest.manager.active_connections:
                break
            time.sleep(0.01)
        assert len(telemetry_ingest.manager.active_connections) == 1


@pytest.mark.parametrize("first_message", [
    {"type": "auth", "token": "wrong"},
    {"type": "auth", "token": "edge-token"},   # device token has no crew/MCC role
    {"type": "hello"},
    "not json",
])
def test_realtime_rejects_bad_auth(ws_auth, first_message):
    with ingest_client.websocket_connect("/realtime") as ws:
        if isinstance(first_message, dict):
            ws.send_json(first_message)
        else:
            ws.send_text(first_message)
        with pytest.raises(WebSocketDisconnect) as e:
            ws.receive_text()
    assert e.value.code == 4401
    assert telemetry_ingest.manager.active_connections == []


def test_realtime_times_out_without_auth(ws_auth):
    with ingest_client.websocket_connect("/realtime") as ws:
        with pytest.raises(WebSocketDisconnect) as e:
            ws.receive_text()
    assert e.value.code == 4401


def test_command_operator_comes_from_token(monkeypatch):
    inserted = []

    class Conn:
        async def execute(self, sql, *args):
            inserted.append(args)

        async def close(self):
            pass

    async def connect(**kw):
        return Conn()

    monkeypatch.setattr(telemetry_ingest.asyncpg, "connect", connect)
    telemetry_ingest.app.dependency_overrides[authenticated] = lambda: MCC
    r = ingest_client.post("/commands", json={"operator_id": "someone-else", "command_text": "VENT LAB"})
    assert r.status_code == 201
    assert inserted == [("capcom", "VENT LAB")]


def test_ingest_requires_edge_device():
    assert ingest_client.post("/ingest", json={}).status_code == 401
    telemetry_ingest.app.dependency_overrides[authenticated] = lambda: CREW
    assert ingest_client.post("/ingest", json={}).status_code == 403


def test_history_and_alerts_require_login():
    assert ingest_client.get("/history?start=0&end=1&sensor=a&metric=b").status_code == 401
    assert ingest_client.get("/alerts").status_code == 401


# ── time service ──────────────────────────────────────────────────

def test_time_service_roles(monkeypatch):
    from services.time_service import api as time_api
    time_client = TestClient(time_api.app)

    async def fake_set(mode, val=None):
        return {"mode": mode, "value": val or 0}

    monkeypatch.setattr(time_api.delay_queue, "set_delay_config", fake_set)
    assert time_client.get("/api/v1/time/now").status_code == 401
    time_api.app.dependency_overrides[authenticated] = lambda: CREW
    try:
        assert time_client.get("/api/v1/time/now").status_code == 200
        assert time_client.post("/api/v1/time/delay/config", json={"mode": "mars"}).status_code == 403
        time_api.app.dependency_overrides[authenticated] = lambda: MCC
        assert time_client.post("/api/v1/time/delay/config", json={"mode": "mars"}).status_code == 200
    finally:
        time_api.app.dependency_overrides.clear()


def test_auth_module_exports():
    assert auth.EDGE_DEVICE == "edge_device"
