"""
Phase 0 smoke tests for IMM-OS Backend.
These run in CI: lint → test → build.
"""
from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_health_returns_ok():
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "imm-os-backend"


def test_root_returns_200():
    response = client.get("/")
    assert response.status_code == 200
    assert "version" in response.json()


def test_health_has_version():
    response = client.get("/health")
    assert "version" in response.json()
