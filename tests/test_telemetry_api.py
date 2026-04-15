"""
Telemetry API tests — uses TestClient; falls back to mock data (no real InfluxDB).
"""
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_nodes_returns_list():
    response = client.get("/api/telemetry/nodes")
    assert response.status_code == 200
    data = response.json()
    assert "nodes" in data
    assert len(data["nodes"]) == 3


def test_nodes_have_required_fields():
    response = client.get("/api/telemetry/nodes")
    for node in response.json()["nodes"]:
        assert "id" in node
        assert "type" in node
        assert "zone" in node


def test_latest_returns_readings():
    response = client.get("/api/telemetry/latest")
    assert response.status_code == 200
    data = response.json()
    assert "readings" in data


def test_latest_has_all_three_nodes():
    response = client.get("/api/telemetry/latest")
    readings = response.json()["readings"]
    assert "node-rpi-01" in readings
    assert "node-rpi-02" in readings
    assert "node-jetson" in readings


def test_node_latest_known_node():
    response = client.get("/api/telemetry/node-rpi-01/latest")
    assert response.status_code == 200
    assert "readings" in response.json()


def test_node_latest_unknown_node_returns_404():
    response = client.get("/api/telemetry/nonexistent-node/latest")
    assert response.status_code == 404


def test_node_history_missing_measurement_returns_422():
    response = client.get("/api/telemetry/node-rpi-01/history")
    assert response.status_code == 422  # measurement is required
