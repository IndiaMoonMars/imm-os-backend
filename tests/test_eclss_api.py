"""
ECLSS API tests — lighting persistence + MQTT edge bridge, event logging.
DB and MQTT are replaced with in-memory fakes; startup hooks are not run.
"""
import json

import paho.mqtt.client as mqtt
import pytest
from fastapi.testclient import TestClient

from services import eclss_api


class FakeTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeConn:
    def __init__(self, db):
        self.db = db

    def transaction(self):
        return FakeTx()

    async def fetch(self, sql, *args):
        return [{"zone": z, **s} for z, s in sorted(self.db["lighting"].items())]

    async def execute(self, sql, zone, brightness, kelvin):
        self.db["lighting"][zone] = {"brightness": brightness, "kelvin": kelvin}

    async def fetchval(self, sql, *args):
        self.db["inserts"].append((sql, args))
        return len(self.db["inserts"])

    async def close(self):
        pass


class FakePublishResult:
    rc = mqtt.MQTT_ERR_SUCCESS


class FakeMqtt:
    def __init__(self, connected=True):
        self.connected = connected
        self.published = []

    def is_connected(self):
        return self.connected

    def publish(self, topic, payload, qos, retain):
        self.published.append((topic, json.loads(payload), qos, retain))
        return FakePublishResult()


@pytest.fixture
def db(monkeypatch):
    db = {"lighting": {"core": {"brightness": 80, "kelvin": 5000},
                       "lab": {"brightness": 80, "kelvin": 5000}},
          "inserts": []}

    async def fake_get_conn():
        return FakeConn(db)

    monkeypatch.setattr(eclss_api, "get_conn", fake_get_conn)
    monkeypatch.setattr(eclss_api, "_published_state", {})
    return db


@pytest.fixture
def broker(monkeypatch):
    fake = FakeMqtt()
    monkeypatch.setattr(eclss_api, "mqtt_client", fake)
    return fake


client = TestClient(eclss_api.app)


def test_get_lighting_reads_db(db, broker):
    r = client.get("/api/v1/eclss/lighting")
    assert r.status_code == 200
    assert r.json() == db["lighting"]


def test_put_zone_persists_and_publishes_retained(db, broker):
    r = client.put("/api/v1/eclss/lighting/lab", json={"brightness": 40, "kelvin": 3000})
    assert r.status_code == 200
    assert r.json()["edge_synced"] is True
    assert db["lighting"]["lab"] == {"brightness": 40, "kelvin": 3000}
    topic, payload, qos, retain = broker.published[0]
    assert topic == "habitat/control/lighting/lab"
    assert payload["brightness"] == 40 and payload["kelvin"] == 3000
    assert qos == 1 and retain is True


def test_put_all_updates_every_known_zone(db, broker):
    r = client.put("/api/v1/eclss/lighting/all", json={"brightness": 10, "kelvin": 2700})
    assert r.status_code == 200
    assert all(s == {"brightness": 10, "kelvin": 2700} for s in db["lighting"].values())
    assert sorted(t for t, *_ in broker.published) == [
        "habitat/control/lighting/core", "habitat/control/lighting/lab"]


def test_put_new_zone_is_registered(db, broker):
    client.put("/api/v1/eclss/lighting/greenhouse", json={"brightness": 60, "kelvin": 4000})
    assert "greenhouse" in db["lighting"]


def test_put_rejects_out_of_range(db, broker):
    assert client.put("/api/v1/eclss/lighting/lab", json={"brightness": 150, "kelvin": 3000}).status_code == 422
    assert client.put("/api/v1/eclss/lighting/lab", json={"brightness": 50, "kelvin": 9000}).status_code == 422


def test_put_with_broker_down_stores_and_resyncs_on_connect(db, broker):
    broker.connected = False
    r = client.put("/api/v1/eclss/lighting/core", json={"brightness": 5, "kelvin": 2200})
    assert r.status_code == 200
    assert r.json()["edge_synced"] is False
    assert db["lighting"]["core"] == {"brightness": 5, "kelvin": 2200}
    assert broker.published == []

    broker.connected = True
    eclss_api._on_connect(broker, None, {}, 0)
    assert ("habitat/control/lighting/core", 5) in [(t, p["brightness"]) for t, p, *_ in broker.published]


@pytest.mark.parametrize("path,body,table", [
    ("/api/v1/waste/log", {"weight_kg": 1.0, "rfid_tag": "BAG-1", "container": "galley"}, "waste_events"),
    ("/api/v1/water/shower", {"duration_seconds": 180, "estimated_liters": 45}, "shower_events"),
    ("/api/v1/water/log", {"event_ml": 500, "daily_total_ml": 1500, "source": "galley"}, "water_flow_events"),
    ("/api/v1/biolab/log", {"ph_level": 7.0, "water_temp_c": 22.5}, "biolab_readings"),
])
def test_event_endpoints_persist(db, broker, path, body, table):
    r = client.post(path, json=body)
    assert r.status_code == 201
    assert r.json()["status"] == "logged"
    sql, _ = db["inserts"][-1]
    assert f"INSERT INTO {table}" in sql


def test_biolab_rejects_invalid_ph(db, broker):
    assert client.post("/api/v1/biolab/log", json={"ph_level": 15, "water_temp_c": 20}).status_code == 422
