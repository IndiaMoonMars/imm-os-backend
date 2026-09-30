"""
Inventory API (Phase 11) tests.

Unit tests always run. Integration tests run the real service against a real
PostgreSQL loaded with imm-os-infra/postgres/init.sql; they are skipped unless
IMM_TEST_DATABASE_URL is set, e.g.:

    IMM_TEST_DATABASE_URL=postgresql://admin:changeme@127.0.0.1:5432/imm_test \\
    IMM_INIT_SQL=../imm-os-infra/postgres/init.sql pytest tests/test_inventory_api.py
"""
import asyncio
import os
import uuid

import asyncpg
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from services import inventory_api as inv
from services.auth import User, authenticated

# ── Unit tests ────────────────────────────────────────────────────


@pytest.mark.parametrize("state,unit,ok", [
    ("solid", "units", True), ("solid", "kg", True), ("liquid", "mL", True),
    ("gas", "bar", True), ("liquid", "kg", False), ("gas", "units", False), ("plasma", "units", False),
])
def test_item_unit_must_match_state(state, unit, ok):
    body = {"barcode": "B1", "name": "x", "physical_state": state, "unit": unit}
    if ok:
        inv.ItemIn(**body)
    else:
        with pytest.raises(ValidationError):
            inv.ItemIn(**body)


def test_negative_stock_and_zero_adjust_rejected():
    with pytest.raises(ValidationError):
        inv.ItemIn(barcode="B", name="x", physical_state="solid", unit="units", quantity=-1)
    with pytest.raises(ValidationError):
        inv.Adjustment(delta=0, reason="none")


def test_repair_part_needs_exactly_one_reference():
    inv.RepairPart(barcode="B", quantity=1)
    for bad in ({"quantity": 1}, {"barcode": "B", "item_id": 1, "quantity": 1}, {"barcode": "B", "quantity": 0}):
        with pytest.raises(ValidationError):
            inv.RepairPart(**bad)


def test_photo_type_detected_from_bytes():
    assert inv.detect_image_type(b"\xff\xd8\xff\xe0rest")[0] == "image/jpeg"
    assert inv.detect_image_type(b"\x89PNG\r\n\x1a\nrest")[0] == "image/png"
    assert inv.detect_image_type(b"RIFF\x00\x00\x00\x00WEBPVP8 ")[0] == "image/webp"
    assert inv.detect_image_type(b"<?php evil") is None


# ── Integration tests (real PostgreSQL) ───────────────────────────

DSN = os.getenv("IMM_TEST_DATABASE_URL")
_DEFAULT_INIT_SQL = os.path.join(os.path.dirname(__file__), "..", "..", "imm-os-infra", "postgres", "init.sql")
INIT_SQL = os.getenv("IMM_INIT_SQL", _DEFAULT_INIT_SQL)
pytestmark_integration = pytest.mark.skipif(
    not DSN or not os.path.exists(INIT_SQL), reason="set IMM_TEST_DATABASE_URL (and IMM_INIT_SQL)")

CREW = User("ev1", frozenset({"crew"}))
CREW2 = User("ev2", frozenset({"crew"}))
CDR = User("cdr", frozenset({"commander"}))
EDGE = User("service-account-imm-edge", frozenset({"edge_device"}))

JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 64


@pytest.fixture(scope="module")
def db():
    """Fresh schema from init.sql; TestClient's startup creates the pool from POSTGRES_* env."""
    async def setup():
        conn = await asyncpg.connect(DSN)
        await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await conn.execute(open(INIT_SQL).read())
        await conn.close()
    asyncio.run(setup())
    return DSN


@pytest.fixture(scope="module")
def client(db, tmp_path_factory):
    from urllib.parse import urlparse
    u = urlparse(db)
    os.environ.update({"POSTGRES_HOST": u.hostname, "PGPORT": str(u.port or 5432),
                       "POSTGRES_USER": u.username, "POSTGRES_PASSWORD": u.password or "",
                       "POSTGRES_DB": u.path.lstrip("/")})
    inv.MEDIA_DIR = str(tmp_path_factory.mktemp("incidents"))
    with TestClient(inv.app) as c:   # runs startup → real asyncpg pool
        yield c
    inv.app.dependency_overrides.clear()


def as_user(user):
    inv.app.dependency_overrides[authenticated] = lambda: user


def new_item(c, **kw):
    body = {"barcode": f"BC-{uuid.uuid4().hex[:8]}", "name": "Item", "physical_state": "solid",
            "unit": "units", "quantity": 10, "min_quantity": 2}
    body.update(kw)
    as_user(CREW)
    r = c.post("/api/v1/inventory/items", json=body)
    assert r.status_code == 201, r.text
    return r.json()


@pytestmark_integration
def test_all_three_physical_states_and_scan(client):
    solid = new_item(client, name="BME280 sensor", category="spare")
    liquid = new_item(client, name="Isopropyl alcohol", physical_state="liquid", unit="mL", quantity=500)
    gas = new_item(client, name="O2 cylinder", physical_state="gas", unit="bar", quantity=180)
    for item in (solid, liquid, gas):
        as_user(EDGE)  # scanner station
        r = client.get(f"/api/v1/inventory/scan/{item['barcode']}")
        assert r.status_code == 200 and r.json()["name"] == item["name"]
    assert client.get("/api/v1/inventory/scan/NOPE").status_code == 404


@pytestmark_integration
def test_duplicate_barcode_rejected(client):
    item = new_item(client)
    r = client.post("/api/v1/inventory/items", json={
        "barcode": item["barcode"], "name": "dup", "physical_state": "solid", "unit": "units"})
    assert r.status_code == 409


@pytestmark_integration
def test_adjust_logs_and_never_goes_negative(client):
    item = new_item(client, quantity=5, min_quantity=3)
    as_user(CREW)
    r = client.post(f"/api/v1/inventory/items/{item['id']}/adjust", json={"delta": -3, "reason": "used"})
    assert r.json()["quantity"] == 2 and r.json()["low_stock"] is True
    assert client.post(f"/api/v1/inventory/items/{item['id']}/adjust",
                       json={"delta": -5, "reason": "too much"}).status_code == 409
    history = client.get(f"/api/v1/inventory/items/{item['id']}").json()["history"]
    assert [h["reason"] for h in history] == ["used", "initial stock"]
    assert client.get("/api/v1/inventory/items?low_stock=true").json()[0]["low_stock"] is True


@pytestmark_integration
def test_tool_checkout_and_checkin_logs_duration(client):
    tool = new_item(client, name="Torque wrench", is_tool=True, quantity=1)
    as_user(CREW)
    r = client.post("/api/v1/inventory/checkout", json={"barcode": tool["barcode"], "activity": "EVA-03"})
    assert r.status_code == 201 and r.json()["crew_id"] == "ev1"
    # second checkout of the same tool is refused while it's out
    as_user(CREW2)
    r = client.post("/api/v1/inventory/checkout", json={"barcode": tool["barcode"], "activity": "Lab"})
    assert r.status_code == 409 and "ev1" in r.json()["detail"]
    as_user(EDGE)
    assert client.get(f"/api/v1/inventory/scan/{tool['barcode']}").json()["checked_out"]["activity"] == "EVA-03"
    r = client.post("/api/v1/inventory/checkin", json={"barcode": tool["barcode"]})
    assert r.status_code == 200 and r.json()["duration_seconds"] >= 0
    assert client.post("/api/v1/inventory/checkin", json={"barcode": tool["barcode"]}).status_code == 409


@pytestmark_integration
def test_checkout_rules(client):
    tool = new_item(client, is_tool=True, quantity=1)
    consumable = new_item(client)
    as_user(CREW)
    assert client.post("/api/v1/inventory/checkout",
                       json={"barcode": consumable["barcode"], "activity": "x"}).status_code == 409
    assert client.post("/api/v1/inventory/checkout",   # crew can't assign to someone else
                       json={"barcode": tool["barcode"], "activity": "x", "crew_id": "ev2"}).status_code == 403
    as_user(EDGE)
    assert client.post("/api/v1/inventory/checkout",   # station must say who
                       json={"barcode": tool["barcode"], "activity": "x"}).status_code == 422
    r = client.post("/api/v1/inventory/checkout", json={"barcode": tool["barcode"], "activity": "EVA-03", "crew_id": "ev2"})
    assert r.status_code == 201 and r.json()["crew_id"] == "ev2" and r.json()["checked_out_by"] == EDGE.username
    as_user(EDGE)  # edge may not use crew-only endpoints
    assert client.get("/api/v1/inventory/items").status_code == 403


@pytestmark_integration
def test_repair_uses_two_bme280_and_decrements_stock(client):
    bme = new_item(client, name="BME280 sensor", quantity=5)
    solder = new_item(client, name="Solder", physical_state="solid", unit="g", quantity=100)
    as_user(CREW)
    r = client.post("/api/v1/repairs", json={
        "item_description": "Zone 2 environmental sensor board", "repair_minutes": 45, "signature": "EV1",
        "parts": [{"barcode": bme["barcode"], "quantity": 2}, {"item_id": solder["id"], "quantity": 3.5}]})
    assert r.status_code == 201, r.text
    assert r.json()["technician"] == "ev1"
    assert client.get(f"/api/v1/inventory/items/{bme['id']}").json()["quantity"] == 3
    assert client.get(f"/api/v1/inventory/items/{solder['id']}").json()["quantity"] == 96.5


@pytestmark_integration
def test_repair_with_insufficient_stock_changes_nothing(client):
    a = new_item(client, quantity=5)
    b = new_item(client, quantity=1)
    as_user(CREW)
    r = client.post("/api/v1/repairs", json={
        "item_description": "x", "repair_minutes": 5, "signature": "s",
        "parts": [{"item_id": a["id"], "quantity": 2}, {"item_id": b["id"], "quantity": 4}]})
    assert r.status_code == 409
    assert client.get(f"/api/v1/inventory/items/{a['id']}").json()["quantity"] == 5   # rolled back
    assert client.get(f"/api/v1/inventory/items/{b['id']}").json()["quantity"] == 1


@pytestmark_integration
def test_incident_with_photo(client):
    as_user(CREW)
    r = client.post("/api/v1/incidents",
                    data={"zone": "lab", "severity": "3", "description": "Coolant leak", "immediate_action": "Valve closed"},
                    files={"photo": ("../../leak.jpg", JPEG, "image/jpeg")})
    assert r.status_code == 201, r.text
    inc = r.json()
    assert inc["reported_by"] == "ev1" and inc["has_photo"] is True
    photo = client.get(f"/api/v1/incidents/{inc['id']}/photo")
    assert photo.status_code == 200 and photo.content == JPEG and photo.headers["content-type"] == "image/jpeg"
    assert os.listdir(inv.MEDIA_DIR) == [f"incident_{inc['id']}.jpg"]   # server-chosen name
    bad = client.post("/api/v1/incidents", data={"zone": "lab", "severity": "2", "description": "x"},
                      files={"photo": ("x.jpg", b"<?php", "image/jpeg")})
    assert bad.status_code == 415
    assert client.post("/api/v1/incidents", data={"zone": "lab", "severity": "9", "description": "x"}).status_code == 422


@pytestmark_integration
def test_delete_needs_commander_or_mcc(client):
    item = new_item(client)
    as_user(CREW)
    assert client.delete(f"/api/v1/inventory/items/{item['id']}").status_code == 403
    as_user(CDR)
    assert client.delete(f"/api/v1/inventory/items/{item['id']}").status_code == 204
