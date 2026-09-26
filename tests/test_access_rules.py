"""
Access-rule tests for crew-facing services: identity now comes from the
verified token (not query params / body fields), so crew can only act on
their own data unless they hold a privileged role.
DB access is faked; the logged-in user is set via dependency_overrides.
"""
from contextlib import asynccontextmanager

import pytest
from fastapi.testclient import TestClient

from services import comms_api, medical_api, psychology_api
from services.auth import User, current_user

EV1 = User("ev1", frozenset({"crew"}))
EV2 = User("ev2", frozenset({"crew"}))
SURGEON = User("doc", frozenset({"flight_surgeon"}))
MCC = User("capcom", frozenset({"mcc_operator"}))
SERVICE = User("imm-service", frozenset({"service"}))


class FakeConn:
    """Answers every query with the configured canned values."""
    def __init__(self, rows=None, row=None, val=None):
        self.rows, self.row, self.val = rows or [], row, val

    async def fetch(self, *a):
        return self.rows

    async def fetchrow(self, *a):
        return self.row

    async def fetchval(self, *a):
        return self.val

    async def execute(self, *a):
        return "OK"

    async def close(self):
        pass


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def login(app, user):
    app.dependency_overrides[current_user] = lambda: user


@pytest.fixture(autouse=True)
def reset_overrides():
    yield
    for app in (comms_api.app, medical_api.app, psychology_api.app):
        app.dependency_overrides.clear()


# ── Comms ─────────────────────────────────────────────────────────

@pytest.fixture
def comms(monkeypatch):
    conn = FakeConn()

    async def get_conn():
        return conn

    async def delay():
        return 480.0

    async def day():
        return 1

    monkeypatch.setattr(comms_api, "get_conn", get_conn)
    monkeypatch.setattr(comms_api, "current_delay_seconds", delay)
    monkeypatch.setattr(comms_api, "current_mission_day", day)
    return conn


comms_client = TestClient(comms_api.app)
JOURNAL = {"id": 1, "mission_day": 1, "title": "t", "body": "secret", "author_id": "ev1"}


@pytest.mark.parametrize("user,expect_body", [(EV1, "secret"), (SURGEON, "secret"), (EV2, "[PRIVATE]")])
def test_journal_privacy_from_token(comms, user, expect_body):
    comms.rows = [JOURNAL]
    login(comms_api.app, user)
    r = comms_client.get("/api/v1/journal/entries/ev1")
    assert r.status_code == 200 and r.json()[0]["body"] == expect_body


def test_journal_requester_role_param_no_longer_grants_access(comms):
    comms.rows = [JOURNAL]
    login(comms_api.app, EV2)
    r = comms_client.get("/api/v1/journal/entries/ev1?requester_role=flight_surgeon")
    assert r.json()[0]["body"] == "[PRIVATE]"
    assert comms_client.get("/api/v1/journal/search?author_id=ev1&keyword=x").status_code == 403


def test_cannot_write_journal_or_upload_as_someone_else(comms):
    login(comms_api.app, EV2)
    r = comms_client.post("/api/v1/journal/entry", json={"author_id": "ev1", "body": "x"})
    assert r.status_code == 403
    comms.val = "ev1"  # journal 1 belongs to ev1
    r = comms_client.post("/api/v1/journal/upload/1", files={"file": ("a.wav", b"x")})
    assert r.status_code == 403


def test_message_sender_must_be_caller(comms):
    login(comms_api.app, EV2)
    r = comms_client.post("/api/v1/comms/message",
                          json={"sender_id": "ev1", "recipient_group": "mcc", "body": "hi"})
    assert r.status_code == 403


@pytest.mark.parametrize("user,group,expected_delay", [
    (EV1, "mcc", 480.0),      # habitat crew → MCC is delayed
    (EV1, "astro", 0.0),      # astro ↔ astro is immediate
    (MCC, "astro", 0.0),      # MCC → habitat not delayed at send time
])
def test_comm_delay_uses_role(comms, user, group, expected_delay):
    comms.row = {"id": 7}
    login(comms_api.app, user)
    r = comms_client.post("/api/v1/comms/message",
                          json={"sender_id": user.username, "recipient_group": group, "body": "hi"})
    assert r.status_code == 201 and r.json()["delay_seconds"] == expected_delay


def test_internal_service_can_send_system_messages(comms):
    comms.row = {"id": 7}
    login(comms_api.app, SERVICE)
    r = comms_client.post("/api/v1/comms/message",
                          json={"sender_id": "medical-system", "recipient_group": "mcc", "body": "alert"})
    assert r.status_code == 201 and r.json()["delay_seconds"] == 0.0


def test_inbox_and_pending_are_self_only(comms):
    login(comms_api.app, EV2)
    assert comms_client.get("/api/v1/comms/inbox/ev1").status_code == 403
    assert comms_client.get("/api/v1/comms/pending/ev1").status_code == 403
    assert comms_client.get("/api/v1/comms/inbox/ev2").status_code == 200


def test_push_send_needs_mcc_or_commander(comms):
    login(comms_api.app, EV1)
    assert comms_client.post("/api/v1/push/send?user_id=ev2&title=t&body=b").status_code == 403


def test_comms_requires_login(comms):
    assert comms_client.get("/api/v1/comms/thread/1").status_code == 401


# ── Medical ───────────────────────────────────────────────────────

@pytest.fixture
def medical(monkeypatch):
    conn = FakeConn()
    monkeypatch.setattr(medical_api, "pool", FakePool(conn))
    return conn


med_client = TestClient(medical_api.app)


@pytest.mark.parametrize("user,status", [(EV1, 200), (SURGEON, 200), (EV2, 403)])
def test_medical_readings_self_or_surgeon(medical, user, status):
    login(medical_api.app, user)
    assert med_client.get("/api/v1/medical/readings/ev1").status_code == status


def test_requester_id_param_no_longer_grants_access(medical):
    login(medical_api.app, EV2)
    assert med_client.get("/api/v1/medical/readings/ev1?requester_id=ev1").status_code == 403
    assert med_client.get("/api/v1/medical/all-crew?requester_id=doc").status_code == 403


def test_cannot_post_readings_or_take_doses_for_others(medical):
    login(medical_api.app, EV2)
    r = med_client.post("/api/v1/medical/reading",
                        json={"crew_id": "ev1", "reading_type": "hr", "value": 70, "unit": "bpm"})
    assert r.status_code == 403
    medical.val = "ev1"  # medication 5 belongs to ev1
    assert med_client.patch("/api/v1/medical/medication/5/taken").status_code == 403


def test_all_crew_for_surgeon(medical):
    login(medical_api.app, SURGEON)
    assert med_client.get("/api/v1/medical/all-crew").status_code == 200


# ── Psychology ────────────────────────────────────────────────────

@pytest.fixture
def psych(monkeypatch):
    conn = FakeConn()
    monkeypatch.setattr(psychology_api, "pool", FakePool(conn))
    return conn


psych_client = TestClient(psychology_api.app)


def test_sociogram_rater_is_caller(psych):
    login(psychology_api.app, EV2)
    spoof = {"rater_id": "ev1", "ratee_id": "ev3", "comfort_score": 1}
    assert psych_client.post("/api/v1/psych/sociogram", json=spoof).status_code == 403
    own = {"rater_id": "ev2", "ratee_id": "ev3", "comfort_score": 4}
    assert psych_client.post("/api/v1/psych/sociogram", json=own).status_code == 200


def test_outgoing_ratings_private_even_from_surgeon(psych):
    login(psychology_api.app, SURGEON)
    assert psych_client.get("/api/v1/psych/sociogram/my-ratings/ev1").status_code == 403


@pytest.mark.parametrize("user,status", [(EV1, 403), (SURGEON, 200)])
def test_sociogram_aggregate_surgeon_only(psych, user, status):
    login(psychology_api.app, user)
    assert psych_client.get("/api/v1/psych/sociogram/aggregate").status_code == status


@pytest.mark.parametrize("user,status", [(EV1, 200), (SURGEON, 200), (EV2, 403)])
def test_trends_self_or_surgeon(psych, user, status):
    login(psychology_api.app, user)
    assert psych_client.get("/api/v1/psych/trends/ev1").status_code == status
