"""
Upload path safety: client-supplied filenames must never place files outside MEDIA_DIR.
"""
import os

import pytest
from fastapi.testclient import TestClient

from services import comms_api
from services.auth import User, current_user

EV1 = User("ev1", frozenset({"crew"}))


class FakeConn:
    async def fetchval(self, *a):
        return "ev1"          # journal owner

    async def fetchrow(self, *a):
        return {"id": 1}

    async def execute(self, *a):
        return "OK"

    async def close(self):
        pass


@pytest.fixture
def client(monkeypatch, tmp_path):
    media = tmp_path / "media"
    media.mkdir()
    monkeypatch.setattr(comms_api, "MEDIA_DIR", str(media))

    async def get_conn():
        return FakeConn()

    async def day():
        return 1

    monkeypatch.setattr(comms_api, "get_conn", get_conn)
    monkeypatch.setattr(comms_api, "current_mission_day", day)
    comms_api.app.dependency_overrides[current_user] = lambda: EV1
    yield TestClient(comms_api.app), media, tmp_path
    comms_api.app.dependency_overrides.clear()


def files_outside(root, media):
    return [os.path.join(d, f) for d, _, fs in os.walk(root) for f in fs
            if not os.path.join(d, f).startswith(str(media) + os.sep)]


@pytest.mark.parametrize("name", [
    "../../evil.txt", "..\\..\\evil.txt", "/etc/evil.txt", "....//evil.txt", "..",
])
def test_journal_upload_stays_in_media_dir(client, name):
    c, media, root = client
    r = c.post("/api/v1/journal/upload/1", files={"file": (name, b"x")})
    assert r.status_code == 200
    assert "/" not in r.json()["stored"] and "\\" not in r.json()["stored"]
    assert files_outside(root, media) == []
    assert len(os.listdir(media)) == 1


def test_empty_filename_is_rejected(client):
    c, media, root = client
    r = c.post("/api/v1/journal/upload/1", files={"file": ("", b"x")})
    assert r.status_code == 422
    assert os.listdir(media) == [] and files_outside(root, media) == []


def test_videolog_upload_stays_in_media_dir(client):
    c, media, root = client
    r = c.post("/api/v1/videolog/upload", data={"crew_id": "ev1"},
               files={"file": ("../../../evil.mp4", b"x", "video/mp4")})
    assert r.status_code == 201
    assert r.json()["path"].startswith("vlog_ev1_") and r.json()["path"].endswith("evil.mp4")
    assert files_outside(root, media) == []


def test_normal_filename_is_kept(client):
    c, media, _ = client
    r = c.post("/api/v1/journal/upload/1", files={"file": ("memo 01.wav", b"x")})
    assert r.json()["stored"] == "journal_1_memo_01.wav"
    assert os.path.exists(media / "journal_1_memo_01.wav")
