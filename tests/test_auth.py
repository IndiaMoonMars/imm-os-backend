"""
Auth module tests — real RS256 tokens signed with a throwaway key; the
Keycloak JWKS fetch is replaced with that key's public half.
"""
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from services import auth
from services.auth import User, current_user, ensure_self_or_roles, require_roles

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


class FakeSigningKey:
    key = KEY.public_key()


class FakeJwksClient:
    def get_signing_key_from_jwt(self, token):
        return FakeSigningKey()


@pytest.fixture(autouse=True)
def fake_keycloak(monkeypatch):
    monkeypatch.setattr(auth, "_get_jwks_client", lambda: FakeJwksClient())
    monkeypatch.setenv("IMM_SERVICE_TOKEN", "internal-secret")


def make_token(key=KEY, **overrides):
    claims = {
        "iss": auth.KEYCLOAK_ISSUER,
        "aud": auth.KEYCLOAK_AUDIENCE,
        "exp": int(time.time()) + 300,
        "preferred_username": "ev1",
        "realm_access": {"roles": ["crew"]},
    }
    claims.update(overrides)
    claims = {k: v for k, v in claims.items() if v is not None}
    return jwt.encode(claims, key, algorithm="RS256", headers={"kid": "test"})


app = FastAPI()


@app.get("/me")
def me(user: User = Depends(current_user)):
    return {"username": user.username, "roles": sorted(user.roles)}


@app.get("/surgeon", dependencies=[Depends(require_roles("flight_surgeon"))])
def surgeon_only():
    return {"ok": True}


client = TestClient(app)


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


def test_valid_token_yields_user_and_roles():
    r = client.get("/me", headers=bearer(make_token()))
    assert r.status_code == 200
    assert r.json() == {"username": "ev1", "roles": ["crew"]}


@pytest.mark.parametrize("token", [
    make_token(exp=int(time.time()) - 10),        # expired
    make_token(aud="some-other-client"),          # wrong audience
    make_token(iss="http://evil/realms/x"),       # wrong issuer
    make_token(key=OTHER_KEY),                    # not signed by the realm
    make_token(aud=None),                         # audience missing
])
def test_rejected_tokens(token):
    assert client.get("/me", headers=bearer(token)).status_code == 401


def test_account_without_imm_role_is_refused():
    token = make_token(realm_access={"roles": ["offline_access", "default-roles-indiamoonmars"]})
    assert client.get("/me", headers=bearer(token)).status_code == 403


def test_missing_or_malformed_header():
    assert client.get("/me").status_code == 401
    assert client.get("/me", headers={"Authorization": "Basic abc"}).status_code == 401
    assert client.get("/me", headers=bearer("not-a-jwt")).status_code == 401


def test_service_token():
    r = client.get("/me", headers={"X-IMM-Service-Token": "internal-secret"})
    assert r.status_code == 200 and r.json()["roles"] == ["service"]
    assert client.get("/me", headers={"X-IMM-Service-Token": "guess"}).status_code == 401


def test_service_token_disabled_when_unset(monkeypatch):
    monkeypatch.setenv("IMM_SERVICE_TOKEN", "")
    assert client.get("/me", headers={"X-IMM-Service-Token": ""}).status_code == 401


def test_require_roles():
    assert client.get("/surgeon", headers=bearer(make_token())).status_code == 403
    fs = make_token(preferred_username="doc", realm_access={"roles": ["flight_surgeon"]})
    assert client.get("/surgeon", headers=bearer(fs)).status_code == 200
    assert client.get("/surgeon", headers={"X-IMM-Service-Token": "internal-secret"}).status_code == 200


def test_ensure_self_or_roles():
    crew = User("ev1", frozenset({"crew"}))
    ensure_self_or_roles(crew, "EV1")  # case-insensitive self match
    with pytest.raises(Exception) as e:
        ensure_self_or_roles(crew, "ev2", "flight_surgeon")
    assert e.value.status_code == 403
    ensure_self_or_roles(User("doc", frozenset({"flight_surgeon"})), "ev2", "flight_surgeon")
    ensure_self_or_roles(User("svc", frozenset({"service"})), "anyone")


def test_service_headers(monkeypatch):
    assert auth.service_headers() == {"X-IMM-Service-Token": "internal-secret"}
    monkeypatch.setenv("IMM_SERVICE_TOKEN", "")
    assert auth.service_headers() == {}
