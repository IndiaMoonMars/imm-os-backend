"""
Shared authentication for IMM-OS FastAPI services.

Two kinds of caller are accepted:
  - Crew / MCC users: `Authorization: Bearer <Keycloak access token>` from the
    IndiaMoonMars realm. Signature (RS256, realm JWKS), issuer, audience and
    expiry are verified; identity comes from `preferred_username` and roles
    from `realm_access.roles`.
  - Internal services (docker network only): `X-IMM-Service-Token` matching
    IMM_SERVICE_TOKEN. Interim until each service gets its own Keycloak
    client-credentials client.

Usage:
    from services.auth import User, current_user, require_roles, ensure_self_or_roles
    @app.get("/x", dependencies=[Depends(current_user)])           # any logged-in user
    async def y(user: User = Depends(require_roles("flight_surgeon"))): ...
    ensure_self_or_roles(user, crew_id, "flight_surgeon")          # own data or privileged
"""
import hmac
import os
from dataclasses import dataclass, field
from typing import FrozenSet, Optional

import jwt
from fastapi import Depends, Header, HTTPException

KEYCLOAK_ISSUER = os.getenv(
    "KEYCLOAK_ISSUER", "http://imm.local/auth/realms/IndiaMoonMars")
KEYCLOAK_JWKS_URL = os.getenv(
    "KEYCLOAK_JWKS_URL",
    "http://keycloak:8080/auth/realms/IndiaMoonMars/protocol/openid-connect/certs")
KEYCLOAK_AUDIENCE = os.getenv("KEYCLOAK_AUDIENCE", "imm-api")
SERVICE_TOKEN_HEADER = "X-IMM-Service-Token"

# Realm roles
CREW = "crew"
COMMANDER = "commander"
FLIGHT_SURGEON = "flight_surgeon"
MCC_OPERATOR = "mcc_operator"
SERVICE = "service"  # internal service-to-service calls only
IMM_ROLES = frozenset({CREW, COMMANDER, FLIGHT_SURGEON, MCC_OPERATOR})

_jwks_client: Optional[jwt.PyJWKClient] = None


def _get_jwks_client() -> jwt.PyJWKClient:
    global _jwks_client
    if _jwks_client is None:
        _jwks_client = jwt.PyJWKClient(KEYCLOAK_JWKS_URL, cache_keys=True, lifespan=300)
    return _jwks_client


@dataclass(frozen=True)
class User:
    username: str
    roles: FrozenSet[str] = field(default_factory=frozenset)

    @property
    def is_service(self) -> bool:
        return SERVICE in self.roles

    def has_any(self, *roles: str) -> bool:
        return bool(self.roles.intersection(roles))

    def is_self(self, subject_id: Optional[str]) -> bool:
        # Keycloak lower-cases usernames; crew IDs elsewhere may not be
        return subject_id is not None and subject_id.lower() == self.username.lower()


def service_headers() -> dict:
    """Headers for calling another IMM-OS service from inside the stack."""
    token = os.getenv("IMM_SERVICE_TOKEN", "")
    return {SERVICE_TOKEN_HEADER: token} if token else {}


def decode_token(token: str) -> dict:
    signing_key = _get_jwks_client().get_signing_key_from_jwt(token).key
    return jwt.decode(
        token, signing_key, algorithms=["RS256"],
        audience=KEYCLOAK_AUDIENCE, issuer=KEYCLOAK_ISSUER,
        options={"require": ["exp", "iss", "aud"]},
    )


def current_user(
    authorization: Optional[str] = Header(None),
    x_imm_service_token: Optional[str] = Header(None),
) -> User:
    expected = os.getenv("IMM_SERVICE_TOKEN", "")
    if x_imm_service_token is not None:
        if expected and hmac.compare_digest(x_imm_service_token, expected):
            return User(username="imm-service", roles=frozenset({SERVICE}))
        raise HTTPException(401, "Invalid service token")

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Missing bearer token",
                            headers={"WWW-Authenticate": "Bearer"})
    try:
        claims = decode_token(authorization[7:].strip())
    except jwt.PyJWKClientError:
        raise HTTPException(503, "Identity provider unavailable")
    except jwt.PyJWTError:
        raise HTTPException(401, "Invalid or expired token",
                            headers={"WWW-Authenticate": "Bearer"})

    username = claims.get("preferred_username")
    if not username:
        raise HTTPException(401, "Token has no username")
    roles = frozenset(claims.get("realm_access", {}).get("roles", []))
    # Realm accounts carry Keycloak default roles; require an explicit IMM-OS role
    if not roles & IMM_ROLES:
        raise HTTPException(403, "No IMM-OS role assigned to this account")
    return User(username=username, roles=roles)


def require_roles(*roles: str):
    """Dependency: caller must hold at least one of `roles` (services always pass)."""
    def checker(user: User = Depends(current_user)) -> User:
        if not (user.is_service or user.has_any(*roles)):
            raise HTTPException(403, "Insufficient role")
        return user
    return checker


def ensure_self_or_roles(user: User, subject_id: Optional[str], *roles: str) -> None:
    """Allow access to `subject_id`'s data only for that user, a privileged role, or a service."""
    if user.is_service or user.is_self(subject_id) or user.has_any(*roles):
        return
    raise HTTPException(403, "Access denied")
