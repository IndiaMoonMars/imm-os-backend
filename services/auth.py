"""
Shared authentication for IMM-OS FastAPI services.

Callers:
  - Crew / MCC users: `Authorization: Bearer <Keycloak access token>` from the
    IndiaMoonMars realm (browser login). Signature (RS256, realm JWKS), issuer,
    audience and expiry are verified; identity comes from `preferred_username`
    and roles from `realm_access.roles`.
  - Edge devices (RPi / Jetson): Bearer token from the `imm-edge`
    client-credentials client, carrying the `edge_device` role.
  - Internal services (docker network only): `X-IMM-Service-Token` matching
    IMM_SERVICE_TOKEN.
  - Third-party webhooks that can't use Keycloak: a shared secret
    (require_shared_secret).

Usage:
    from services.auth import User, current_user, edge_device, require_roles, ensure_self_or_roles
    @app.get("/x", dependencies=[Depends(current_user)])           # any crew/MCC user
    @app.post("/y", dependencies=[Depends(edge_device)])           # edge devices
    async def z(user: User = Depends(require_roles("flight_surgeon"))): ...
    ensure_self_or_roles(user, crew_id, "flight_surgeon")          # own data or privileged
"""
import hmac
import os
from dataclasses import dataclass, field
from typing import FrozenSet, Optional

import jwt
from fastapi import Depends, Header, HTTPException, Query

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
EDGE_DEVICE = "edge_device"  # imm-edge client-credentials service account
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


def user_from_token(token: str) -> User:
    """Verify a Keycloak access token and return its user (no role check)."""
    try:
        claims = decode_token(token)
    except jwt.PyJWKClientError:
        raise HTTPException(503, "Identity provider unavailable")
    except jwt.PyJWTError:
        raise HTTPException(401, "Invalid or expired token",
                            headers={"WWW-Authenticate": "Bearer"})
    username = claims.get("preferred_username")
    if not username:
        raise HTTPException(401, "Token has no username")
    return User(username=username, roles=frozenset(claims.get("realm_access", {}).get("roles", [])))


def authenticated(
    authorization: Optional[str] = Header(None),
    x_imm_service_token: Optional[str] = Header(None),
) -> User:
    """Any verified caller: service token or a valid realm token (roles not checked)."""
    expected = os.getenv("IMM_SERVICE_TOKEN", "")
    if x_imm_service_token is not None:
        if expected and hmac.compare_digest(x_imm_service_token, expected):
            return User(username="imm-service", roles=frozenset({SERVICE}))
        raise HTTPException(401, "Invalid service token")

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Missing bearer token",
                            headers={"WWW-Authenticate": "Bearer"})
    return user_from_token(authorization[7:].strip())


def current_user(user: User = Depends(authenticated)) -> User:
    """Crew / MCC user (or internal service)."""
    # Realm accounts carry Keycloak default roles, and edge devices carry only
    # edge_device; require an explicit crew/MCC role for human-facing APIs
    if not (user.is_service or user.roles & IMM_ROLES):
        raise HTTPException(403, "No IMM-OS role assigned to this account")
    return user


def edge_device(user: User = Depends(authenticated)) -> User:
    """Edge device (imm-edge service account) or internal service."""
    if not (user.is_service or EDGE_DEVICE in user.roles):
        raise HTTPException(403, "Edge device credentials required")
    return user


def require_shared_secret(env_var: str, header: str = "X-IMM-Webhook-Token"):
    """
    Dependency for third-party webhooks that can't obtain Keycloak tokens.
    The secret may be sent in `header` or a `token` query parameter (for
    providers that only let you configure a URL). Disabled (503) when the
    env var is unset, so an unconfigured webhook is never open.
    """
    def checker(
        header_token: Optional[str] = Header(None, alias=header),
        token: Optional[str] = Query(None),
    ) -> None:
        expected = os.getenv(env_var, "")
        if not expected:
            raise HTTPException(503, "Webhook not configured")
        provided = header_token or token or ""
        if not hmac.compare_digest(provided, expected):
            raise HTTPException(401, "Invalid webhook token")
    return checker


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
