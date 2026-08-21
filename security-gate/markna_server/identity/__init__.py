"""Identity: who is calling, and what they may do.

Authentication is pluggable (:mod:`markna_server.identity.providers`);
authorisation is centralised (:class:`markna_server.identity.access.AccessControl`).
Nothing outside this package should reference a provider by name.
"""

from .access import AccessControl
from .credentials import ApiToken, Session, expiry_from_now, is_expired
from .passwords import generate_api_token, hash_api_token, hash_password, verify_password
from .principal import Permission, Principal, Role, User, permissions_for
from .providers import (
    SESSION_COOKIE,
    ApiTokenProvider,
    AuthenticationProvider,
    AuthRequest,
    Challenge,
    IdentityStore,
    ProviderChain,
    SessionProvider,
    TrustedHeaderProvider,
    build_chain,
)

__all__ = [
    "AccessControl", "ApiToken", "ApiTokenProvider", "AuthRequest",
    "AuthenticationProvider", "Challenge", "IdentityStore", "Permission",
    "Principal", "ProviderChain", "Role", "SESSION_COOKIE", "Session",
    "SessionProvider", "TrustedHeaderProvider", "User", "build_chain",
    "expiry_from_now", "generate_api_token", "hash_api_token", "hash_password",
    "is_expired", "permissions_for", "verify_password",
]
