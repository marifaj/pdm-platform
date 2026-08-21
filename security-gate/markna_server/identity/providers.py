"""Pluggable authentication.

An authentication provider turns an inbound HTTP request into a
:class:`~markna_server.identity.principal.Principal`, or into nothing. Services
and HTTP handlers depend on the ``Principal``, never on how it was obtained, so
adding Entra ID (or any OIDC provider, or SAML, or mTLS) is a new class in this
module plus one line of configuration.

Three providers ship in v1.0:

* :class:`ApiTokenProvider`      — ``Authorization: Bearer mkna_…`` for the API.
* :class:`SessionProvider`       — an opaque, server-side session cookie for the UI.
* :class:`TrustedHeaderProvider` — identity asserted by an authenticating reverse
  proxy. This is the seam an Entra ID deployment uses today: front the app with
  Azure App Service Authentication, oauth2-proxy or an equivalent, and map its
  identity header onto an existing user. It is off unless configured, because a
  header-trusting provider that is reachable directly is an authentication
  bypass.

See ``docs/architecture.md`` for what a native OIDC provider would implement.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Protocol, Sequence

from .credentials import ApiToken, Session
from .passwords import hash_api_token
from .principal import Principal, User, permissions_for

SESSION_COOKIE = "markna_session"


@dataclass
class AuthRequest:
    """The parts of an HTTP request an authenticator is allowed to see."""

    method: str = "GET"
    path: str = "/"
    headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)
    remote_addr: str = ""

    def header(self, name: str) -> Optional[str]:
        return self.headers.get(name.lower())


@dataclass
class Challenge:
    """How the edge should ask an anonymous caller to authenticate."""

    status: int = 401
    headers: Dict[str, str] = field(default_factory=dict)
    redirect_to: Optional[str] = None


class IdentityStore(Protocol):
    """The lookups an authentication provider needs.

    A port, not a repository: it is implemented by the storage layer, but the
    providers must not know that storage is SQLite, or that users and tokens
    live in the same database.
    """

    def get_user(self, user_id: str) -> Optional[User]: ...

    def find_user_by_email(self, organization_id: str, email: str) -> Optional[User]: ...

    def find_user_by_subject(self, issuer: str, subject: str) -> Optional[User]: ...

    def find_api_token(self, token_hash: str) -> Optional[ApiToken]: ...

    def touch_api_token(self, token_id: str) -> None: ...

    def get_session(self, session_id: str) -> Optional[Session]: ...


class AuthenticationProvider(Protocol):
    """Turn a request into a principal, or return ``None``."""

    name: str

    def authenticate(self, request: AuthRequest) -> Optional[Principal]: ...

    def challenge(self, request: AuthRequest) -> Optional[Challenge]: ...


class _BaseProvider:
    name = "base"

    def __init__(self, store: IdentityStore) -> None:
        self.store = store

    def challenge(self, request: AuthRequest) -> Optional[Challenge]:
        return None

    def _principal_for(self, user: Optional[User], method: str, *, service: bool = False):
        if user is None or not user.is_active:
            return None
        principal = user.to_principal(method)
        if not service:
            return principal
        return Principal(
            id=principal.id,
            organization_id=principal.organization_id,
            display_name=principal.display_name,
            roles=principal.roles,
            email=principal.email,
            auth_method=method,
            subject=principal.subject,
            is_service=True,
        )


class ApiTokenProvider(_BaseProvider):
    """Bearer tokens for machine callers and CI."""

    name = "api-token"

    def authenticate(self, request: AuthRequest) -> Optional[Principal]:
        header = request.header("authorization") or ""
        scheme, _, value = header.partition(" ")
        if scheme.lower() != "bearer" or not value.strip():
            return None
        token = self.store.find_api_token(hash_api_token(value.strip()))
        if token is None or not token.is_usable:
            return None
        principal = self._principal_for(self.store.get_user(token.user_id), self.name, service=True)
        if principal is None:
            return None

        # The token carries its own roles, but never more than its owner holds
        # *now*: demoting the user invalidates their over-privileged tokens on
        # the next request, without having to hunt them down. Fails closed —
        # a token that outgrew its owner stops working rather than narrowing.
        effective = frozenset(token.roles)
        if not effective or permissions_for(effective) - principal.permissions:
            return None
        self.store.touch_api_token(token.id)
        return Principal(
            id=principal.id,
            organization_id=principal.organization_id,
            display_name=f"{principal.display_name} (token: {token.name})",
            roles=effective,
            email=principal.email,
            auth_method=self.name,
            subject=principal.subject,
            is_service=True,
        )

    def challenge(self, request: AuthRequest) -> Optional[Challenge]:
        return Challenge(status=401, headers={"WWW-Authenticate": 'Bearer realm="markna"'})


class SessionProvider(_BaseProvider):
    """Opaque server-side sessions for the browser UI."""

    name = "session"

    def __init__(self, store: IdentityStore, cookie_name: str = SESSION_COOKIE) -> None:
        super().__init__(store)
        self.cookie_name = cookie_name

    def authenticate(self, request: AuthRequest) -> Optional[Principal]:
        session_id = request.cookies.get(self.cookie_name)
        if not session_id:
            return None
        session = self.store.get_session(session_id)
        if session is None or not session.is_usable:
            return None
        return self._principal_for(self.store.get_user(session.user_id), self.name)

    def challenge(self, request: AuthRequest) -> Optional[Challenge]:
        return Challenge(status=302, redirect_to="/login")


class TrustedHeaderProvider(_BaseProvider):
    """Identity asserted by an authenticating reverse proxy.

    Only enable this when the application is unreachable except through that
    proxy — otherwise anyone can set the header. The provider matches an
    existing, active user; it never auto-provisions, so revoking access in the
    directory is not the only control.
    """

    name = "trusted-header"

    def __init__(
        self,
        store: IdentityStore,
        *,
        email_header: str = "x-auth-request-email",
        subject_header: Optional[str] = None,
        issuer: Optional[str] = None,
        organization_id: Optional[str] = None,
    ) -> None:
        super().__init__(store)
        self.email_header = email_header.lower()
        self.subject_header = subject_header.lower() if subject_header else None
        self.issuer = issuer
        self.organization_id = organization_id

    def authenticate(self, request: AuthRequest) -> Optional[Principal]:
        if self.subject_header and self.issuer:
            subject = request.header(self.subject_header)
            if subject:
                user = self.store.find_user_by_subject(self.issuer, subject)
                if user is not None:
                    return self._principal_for(user, self.name)
        email = request.header(self.email_header)
        if not email or not self.organization_id:
            return None
        return self._principal_for(
            self.store.find_user_by_email(self.organization_id, email.strip().lower()), self.name
        )


class ProviderChain:
    """Try each provider in order; the first to recognise the caller wins."""

    def __init__(self, providers: Sequence[AuthenticationProvider]) -> None:
        if not providers:
            raise ValueError("at least one authentication provider must be configured")
        self.providers: List[AuthenticationProvider] = list(providers)

    @property
    def names(self) -> List[str]:
        return [provider.name for provider in self.providers]

    def authenticate(self, request: AuthRequest) -> Optional[Principal]:
        for provider in self.providers:
            principal = provider.authenticate(request)
            if principal is not None:
                return principal
        return None

    def challenge(self, request: AuthRequest, *, prefer: str = "") -> Challenge:
        """The challenge to issue for an unauthenticated request."""
        ordered = self.providers
        if prefer:
            ordered = sorted(ordered, key=lambda provider: provider.name != prefer)
        for provider in ordered:
            challenge = provider.challenge(request)
            if challenge is not None:
                return challenge
        return Challenge(status=401)


#: Provider constructors by configuration name. Adding an OIDC/Entra ID provider
#: means implementing :class:`AuthenticationProvider` and registering it here.
PROVIDER_FACTORIES: Dict[str, Callable[..., AuthenticationProvider]] = {
    ApiTokenProvider.name: ApiTokenProvider,
    SessionProvider.name: SessionProvider,
    TrustedHeaderProvider.name: TrustedHeaderProvider,
}


def build_chain(
    store: IdentityStore,
    names: Sequence[str],
    options: Optional[Dict[str, Dict[str, object]]] = None,
) -> ProviderChain:
    """Construct the configured chain, failing loudly on an unknown name."""
    options = options or {}
    providers: List[AuthenticationProvider] = []
    for name in names:
        factory = PROVIDER_FACTORIES.get(name)
        if factory is None:
            known = ", ".join(sorted(PROVIDER_FACTORIES))
            raise ValueError(f"unknown authentication provider {name!r}; known providers: {known}")
        providers.append(factory(store, **options.get(name, {})))
    return ProviderChain(providers)
