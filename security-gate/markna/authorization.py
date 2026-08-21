"""Authorisation and scope control for the environment layer.

The architecture and code layers read artefacts the operator already owns. The
environment layer sends traffic to a running system, which is only acceptable
against a target the operator is permitted to test. MARKNA refuses to run
environment probes without a recorded authorisation, and refuses to touch a host
outside the declared scope even if a redirect points there.

All probes are read-only: GET/HEAD/OPTIONS with no payloads, no fuzzing, no
credential guessing, no state-changing verbs.
"""

from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


class AuthorizationError(ValueError):
    """Raised when environment testing is requested without valid authorisation."""


class ScopeError(ValueError):
    """Raised when a URL falls outside the authorised scope."""


@dataclass
class Authorization:
    """Recorded permission to probe a deployed environment."""

    authorized_by: str
    reference: Optional[str] = None          # change ticket / engagement id
    expires: Optional[str] = None            # YYYY-MM-DD
    scope_hosts: List[str] = field(default_factory=list)
    allow_private_targets: bool = False
    note: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Authorization":
        authorized_by = str(data.get("authorized_by") or "").strip()
        if not authorized_by:
            raise AuthorizationError(
                "authorization.authorized_by is required: record who permitted this test."
            )
        return cls(
            authorized_by=authorized_by,
            reference=_optional_str(data.get("reference")),
            expires=_optional_str(data.get("expires")),
            scope_hosts=[str(h).strip().lower() for h in data.get("scope_hosts", []) if str(h).strip()],
            allow_private_targets=bool(data.get("allow_private_targets", False)),
            note=str(data.get("note") or ""),
        )

    def validate(self, today: Optional[date] = None) -> None:
        if not self.authorized_by.strip():
            raise AuthorizationError("authorization.authorized_by must not be empty")
        if self.expires:
            try:
                expiry = date.fromisoformat(self.expires)
            except ValueError as exc:
                raise AuthorizationError(
                    f"authorization.expires must be YYYY-MM-DD, got {self.expires!r}"
                ) from exc
            if expiry < (today or date.today()):
                raise AuthorizationError(
                    f"authorization expired on {self.expires}; obtain fresh permission before testing"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "authorized_by": self.authorized_by,
            "reference": self.reference,
            "expires": self.expires,
            "scope_hosts": list(self.scope_hosts),
            "allow_private_targets": self.allow_private_targets,
            "note": self.note,
        }


@dataclass
class Scope:
    """Host allow-list derived from the target URL plus any extra scope hosts."""

    hosts: List[str]
    allow_private: bool = False

    @classmethod
    def for_target(cls, url: str, authorization: Authorization) -> "Scope":
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ScopeError(f"target URL must be http(s), got {parsed.scheme or 'no scheme'}")
        if not parsed.hostname:
            raise ScopeError(f"target URL has no host: {url}")
        hosts = {parsed.hostname.lower()}
        hosts.update(authorization.scope_hosts)
        scope = cls(hosts=sorted(hosts), allow_private=authorization.allow_private_targets)
        scope.check(url)
        return scope

    def contains_host(self, host: str) -> bool:
        host = (host or "").lower()
        for allowed in self.hosts:
            if host == allowed:
                return True
            if allowed.startswith("*.") and host.endswith(allowed[1:]):
                return True
        return False

    def check(self, url: str) -> None:
        """Raise :class:`ScopeError` unless ``url`` may be requested."""
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ScopeError(f"refusing non-http(s) URL: {url}")
        host = parsed.hostname
        if not host:
            raise ScopeError(f"refusing URL without a host: {url}")
        if not self.contains_host(host):
            raise ScopeError(
                f"host '{host}' is not in the authorised scope ({', '.join(self.hosts)}); "
                "add it to authorization.scope_hosts if testing it is permitted"
            )
        if not self.allow_private and is_private_target(host):
            raise ScopeError(
                f"host '{host}' resolves to a private, loopback or link-local address. "
                "Set authorization.allow_private_targets when assessing an internal UAT host."
            )

    def allows(self, url: str) -> bool:
        try:
            self.check(url)
        except ScopeError:
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {"hosts": list(self.hosts), "allow_private": self.allow_private}


def is_private_target(host: str) -> bool:
    """True when the host is (or resolves to) a non-public address."""
    try:
        address = ipaddress.ip_address(host)
        return _is_private_address(address)
    except ValueError:
        pass
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        # Unresolvable: treat as non-private so the caller gets a connection
        # error rather than a confusing scope error.
        return False
    for info in infos:
        try:
            address = ipaddress.ip_address(info[4][0])
        except ValueError:
            continue
        if _is_private_address(address):
            return True
    return False


def _is_private_address(address: "ipaddress._BaseAddress") -> bool:
    return bool(
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_reserved
        or address.is_multicast
    )


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
