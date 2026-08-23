"""Authorisation and scope control for the environment layer.

The architecture and code layers read artefacts the operator already owns. The
environment layer sends traffic to a running system, which is only acceptable
against a target the operator is permitted to test. MARKNA refuses to run
environment probes without a recorded authorisation, and refuses to touch a host
outside the declared scope even if a redirect points there.

Scope has two dimensions, not one. A host grant is not a grant over every
service that happens to answer on that host: a UAT web application on 443 and a
container runtime API on 2375 are different systems with different owners and
different blast radii. An authorisation therefore names the ports it covers --
the target's own port, plus whatever ``authorization.authorized_ports`` records
-- and a redirect to another port on the same host is refused exactly as a
redirect to another host is.

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
    authorized_ports: List[int] = field(default_factory=list)
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
            authorized_ports=_port_list(data.get("authorized_ports")),
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
            "authorized_ports": list(self.authorized_ports),
            "allow_private_targets": self.allow_private_targets,
            "note": self.note,
        }


@dataclass
class Scope:
    """Host *and port* allow-list derived from the target URL plus the authorisation.

    Two independent dimensions. ``hosts`` says which names may be addressed;
    ``ports`` says which services on those names may be addressed. Both are
    checked for every URL, including every redirect hop, because a permitted
    host is not a permitted attack surface: the same name commonly carries an
    unauthenticated admin API, a database, a metrics endpoint or a container
    runtime socket on a port nobody authorised.
    """

    hosts: List[str]
    ports: List[int] = field(default_factory=list)
    allow_private: bool = False

    @classmethod
    def for_target(cls, url: str, authorization: Authorization) -> "Scope":
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ScopeError(f"target URL must be http(s), got {parsed.scheme or 'no scheme'}")
        if not parsed.hostname:
            raise ScopeError(f"target URL has no host: {url}")
        target_port = _target_port(parsed)
        if target_port is None:
            raise ScopeError(f"target URL has an unusable port: {url}")
        hosts = {parsed.hostname.lower()}
        hosts.update(authorization.scope_hosts)
        # The target's own port is authorised by the act of naming it as the
        # target. Everything else has to be written down.
        ports = {target_port}
        ports.update(authorization.authorized_ports)
        scope = cls(
            hosts=sorted(hosts),
            ports=sorted(ports),
            allow_private=authorization.allow_private_targets,
        )
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

    def port_permitted(self, port: Any) -> bool:
        """Is this port one the authorisation covers?

        Takes the port rather than the URL so the connect-time guard in
        :mod:`markna.http` can ask the same question about the port the socket
        is actually about to dial, without re-parsing anything.
        """
        try:
            number = int(port)
        except (TypeError, ValueError):
            return False
        return number in self.ports

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
        port = _target_port(parsed)
        if port is None or not self.port_permitted(port):
            # Deliberately not interpolating ``parsed.port`` here: for a URL like
            # ``http://h:notaport/`` reading it raises, which would turn a scope
            # refusal into an unhandled ValueError.
            raise ScopeError(
                f"port {port if port is not None else 'unreadable'} on '{host}' is not "
                f"authorised for this assessment (authorised port(s): "
                f"{', '.join(str(p) for p in self.ports) or 'none'}); a host grant does not "
                "authorise every service listening on that host. Add it to "
                "authorization.authorized_ports if testing it is permitted."
            )
        if not self.allow_private and is_private_target(host):
            raise ScopeError(
                f"host '{host}' resolves to a private, loopback or link-local address. "
                "Set authorization.allow_private_targets when assessing an internal UAT host."
            )

    def address_allowed(self, address: str) -> bool:
        """Is this *resolved IP* one the scope permits?

        Checked immediately before the socket connects, on the exact address
        the socket will use. The hostname check in :meth:`check` happens before
        name resolution and can therefore be defeated by a name that answers
        differently the second time it is resolved; this cannot.
        """
        try:
            parsed = ipaddress.ip_address(address)
        except ValueError:
            return False
        if self.allow_private:
            return True
        return not _is_private_address(parsed)

    def require_address(self, host: str, address: str) -> None:
        if not self.address_allowed(address):
            raise ScopeError(
                f"host '{host}' resolved to {address}, which is a private, loopback or "
                "otherwise non-public address. Set authorization.allow_private_targets when "
                "assessing an internal UAT host."
            )

    def allows(self, url: str) -> bool:
        try:
            self.check(url)
        except ScopeError:
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hosts": list(self.hosts),
            "ports": list(self.ports),
            "allow_private": self.allow_private,
        }


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


#: Ports assumed when a URL does not name one.
DEFAULT_PORTS = {"http": 80, "https": 443}


def _target_port(parsed) -> Optional[int]:
    """The port a URL will actually connect to: explicit, else scheme default.

    ``http://h/`` and ``http://h:80/`` are the same endpoint and must be treated
    as one, or an authorisation written in one spelling is silently absent in
    the other.
    """
    try:
        explicit = parsed.port
    except ValueError:
        # urlsplit raises rather than returning None for ``http://h:notaport/``.
        return None
    if explicit is not None:
        return explicit if 1 <= explicit <= 65535 else None
    return DEFAULT_PORTS.get(parsed.scheme)


def _port_list(value: Any) -> List[int]:
    """Parse ``authorization.authorized_ports``.

    A list of integers, and nothing else. A bare integer or a comma-separated
    string is rejected rather than guessed at: a misread authorisation that
    widens scope is worse than one that fails loudly.
    """
    if value is None:
        return []
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise AuthorizationError(
            "authorization.authorized_ports must be a list of port numbers, "
            f"got {type(value).__name__}"
        )
    ports: List[int] = []
    for entry in value:
        if isinstance(entry, bool):
            raise AuthorizationError(
                f"authorization.authorized_ports contains a non-port value: {entry!r}"
            )
        try:
            number = int(entry)
        except (TypeError, ValueError) as exc:
            raise AuthorizationError(
                f"authorization.authorized_ports contains a non-port value: {entry!r}"
            ) from exc
        if not 1 <= number <= 65535:
            raise AuthorizationError(
                f"authorization.authorized_ports contains an out-of-range port: {entry!r}"
            )
        if number not in ports:
            ports.append(number)
    return sorted(ports)


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
