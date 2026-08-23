"""A small, scope-aware HTTP client for the environment layer.

Built on the standard library so the gate has no hard runtime dependency, and
deliberately restricted:

* only GET / HEAD / OPTIONS — never a state-changing verb, never a request body;
* every request (including every redirect hop) is checked against the authorised
  scope before it leaves the process;
* requests are rate-limited and identify themselves in the User-Agent, so the
  team running the UAT environment can see what hit their logs and why.
"""

from __future__ import annotations

import http.client
import socket
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from .authorization import Scope, ScopeError

USER_AGENT = "MARKNA-Security-Gate/1.0 (authorised pre-UAT security assessment; read-only)"
ALLOWED_METHODS = ("GET", "HEAD", "OPTIONS")
MAX_BODY_BYTES = 512 * 1024


@dataclass
class HttpResponse:
    url: str
    status: int
    reason: str
    headers: List[Tuple[str, str]] = field(default_factory=list)
    body: bytes = b""
    elapsed: float = 0.0
    redirect_chain: List[Tuple[int, str]] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None

    def header(self, name: str) -> Optional[str]:
        lowered = name.lower()
        values = [value for key, value in self.headers if key.lower() == lowered]
        return ", ".join(values) if values else None

    def header_values(self, name: str) -> List[str]:
        lowered = name.lower()
        return [value for key, value in self.headers if key.lower() == lowered]

    def header_map(self) -> Dict[str, str]:
        merged: Dict[str, str] = {}
        for key, value in self.headers:
            lowered = key.lower()
            merged[lowered] = f"{merged[lowered]}, {value}" if lowered in merged else value
        return merged

    def text(self, limit: int = 4000) -> str:
        return self.body.decode("utf-8", errors="replace")[:limit]

    def header_block(self) -> str:
        lines = [f"HTTP/1.1 {self.status} {self.reason}"]
        lines.extend(f"{key}: {value}" for key, value in self.headers)
        return "\n".join(lines)


def _guarded_socket(
    host: str,
    port: int,
    timeout: float,
    source_address,
    scope: Scope,
) -> socket.socket:
    """Resolve, vet every candidate address, then connect to a vetted one.

    This is the connect-time half of scope enforcement. Validating the hostname
    before the request and then letting the socket resolve the name again leaves
    a window in which the answer can change (DNS rebinding); connecting to an
    address that has *already* been checked closes it, because the address the
    kernel dials is the address that was approved.

    The port is re-checked here for the same reason the address is: this is the
    last point before a packet leaves the process, and it is the only check that
    sees the port the socket will genuinely dial rather than the one a URL
    claimed. Anything that reaches a socket without passing :meth:`Scope.check`
    -- a handler added later, a library redirect, a bug -- still stops here.
    """
    if not scope.port_permitted(port):
        raise ScopeError(
            f"port {port} on '{host}' is not authorised for this assessment "
            f"(authorised port(s): {', '.join(str(p) for p in scope.ports) or 'none'}). "
            "Add it to authorization.authorized_ports if testing it is permitted."
        )
    try:
        candidates = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise OSError(f"could not resolve {host}: {exc}") from exc

    last_error: Optional[Exception] = None
    refused: List[str] = []
    for family, socket_type, proto, _canonical, sockaddr in candidates:
        address = sockaddr[0]
        if not scope.address_allowed(address):
            refused.append(address)
            continue
        connection = socket.socket(family, socket_type, proto)
        try:
            if timeout is not None:
                connection.settimeout(timeout)
            if source_address:
                connection.bind(source_address)
            connection.connect(sockaddr)
            return connection
        except OSError as exc:
            connection.close()
            last_error = exc

    if refused and last_error is None:
        raise ScopeError(
            f"host '{host}' resolved only to out-of-scope address(es): {', '.join(refused)}. "
            "Set authorization.allow_private_targets when assessing an internal UAT host."
        )
    raise last_error or OSError(f"could not connect to {host}:{port}")


class _GuardedHTTPConnection(http.client.HTTPConnection):
    """An HTTP connection that only dials scope-approved addresses."""

    def __init__(self, *args, scope: Scope, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._scope = scope

    def connect(self) -> None:  # pragma: no cover - exercised through HttpClient
        self.sock = _guarded_socket(
            self.host, self.port, self.timeout, self.source_address, self._scope
        )
        if getattr(self, "_tunnel_host", None):
            self._tunnel()


class _GuardedHTTPSConnection(http.client.HTTPSConnection):
    """As above, with TLS wrapped over the vetted socket (SNI preserved)."""

    def __init__(self, *args, scope: Scope, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._scope = scope

    def connect(self) -> None:  # pragma: no cover - exercised through HttpClient
        sock = _guarded_socket(
            self.host, self.port, self.timeout, self.source_address, self._scope
        )
        if getattr(self, "_tunnel_host", None):
            self.sock = sock
            self._tunnel()
            sock = self.sock
        server_hostname = self._tunnel_host or self.host
        self.sock = self._context.wrap_socket(sock, server_hostname=server_hostname)


class _GuardedHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, scope: Scope) -> None:
        super().__init__()
        self._scope = scope

    def http_open(self, req):  # noqa: N802 - stdlib API
        return self.do_open(
            lambda *args, **kwargs: _GuardedHTTPConnection(*args, scope=self._scope, **kwargs),
            req,
        )


class _GuardedHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, scope: Scope, context: ssl.SSLContext) -> None:
        super().__init__(context=context)
        self._scope = scope
        self._ssl_context = context

    def https_open(self, req):  # noqa: N802 - stdlib API
        return self.do_open(
            lambda *args, **kwargs: _GuardedHTTPSConnection(
                *args, scope=self._scope, context=self._ssl_context, **kwargs
            ),
            req,
        )


class _ScopedRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Records the redirect chain and refuses out-of-scope hops."""

    def __init__(self, scope: Scope, chain: List[Tuple[int, str]]) -> None:
        super().__init__()
        self.scope = scope
        self.chain = chain

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: N802 - stdlib API
        target = urljoin(req.full_url, newurl)
        self.chain.append((code, target))
        self.scope.check(target)  # raises ScopeError, which propagates to the caller
        new_request = super().redirect_request(req, fp, code, msg, headers, target)
        if new_request is not None:
            new_request.get_method = lambda: req.get_method()  # never downgrade to GET silently
        return new_request


class HttpClient:
    """Read-only HTTP client bound to an authorised scope."""

    def __init__(
        self,
        scope: Scope,
        *,
        timeout: float = 20.0,
        rate_limit_seconds: float = 0.2,
        verify_tls: bool = True,
        max_redirects: int = 5,
    ) -> None:
        self.scope = scope
        self.timeout = timeout
        self.rate_limit_seconds = rate_limit_seconds
        self.verify_tls = verify_tls
        self.max_redirects = max_redirects
        self._last_request_at = 0.0
        self.request_log: List[Tuple[str, str, Optional[int]]] = []

    # ------------------------------------------------------------------ core

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        follow_redirects: bool = True,
        read_body: bool = True,
    ) -> HttpResponse:
        method = method.upper()
        if method not in ALLOWED_METHODS:
            raise ValueError(
                f"{method} is not permitted; MARKNA environment probes are read-only "
                f"({', '.join(ALLOWED_METHODS)})"
            )
        self.scope.check(url)
        self._respect_rate_limit()

        chain: List[Tuple[int, str]] = []
        request = urllib.request.Request(url, method=method)
        request.add_header("User-Agent", USER_AGENT)
        request.add_header("Accept", "*/*")
        for key, value in (headers or {}).items():
            request.add_header(key, value)

        # Both handlers vet the resolved address at connect time, so a
        # redirect, a rebinding answer or a multi-A record cannot reach a
        # destination the scope forbids.
        handlers: List[urllib.request.BaseHandler] = [
            _GuardedHTTPHandler(self.scope),
            _GuardedHTTPSHandler(self.scope, self._ssl_context()),
        ]
        if follow_redirects:
            handler = _ScopedRedirectHandler(self.scope, chain)
            handler.max_redirections = self.max_redirects
            handlers.append(handler)
        else:
            handlers.append(_NoRedirectHandler())
        opener = urllib.request.build_opener(*handlers)

        started = time.monotonic()
        try:
            with opener.open(request, timeout=self.timeout) as response:
                body = response.read(MAX_BODY_BYTES) if read_body else b""
                result = HttpResponse(
                    url=response.geturl(),
                    status=response.status,
                    reason=response.reason or "",
                    headers=list(response.headers.items()),
                    body=body,
                    elapsed=time.monotonic() - started,
                    redirect_chain=chain,
                )
        except urllib.error.HTTPError as exc:
            body = b""
            if read_body:
                try:
                    body = exc.read(MAX_BODY_BYTES)
                except Exception:  # pragma: no cover - defensive
                    body = b""
            result = HttpResponse(
                url=exc.geturl() or url,
                status=exc.code,
                reason=exc.reason or "",
                headers=list(exc.headers.items()) if exc.headers else [],
                body=body,
                elapsed=time.monotonic() - started,
                redirect_chain=chain,
            )
        except ScopeError:
            raise
        except (urllib.error.URLError, http.client.HTTPException, socket.timeout, OSError) as exc:
            reason = getattr(exc, "reason", exc)
            result = HttpResponse(
                url=url,
                status=0,
                reason="",
                elapsed=time.monotonic() - started,
                redirect_chain=chain,
                error=f"{type(exc).__name__}: {reason}",
            )

        self.request_log.append((method, url, result.status or None))
        return result

    def get(self, url: str, **kwargs) -> HttpResponse:
        return self.request("GET", url, **kwargs)

    def head(self, url: str, **kwargs) -> HttpResponse:
        return self.request("HEAD", url, **kwargs)

    def options(self, url: str, **kwargs) -> HttpResponse:
        return self.request("OPTIONS", url, **kwargs)

    # ------------------------------------------------------------- internals

    def _ssl_context(self) -> ssl.SSLContext:
        if self.verify_tls:
            return ssl.create_default_context()
        # Used only for the deliberate "does this endpoint present a broken
        # certificate" probe, which reports the failure as a finding.
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    def _respect_rate_limit(self) -> None:
        if self.rate_limit_seconds <= 0:
            return
        delta = time.monotonic() - self._last_request_at
        if delta < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - delta)
        self._last_request_at = time.monotonic()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: N802 - stdlib API
        return None


def base_url(url: str) -> str:
    """``https://host:port`` for a URL, without path or query."""
    parsed = urlparse(url)
    netloc = parsed.netloc
    return f"{parsed.scheme}://{netloc}"


def join_path(url: str, path: str) -> str:
    """Join a probe path onto the target's origin."""
    return urljoin(base_url(url) + "/", path.lstrip("/"))
