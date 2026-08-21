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

        handlers: List[urllib.request.BaseHandler] = [
            urllib.request.HTTPSHandler(context=self._ssl_context())
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
