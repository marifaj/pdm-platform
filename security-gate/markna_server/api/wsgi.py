"""A small WSGI core: requests, responses, routing and the error contract.

Deliberately framework-free. The application is a plain WSGI callable, so it
runs under gunicorn, uWSGI, mod_wsgi or the standard library's development
server, and adding a framework later means replacing this module rather than
unpicking one.

Every response carries the security headers this application checks for in
*other* systems. A security gate that ships without a Content-Security-Policy
would be difficult to take seriously.
"""

from __future__ import annotations

import hmac
import json
import re
from dataclasses import dataclass, field
from hashlib import sha256
from http.cookies import SimpleCookie
from typing import Any, Callable, Dict, Iterable, List, Optional, Pattern, Tuple
from urllib.parse import parse_qs

from ..domain import DomainError
from ..identity import AuthRequest, Principal

Handler = Callable[..., "Response"]

#: Default ceiling on a request body. Overridden from ServerConfig.
DEFAULT_MAX_BODY_BYTES = 1024 * 1024

#: Methods that change state and therefore need CSRF protection when the caller
#: is authenticated by an ambient credential (a cookie).
STATE_CHANGING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

#: Header a browser client must send with a session-authenticated API call.
CSRF_HEADER = "x-markna-csrf"

#: Locked down hard: the UI ships its own CSS inline and runs no JavaScript.
SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'none'; style-src 'unsafe-inline'; img-src 'self' data:; "
        "form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=(), interest-cohort=()",
    "Cache-Control": "no-store",
}


@dataclass
class Request:
    method: str
    path: str
    query: Dict[str, List[str]] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    remote_addr: str = ""
    principal: Optional[Principal] = None
    params: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_environ(
        cls, environ: Dict[str, Any], *, max_body_bytes: int = DEFAULT_MAX_BODY_BYTES
    ) -> "Request":
        """Build a request, refusing anything malformed.

        Raises :class:`BadRequest` or :class:`PayloadTooLarge`; the caller runs
        this inside the error boundary so a hostile Content-Length produces a
        clean 400 rather than an unhandled exception escaping the WSGI callable.
        """
        headers = {
            key[5:].replace("_", "-").lower(): value
            for key, value in environ.items()
            if key.startswith("HTTP_")
        }
        for key, header in (("CONTENT_TYPE", "content-type"), ("CONTENT_LENGTH", "content-length")):
            if environ.get(key):
                headers[header] = environ[key]

        body = cls._read_body(environ, max_body_bytes)

        cookies: Dict[str, str] = {}
        if headers.get("cookie"):
            jar = SimpleCookie()
            jar.load(headers["cookie"])
            cookies = {key: morsel.value for key, morsel in jar.items()}

        return cls(
            method=environ.get("REQUEST_METHOD", "GET").upper(),
            path=environ.get("PATH_INFO", "/") or "/",
            query=parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True),
            headers=headers,
            cookies=cookies,
            body=body,
            remote_addr=environ.get("REMOTE_ADDR", ""),
        )

    @staticmethod
    def _read_body(environ: Dict[str, Any], max_body_bytes: int) -> bytes:
        """Read at most ``max_body_bytes``, and only for a sane Content-Length.

        A negative or non-numeric length is a protocol error, not something to
        coerce: `read(-1)` would drain the socket and defeat the limit entirely.
        """
        raw_length = environ.get("CONTENT_LENGTH")
        if raw_length in (None, ""):
            return b""
        try:
            length = int(str(raw_length).strip())
        except (TypeError, ValueError):
            raise BadRequest(f"malformed Content-Length: {raw_length!r}")
        if length < 0:
            raise BadRequest(f"negative Content-Length: {length}")
        if length > max_body_bytes:
            raise PayloadTooLarge(
                f"request body of {length} bytes exceeds the {max_body_bytes}-byte limit"
            )
        stream = environ.get("wsgi.input")
        if stream is None:
            return b""
        # Read one byte past the ceiling so a lying Content-Length is caught too.
        body = stream.read(min(length, max_body_bytes) + 1)
        if len(body) > max_body_bytes:
            raise PayloadTooLarge(
                f"request body exceeded the {max_body_bytes}-byte limit"
            )
        return body

    # -- accessors ---------------------------------------------------------

    def get(self, name: str, default: str = "") -> str:
        values = self.query.get(name)
        return values[0] if values else default

    def get_int(self, name: str, default: int) -> int:
        try:
            return int(self.get(name, str(default)))
        except ValueError:
            return default

    def get_bool(self, name: str) -> bool:
        return self.get(name, "").lower() in ("1", "true", "yes", "on")

    def json(self, *, require_content_type: bool = True) -> Dict[str, Any]:
        """Parse a JSON body, requiring the caller to declare it as JSON.

        Requiring the content type is a CSRF control as much as a correctness
        one: a cross-site form post cannot set `application/json` without
        triggering a CORS preflight, so this closes the simple-request shape.
        """
        if require_content_type:
            declared = self.headers.get("content-type", "").split(";")[0].strip().lower()
            if declared and declared != "application/json":
                raise UnsupportedMediaType(
                    f"this endpoint requires Content-Type: application/json, got {declared!r}"
                )
            if not declared and self.body:
                raise UnsupportedMediaType(
                    "this endpoint requires Content-Type: application/json"
                )
        if not self.body:
            return {}
        try:
            data = json.loads(self.body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise BadRequest(f"request body is not valid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise BadRequest("request body must be a JSON object")
        return data

    def form(self) -> Dict[str, str]:
        if "application/x-www-form-urlencoded" not in self.headers.get("content-type", ""):
            return {}
        parsed = parse_qs(self.body.decode("utf-8", errors="replace"), keep_blank_values=True)
        return {key: values[0] for key, values in parsed.items()}

    def wants_json(self) -> bool:
        if self.path.startswith("/api/"):
            return True
        return "application/json" in self.headers.get("accept", "")

    def auth_request(self) -> AuthRequest:
        return AuthRequest(
            method=self.method,
            path=self.path,
            headers=dict(self.headers),
            cookies=dict(self.cookies),
            remote_addr=self.remote_addr,
        )


@dataclass
class Context:
    """What a handler is given.

    ``services`` is typed loosely so this module stays free of application
    imports: the WSGI core should be replaceable without touching the services,
    and vice versa.
    """

    request: Request
    services: Any
    params: Dict[str, str] = field(default_factory=dict)
    principal: Optional[Principal] = None
    secret_key: bytes = b""

    @property
    def user(self) -> Principal:
        """The authenticated principal, or fail loudly.

        Routes that need a principal are registered as authenticated, so this
        raising means a routing mistake, not an anonymous caller.
        """
        if self.principal is None:
            raise RuntimeError("this route requires authentication but none was resolved")
        return self.principal

    def param(self, name: str) -> str:
        return self.params[name]


class BadRequest(DomainError):
    status = 400
    code = "bad_request"


class PayloadTooLarge(DomainError):
    status = 413
    code = "payload_too_large"


class UnsupportedMediaType(DomainError):
    status = 415
    code = "unsupported_media_type"


class CsrfError(DomainError):
    status = 403
    code = "csrf_failed"


@dataclass
class Response:
    status: int = 200
    body: bytes = b""
    content_type: str = "text/plain; charset=utf-8"
    headers: List[Tuple[str, str]] = field(default_factory=list)

    def with_header(self, name: str, value: str) -> "Response":
        self.headers.append((name, value))
        return self

    def set_cookie(
        self,
        name: str,
        value: str,
        *,
        max_age: Optional[int] = None,
        secure: bool = True,
        http_only: bool = True,
        same_site: str = "Lax",
        path: str = "/",
    ) -> "Response":
        parts = [f"{name}={value}", f"Path={path}", f"SameSite={same_site}"]
        if max_age is not None:
            parts.append(f"Max-Age={max_age}")
        if http_only:
            parts.append("HttpOnly")
        if secure:
            parts.append("Secure")
        return self.with_header("Set-Cookie", "; ".join(parts))

    def clear_cookie(self, name: str, *, secure: bool = True) -> "Response":
        return self.set_cookie(name, "", max_age=0, secure=secure)


def json_response(data: Any, status: int = 200) -> Response:
    return Response(
        status=status,
        body=json.dumps(data, indent=2, default=str, ensure_ascii=False).encode("utf-8"),
        content_type="application/json",
    )


def html_response(markup: str, status: int = 200) -> Response:
    return Response(status=status, body=markup.encode("utf-8"), content_type="text/html; charset=utf-8")


def text_response(text: str, status: int = 200, content_type: str = "text/plain; charset=utf-8") -> Response:
    return Response(status=status, body=text.encode("utf-8"), content_type=content_type)


def redirect(location: str, status: int = 303) -> Response:
    return Response(status=status).with_header("Location", location)


def error_response(request: Request, exc: DomainError) -> Response:
    payload = {"error": {"code": exc.code, "message": exc.message, **exc.detail}}
    return json_response(payload, status=exc.status)


# ------------------------------------------------------------------- routing


class Route:
    """One method + path pattern. ``{name}`` captures a path segment."""

    def __init__(self, method: str, pattern: str, handler: Handler, *, name: str = "") -> None:
        self.method = method.upper()
        self.pattern = pattern
        self.handler = handler
        self.name = name or handler.__name__
        self.regex: Pattern[str] = re.compile(
            "^" + re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern) + "$"
        )


class Router:
    def __init__(self) -> None:
        self.routes: List[Route] = []

    def add(self, method: str, pattern: str, handler: Handler, *, name: str = "") -> None:
        self.routes.append(Route(method, pattern, handler, name=name))

    def get(self, pattern: str, **kwargs) -> Callable[[Handler], Handler]:
        return self._decorator("GET", pattern, **kwargs)

    def post(self, pattern: str, **kwargs) -> Callable[[Handler], Handler]:
        return self._decorator("POST", pattern, **kwargs)

    def patch(self, pattern: str, **kwargs) -> Callable[[Handler], Handler]:
        return self._decorator("PATCH", pattern, **kwargs)

    def delete(self, pattern: str, **kwargs) -> Callable[[Handler], Handler]:
        return self._decorator("DELETE", pattern, **kwargs)

    def _decorator(self, method: str, pattern: str, **kwargs) -> Callable[[Handler], Handler]:
        def register(handler: Handler) -> Handler:
            self.add(method, pattern, handler, **kwargs)
            return handler

        return register

    def match(self, method: str, path: str) -> Tuple[Optional[Route], Dict[str, str], bool]:
        """Return the route, its captures, and whether the path matched any method."""
        path_matched = False
        for route in self.routes:
            found = route.regex.match(path)
            if not found:
                continue
            path_matched = True
            if route.method == method.upper():
                return route, found.groupdict(), True
        return None, {}, path_matched

    def extend(self, other: "Router") -> None:
        self.routes.extend(other.routes)


# ------------------------------------------------------------------- helpers


def csrf_token(secret_key: bytes, session_id: str) -> str:
    """A session-bound CSRF token.

    Derived rather than stored: it survives a restart and works across several
    web processes without shared state, and it is worthless without the session
    cookie it is bound to.
    """
    return hmac.new(secret_key, session_id.encode("utf-8"), sha256).hexdigest()


def csrf_valid(secret_key: bytes, session_id: str, presented: str) -> bool:
    if not session_id or not presented:
        return False
    return hmac.compare_digest(csrf_token(secret_key, session_id), presented)


def apply_security_headers(response: Response, *, https: bool) -> Response:
    existing = {name.lower() for name, _ in response.headers}
    for name, value in SECURITY_HEADERS.items():
        if name.lower() not in existing:
            response.headers.append((name, value))
    if https and "strict-transport-security" not in existing:
        response.headers.append(
            ("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        )
    return response


def iter_body(response: Response) -> Iterable[bytes]:
    return [response.body]
