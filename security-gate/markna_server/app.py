"""The WSGI application.

Assembles the JSON API and the web UI over one set of services, with one
authentication step and one error contract. It is a plain WSGI callable:

    from markna_server.app import create_app
    application = create_app()

Run it under gunicorn, uWSGI, mod_wsgi, or the development server in
``markna-server serve``.

What this module deliberately does not do is execute scanners. It imports
neither :mod:`markna_server.execution` nor :mod:`markna.runner`; queueing a run
writes a row and returns.
"""

from __future__ import annotations

import logging
import traceback
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from .api import routes as api_routes
from .api.wsgi import (
    CSRF_HEADER,
    STATE_CHANGING_METHODS,
    Context,
    Request,
    Response,
    Router,
    apply_security_headers,
    csrf_valid,
    error_response,
    json_response,
    redirect,
    text_response,
)
from .config import ServerConfig
from .domain import DomainError
from .identity import SESSION_COOKIE, ProviderChain, SessionProvider, build_chain
from .service import Services
from .storage import get_or_create_secret, open_unit_of_work
from .web import views as web_views

LOGGER = logging.getLogger("markna.app")

_STATUS_TEXT = {
    200: "OK", 201: "Created", 202: "Accepted", 204: "No Content",
    302: "Found", 303: "See Other", 304: "Not Modified",
    400: "Bad Request", 401: "Unauthorized", 403: "Forbidden", 404: "Not Found",
    405: "Method Not Allowed", 409: "Conflict", 413: "Payload Too Large",
    415: "Unsupported Media Type", 422: "Unprocessable Entity",
    429: "Too Many Requests", 500: "Internal Server Error",
}


class Application:
    """The WSGI callable."""

    def __init__(
        self,
        services: Services,
        *,
        providers: ProviderChain,
        secret_key: bytes,
    ) -> None:
        self.services = services
        self.config = services.config
        self.providers = providers
        self.secret_key = secret_key
        self.router = Router()
        self.router.extend(api_routes.router)
        self.router.extend(web_views.router)
        self.public_paths = set(api_routes.PUBLIC_PATHS) | set(web_views.PUBLIC_PATHS)

    # ------------------------------------------------------------ wsgi entry

    def __call__(self, environ: Dict[str, Any], start_response: Callable) -> Iterable[bytes]:
        https = environ.get("wsgi.url_scheme") == "https" or (
            str(environ.get("HTTP_X_FORWARDED_PROTO", "")).lower() == "https"
        )
        # Request construction is inside the error boundary: a malformed
        # Content-Length is a 400, not an exception escaping the WSGI callable.
        try:
            request = Request.from_environ(
                environ, max_body_bytes=self.config.max_request_bytes
            )
        except DomainError as exc:
            response = self._parse_error(environ, exc)
        except Exception:  # noqa: BLE001 - malformed input must not crash the server
            LOGGER.error("could not parse request: %s", traceback.format_exc())
            response = json_response(
                {"error": {"code": "bad_request", "message": "the request could not be parsed"}},
                status=400,
            )
        else:
            response = self.handle(request)
        apply_security_headers(response, https=https)

        headers: List[Tuple[str, str]] = [
            ("Content-Type", response.content_type),
            ("Content-Length", str(len(response.body))),
            *response.headers,
        ]
        status_line = f"{response.status} {_STATUS_TEXT.get(response.status, 'Unknown')}"
        start_response(status_line, headers)
        return [response.body]

    # ------------------------------------------------------------- dispatch

    def handle(self, request: Request) -> Response:
        route, params, path_matched = self.router.match(request.method, request.path)
        if route is None:
            if path_matched:
                return self._error(request, 405, "method_not_allowed",
                                   f"{request.method} is not allowed on {request.path}")
            return self._error(request, 404, "not_found", f"no route for {request.path}")

        principal = self.providers.authenticate(request.auth_request())
        if principal is None and request.path not in self.public_paths:
            return self._challenge(request)

        csrf_failure = self._csrf_guard(request, principal)
        if csrf_failure is not None:
            return csrf_failure

        context = Context(
            request=request,
            services=self.services,
            params=params,
            principal=principal,
            secret_key=self.secret_key,
        )
        try:
            return route.handler(context)
        except DomainError as exc:
            return self._domain_error(context, exc)
        except Exception:  # noqa: BLE001 - one bad handler must not take the server down
            LOGGER.error("unhandled error in %s: %s", route.name, traceback.format_exc())
            return self._error(
                request, 500, "internal_error",
                "the request could not be completed; the failure has been logged",
            )

    def _csrf_guard(self, request: Request, principal) -> Optional[Response]:
        """Cookie-authenticated API writes must prove they are not cross-site.

        Token callers are exempt: a bearer token is not an ambient credential,
        so a third-party page cannot cause it to be sent. Session callers must
        present the session-bound CSRF token in a header, which a cross-origin
        form cannot set. SameSite=Lax already blocks the common case; this makes
        it a control rather than the only control.
        """
        if not request.path.startswith("/api/"):
            return None  # the web UI enforces its own per-form token
        if request.method not in STATE_CHANGING_METHODS or principal is None:
            return None
        if principal.auth_method != SessionProvider.name:
            return None
        session_id = request.cookies.get(SESSION_COOKIE, "")
        presented = request.headers.get(CSRF_HEADER, "")
        if csrf_valid(self.secret_key, session_id, presented):
            return None
        return json_response(
            {
                "error": {
                    "code": "csrf_failed",
                    "message": (
                        f"session-authenticated writes to the API must send the {CSRF_HEADER} "
                        "header; use an API token for non-browser clients"
                    ),
                }
            },
            status=403,
        )

    def _parse_error(self, environ: Dict[str, Any], exc: DomainError) -> Response:
        """Render a parse failure without a Request object to consult."""
        path = environ.get("PATH_INFO", "/") or "/"
        accept = str(environ.get("HTTP_ACCEPT", ""))
        if path.startswith("/api/") or "application/json" in accept:
            return json_response(
                {"error": {"code": exc.code, "message": exc.message}}, status=exc.status
            )
        return text_response(f"{exc.status} {exc.message}\n", status=exc.status)

    # --------------------------------------------------------------- errors

    def _challenge(self, request: Request) -> Response:
        """Ask an anonymous caller to authenticate, in the idiom it speaks."""
        if request.wants_json():
            challenge = self.providers.challenge(request.auth_request(), prefer="api-token")
            response = json_response(
                {"error": {"code": "unauthenticated", "message": "authentication required"}},
                status=401,
            )
            for name, value in challenge.headers.items():
                response.with_header(name, value)
            return response
        return redirect("/login")

    def _domain_error(self, context: Context, exc: DomainError) -> Response:
        if context.request.wants_json():
            return error_response(context.request, exc)
        return web_views.render_error(
            context, exc.status, _STATUS_TEXT.get(exc.status, "Error"), exc.message
        )

    def _error(self, request: Request, status: int, code: str, message: str) -> Response:
        if request.wants_json():
            return json_response({"error": {"code": code, "message": message}}, status=status)
        context = Context(request=request, services=self.services, secret_key=self.secret_key)
        return web_views.render_error(
            context, status, _STATUS_TEXT.get(status, "Error"), message
        )


# ------------------------------------------------------------------ factory


def create_app(
    config: Optional[ServerConfig] = None, *, config_path: Optional[str] = None
) -> Application:
    """Build the application from configuration.

    Called once per process. The unit of work holds a thread-local connection,
    so this is safe under a threaded WSGI server.
    """
    config = config or ServerConfig.load(config_path)
    uow = open_unit_of_work(config.database_path)
    services = Services.build(uow, config)
    providers = build_chain(uow.identity, config.auth_providers, config.auth_options)
    secret_key = get_or_create_secret(uow.db)
    LOGGER.info(
        "application ready: database=%s auth=%s workspace=%s",
        config.database_path,
        ",".join(providers.names),
        config.workspace_root,
    )
    return Application(services, providers=providers, secret_key=secret_key)


def application(environ: Dict[str, Any], start_response: Callable) -> Iterable[bytes]:
    """Module-level WSGI entry point for ``gunicorn markna_server.app:application``."""
    global _APPLICATION
    if _APPLICATION is None:
        _APPLICATION = create_app()
    return _APPLICATION(environ, start_response)


_APPLICATION: Optional[Application] = None
