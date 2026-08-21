"""Shared fixtures for the server tests.

Every test gets its own database and workspace, so nothing leaks between them
and the tenant-isolation tests can create as many organisations as they like.
"""

from __future__ import annotations

import io
import json
import shutil
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pytest

from markna_server.app import Application
from markna_server.config import ServerConfig
from markna_server.domain import Organization
from markna_server.identity import Principal, Role, User, build_chain, hash_password
from markna_server.service import Services
from markna_server.storage import open_unit_of_work

EXAMPLE_PROJECT = Path(__file__).resolve().parent.parent / "examples" / "vulnerable-demo"


@pytest.fixture
def config(tmp_path: Path) -> ServerConfig:
    workspace = tmp_path / "workspaces"
    workspace.mkdir()
    shutil.copytree(EXAMPLE_PROJECT, workspace / "demo")
    return ServerConfig(
        database_path=str(tmp_path / "markna.db"),
        workspace_root=str(workspace),
        worker_workdir=str(tmp_path / "work"),
        secure_cookies=False,
        report_formats=["json", "markdown"],
        offline=True,
    )


@pytest.fixture
def services(config: ServerConfig) -> Services:
    return Services.build(open_unit_of_work(config.database_path, initialise=True), config)


@pytest.fixture
def organization(services: Services) -> Organization:
    return services.organizations.bootstrap("markna", "MARKNA Internal")


def make_user(
    services: Services,
    organization: Organization,
    *,
    email: str = "user@example.com",
    roles: Optional[set] = None,
    password: Optional[str] = None,
) -> User:
    user = User(
        organization_id=organization.id,
        email=email,
        display_name=email,
        roles=roles or {Role.ADMIN},
        password_hash=hash_password(password) if password else None,
    )
    return services.uow.identity.create_user(user)


def principal_for(user: User, method: str = "session") -> Principal:
    return user.to_principal(method)


@pytest.fixture
def admin(services: Services, organization: Organization) -> Principal:
    return principal_for(
        make_user(services, organization, email="admin@example.com", roles={Role.ADMIN})
    )


@pytest.fixture
def viewer(services: Services, organization: Organization) -> Principal:
    return principal_for(
        make_user(services, organization, email="viewer@example.com", roles={Role.VIEWER})
    )


@pytest.fixture
def maintainer(services: Services, organization: Organization) -> Principal:
    return principal_for(
        make_user(services, organization, email="maintainer@example.com", roles={Role.MAINTAINER})
    )


@pytest.fixture
def project(services: Services, admin: Principal):
    return services.projects.create(
        admin,
        slug="demo",
        name="Vulnerable Demo",
        source={"local_path": "demo"},
        architecture_manifest="demo/architecture.yaml",
        architecture_documents=["demo/docs/architecture.md"],
    )


class Client:
    """A minimal WSGI test client: cookies, JSON and form bodies."""

    def __init__(self, app: Application) -> None:
        self.app = app
        self.cookies: Dict[str, str] = {}
        self.headers: Dict[str, str] = {}

    def request(
        self,
        method: str,
        path: str,
        body: bytes = b"",
        *,
        content_type: str = "application/json",
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, Dict[str, str], bytes]:
        query = path.split("?", 1)[1] if "?" in path else ""
        environ: Dict[str, Any] = {
            "REQUEST_METHOD": method,
            "PATH_INFO": path.split("?", 1)[0],
            "QUERY_STRING": query,
            "wsgi.input": io.BytesIO(body),
            "CONTENT_LENGTH": str(len(body)),
            "CONTENT_TYPE": content_type,
            "wsgi.url_scheme": "http",
            "REMOTE_ADDR": "127.0.0.1",
        }
        if self.cookies:
            environ["HTTP_COOKIE"] = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
        for key, value in {**self.headers, **(headers or {})}.items():
            environ["HTTP_" + key.upper().replace("-", "_")] = value

        captured: Dict[str, Any] = {}

        def start_response(status: str, response_headers) -> None:
            captured["status"] = int(status.split()[0])
            captured["headers"] = response_headers

        payload = b"".join(self.app(environ, start_response))
        header_map: Dict[str, str] = {}
        for name, value in captured["headers"]:
            if name.lower() == "set-cookie":
                key, _, rest = value.partition("=")
                self.cookies[key] = rest.split(";")[0]
                header_map.setdefault("Set-Cookie", value)
            else:
                header_map[name] = value
        return captured["status"], header_map, payload

    # -- conveniences ------------------------------------------------------

    def get(self, path: str, **kwargs):
        return self.request("GET", path, **kwargs)

    def post_json(self, path: str, payload: Optional[dict] = None, **kwargs):
        body = json.dumps(payload or {}).encode("utf-8")
        status, headers, raw = self.request("POST", path, body, **kwargs)
        return status, _maybe_json(raw)

    def get_json(self, path: str, **kwargs):
        status, headers, raw = self.request("GET", path, **kwargs)
        return status, _maybe_json(raw)

    def patch_json(self, path: str, payload: dict, **kwargs):
        body = json.dumps(payload).encode("utf-8")
        status, headers, raw = self.request("PATCH", path, body, **kwargs)
        return status, _maybe_json(raw)

    def delete_json(self, path: str, **kwargs):
        status, headers, raw = self.request("DELETE", path, **kwargs)
        return status, _maybe_json(raw)

    def post_form(self, path: str, fields: Dict[str, str]):
        from urllib.parse import urlencode

        return self.request(
            "POST",
            path,
            urlencode(fields).encode("utf-8"),
            content_type="application/x-www-form-urlencoded",
        )

    def login(self, email: str, password: str):
        return self.post_form("/login", {"email": email, "password": password})

    def csrf(self) -> str:
        """Read the CSRF token out of a rendered page."""
        _, _, body = self.get("/projects")
        marker = b'name="csrf_token" value="'
        start = body.find(marker)
        if start == -1:
            return ""
        start += len(marker)
        return body[start : body.find(b'"', start)].decode()


def _maybe_json(raw: bytes) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return raw


@pytest.fixture
def app(services: Services, config: ServerConfig, organization: Organization) -> Application:
    from markna_server.storage import get_or_create_secret

    providers = build_chain(services.uow.identity, config.auth_providers, config.auth_options)
    return Application(
        services, providers=providers, secret_key=get_or_create_secret(services.uow.db)
    )


@pytest.fixture
def client(app: Application) -> Client:
    return Client(app)


@pytest.fixture
def api_client(app: Application, services: Services, admin: Principal) -> Client:
    token, secret = services.auth.issue_api_token(admin, "test")
    test_client = Client(app)
    test_client.headers["authorization"] = f"Bearer {secret}"
    return test_client
