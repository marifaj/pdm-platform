"""The HTTP surfaces: JSON API and web UI, over the real WSGI application."""

from __future__ import annotations

import json

import pytest

from markna_server.execution import Worker
from markna_server.identity import ApiToken, Role, hash_api_token
from markna_server.storage import open_unit_of_work

from conftest import Client, make_user, principal_for


class TestPublicSurface:
    def test_health_needs_no_credentials(self, client: Client):
        status, payload = client.get_json("/api/v1/health")
        assert status == 200 and payload["status"] == "ok"

    def test_health_reveals_nothing_about_tenants(self, client: Client):
        _, payload = client.get_json("/api/v1/health")
        assert set(payload) == {"status", "service"}

    def test_api_rejects_anonymous_callers(self, client: Client):
        status, payload = client.get_json("/api/v1/projects")
        assert status == 401
        assert payload["error"]["code"] == "unauthenticated"

    def test_anonymous_browser_is_redirected_to_login(self, client: Client):
        status, headers, _ = client.get("/")
        assert status == 303 and headers["Location"] == "/login"

    def test_unknown_route_is_404(self, client: Client):
        assert client.get_json("/api/v1/nope")[0] == 404

    def test_wrong_method_is_405(self, client: Client):
        status, payload = client.delete_json("/api/v1/health")
        assert status == 405 and payload["error"]["code"] == "method_not_allowed"

    def test_every_response_carries_security_headers(self, client: Client):
        _, headers, _ = client.get("/api/v1/health")
        assert "default-src 'none'" in headers["Content-Security-Policy"]
        assert headers["X-Frame-Options"] == "DENY"
        assert headers["X-Content-Type-Options"] == "nosniff"
        assert headers["Cache-Control"] == "no-store"


class TestApiTokenAuthentication:
    def test_a_valid_token_identifies_its_owner(self, api_client: Client):
        status, payload = api_client.get_json("/api/v1/me")
        assert status == 200
        assert payload["email"] == "admin@example.com"
        assert payload["is_service"] is True

    def test_an_unknown_token_is_rejected(self, client: Client):
        status, _ = client.get_json(
            "/api/v1/me", headers={"authorization": "Bearer mkna_not-a-real-token"}
        )
        assert status == 401

    def test_a_revoked_token_stops_working(self, api_client: Client, services, admin):
        tokens = services.auth.list_api_tokens(admin)
        services.auth.revoke_api_token(admin, tokens[0].id)
        assert api_client.get_json("/api/v1/me")[0] == 401

    def test_the_plaintext_token_is_returned_exactly_once(self, api_client: Client):
        status, created = api_client.post_json("/api/v1/tokens", {"name": "ci"})
        assert status == 201 and created["token"].startswith("mkna_")
        _, listed = api_client.get_json("/api/v1/tokens")
        assert all("token" not in item for item in listed["items"])


class TestProjectApi:
    def test_create_and_read_back(self, api_client: Client):
        status, created = api_client.post_json(
            "/api/v1/projects",
            {"slug": "api-demo", "name": "API Demo", "source": {"local_path": "demo"}},
        )
        assert status == 201
        status, fetched = api_client.get_json(f"/api/v1/projects/{created['id']}")
        assert status == 200 and fetched["slug"] == "api-demo"
        assert fetched["source"]["local_path"].endswith("/demo")

    def test_duplicate_slug_conflicts(self, api_client: Client):
        api_client.post_json("/api/v1/projects", {"slug": "dup", "name": "One"})
        status, payload = api_client.post_json("/api/v1/projects", {"slug": "dup", "name": "Two"})
        assert status == 409 and payload["error"]["code"] == "conflict"

    def test_missing_field_is_a_400(self, api_client: Client):
        status, payload = api_client.post_json("/api/v1/projects", {"name": "No slug"})
        assert status == 400 and "slug" in payload["error"]["message"]

    def test_a_path_outside_the_workspace_is_rejected(self, api_client: Client):
        status, payload = api_client.post_json(
            "/api/v1/projects",
            {"slug": "escape", "name": "Escape", "source": {"local_path": "../../etc"}},
        )
        assert status == 422
        assert "workspace" in payload["error"]["message"]

    def test_the_request_body_cannot_choose_a_tenant(self, api_client: Client, services):
        other = services.organizations.bootstrap("other", "Other")
        status, created = api_client.post_json(
            "/api/v1/projects",
            {"slug": "hijack", "name": "Hijack", "organization_id": other.id},
        )
        assert status == 201
        assert created["organization_id"] != other.id


class TestRunApi:
    def test_queueing_returns_202_and_does_not_execute(self, api_client: Client, project):
        status, run = api_client.post_json(
            f"/api/v1/projects/{project.id}/runs", {"layers": ["architecture"]}
        )
        assert status == 202
        assert run["status"] == "queued"
        assert run["verdict"] is None
        assert run["started_at"] is None

    def test_a_worker_completes_the_queued_run(self, api_client: Client, project, config):
        _, run = api_client.post_json(
            f"/api/v1/projects/{project.id}/runs", {"layers": ["architecture"]}
        )
        executed = Worker(open_unit_of_work(config.database_path), config).drain()
        assert executed == 1

        status, detail = api_client.get_json(f"/api/v1/runs/{run['id']}")
        assert status == 200
        assert detail["status"] == "succeeded"
        assert detail["verdict"] == "BLOCK"
        assert detail["findings_total"] > 0
        assert detail["coverage"]
        assert detail["scanner_runs"]

    def test_findings_can_be_filtered(self, api_client: Client, project, config):
        _, run = api_client.post_json(
            f"/api/v1/projects/{project.id}/runs", {"layers": ["architecture"]}
        )
        Worker(open_unit_of_work(config.database_path), config).drain()

        _, everything = api_client.get_json(f"/api/v1/runs/{run['id']}/findings")
        _, critical = api_client.get_json(
            f"/api/v1/runs/{run['id']}/findings?severity=critical"
        )
        _, blocking = api_client.get_json(f"/api/v1/runs/{run['id']}/findings?blocking=1")
        assert 0 < critical["count"] < everything["count"]
        assert all(item["severity"] == "critical" for item in critical["items"])
        assert all(item["blocking"] for item in blocking["items"])

    def test_reports_are_served_in_their_own_content_type(self, api_client: Client, project, config):
        _, run = api_client.post_json(
            f"/api/v1/projects/{project.id}/runs", {"layers": ["architecture"]}
        )
        Worker(open_unit_of_work(config.database_path), config).drain()

        status, headers, body = api_client.get(f"/api/v1/runs/{run['id']}/reports/markdown")
        assert status == 200
        assert headers["Content-Type"].startswith("text/markdown")
        assert b"MARKNA Security Gate Report" in body

        status, _, raw = api_client.get(f"/api/v1/runs/{run['id']}/reports/json")
        assert json.loads(raw)["verdict"] == "BLOCK"

    def test_a_queued_run_can_be_cancelled(self, api_client: Client, project):
        _, run = api_client.post_json(f"/api/v1/projects/{project.id}/runs", {})
        assert api_client.post_json(f"/api/v1/runs/{run['id']}/cancel")[0] == 200
        _, detail = api_client.get_json(f"/api/v1/runs/{run['id']}")
        assert detail["status"] == "cancelled"

    def test_a_finished_run_cannot_be_cancelled(self, api_client: Client, project, config):
        _, run = api_client.post_json(f"/api/v1/projects/{project.id}/runs", {})
        Worker(open_unit_of_work(config.database_path), config).drain()
        assert api_client.post_json(f"/api/v1/runs/{run['id']}/cancel")[0] == 409

    def test_environment_layer_requires_a_target(self, api_client: Client, project):
        status, payload = api_client.post_json(
            f"/api/v1/projects/{project.id}/runs", {"layers": ["environment"]}
        )
        assert status == 422
        assert "environment" in payload["error"]["message"]


class TestPermissionsOverHttp:
    @pytest.fixture
    def viewer_client(self, app, services, organization) -> Client:
        """A token belonging to a viewer: same transport, fewer permissions."""
        viewer = make_user(services, organization, email="v@example.com", roles={Role.VIEWER})
        secret = "mkna_viewer-token-for-tests"
        services.uow.identity.create_api_token(
            ApiToken(
                organization_id=organization.id,
                user_id=viewer.id,
                name="viewer",
                token_hash=hash_api_token(secret),
            )
        )
        client = Client(app)
        client.headers["authorization"] = f"Bearer {secret}"
        return client

    def test_a_viewer_may_read(self, viewer_client: Client):
        assert viewer_client.get_json("/api/v1/projects")[0] == 200

    def test_a_viewer_may_not_create_a_project(self, viewer_client: Client):
        status, payload = viewer_client.post_json("/api/v1/projects", {"slug": "x", "name": "X"})
        assert status == 403 and payload["error"]["code"] == "permission_denied"

    def test_a_viewer_may_not_queue_a_run(self, viewer_client: Client, project):
        assert viewer_client.post_json(f"/api/v1/projects/{project.id}/runs", {})[0] == 403

    def test_a_viewer_may_mint_a_read_only_token_for_itself(self, viewer_client: Client):
        status, created = viewer_client.post_json(
            "/api/v1/tokens", {"name": "ci", "roles": ["viewer"]}
        )
        assert status == 201 and created["roles"] == ["viewer"]

    def test_a_viewer_may_not_mint_a_token_above_its_own_role(self, viewer_client: Client):
        status, payload = viewer_client.post_json(
            "/api/v1/tokens", {"name": "escalate", "roles": ["admin"]}
        )
        assert status == 403
        assert "issuer does not hold" in payload["error"]["message"]

    def test_a_viewer_only_sees_its_own_tokens(self, viewer_client: Client, api_client: Client):
        api_client.post_json("/api/v1/tokens", {"name": "admins-token"})
        viewer_client.post_json("/api/v1/tokens", {"name": "mine", "roles": ["viewer"]})

        _, listed = viewer_client.get_json("/api/v1/tokens")
        names = {item["name"] for item in listed["items"]}
        assert "mine" in names
        assert "admins-token" not in names, "a viewer must not see another user's tokens"

        _, all_tokens = api_client.get_json("/api/v1/tokens")
        assert {"mine", "admins-token"} <= {item["name"] for item in all_tokens["items"]}


class TestCrossTenantOverHttp:
    def test_another_tenants_project_is_404_not_403(self, api_client: Client, services):
        other = services.organizations.bootstrap("other", "Other")
        other_admin = principal_for(make_user(services, other, email="a@other"))
        hidden = services.projects.create(other_admin, slug="hidden", name="Hidden")

        status, payload = api_client.get_json(f"/api/v1/projects/{hidden.id}")
        assert status == 404
        assert payload["error"]["code"] == "not_found"

    def test_listing_never_leaks_across_tenants(self, api_client: Client, services, project):
        other = services.organizations.bootstrap("other", "Other")
        other_admin = principal_for(make_user(services, other, email="a@other"))
        services.projects.create(other_admin, slug="hidden", name="Hidden")

        _, payload = api_client.get_json("/api/v1/projects")
        assert [item["slug"] for item in payload["items"]] == [project.slug]


class TestWebUi:
    @pytest.fixture
    def signed_in(self, client: Client, services, organization) -> Client:
        make_user(
            services,
            organization,
            email="web@example.com",
            roles={Role.ADMIN},
            password="correct-horse-battery",
        )
        status, headers, _ = client.login("web@example.com", "correct-horse-battery")
        assert status == 303
        return client

    def test_login_sets_a_session_cookie(self, signed_in: Client):
        status, headers, _ = signed_in.get("/")
        assert status == 200
        assert "markna_session" in signed_in.cookies

    def test_bad_credentials_are_rejected_without_saying_which_part(self, client, services, organization):
        make_user(
            services, organization, email="web@example.com",
            roles={Role.VIEWER}, password="correct-horse-battery",
        )
        status, _, wrong_password = client.post_form(
            "/login", {"email": "web@example.com", "password": "nope-not-this"}
        )
        assert status == 401
        _, _, unknown_user = client.post_form(
            "/login", {"email": "nobody@example.com", "password": "nope-not-this"}
        )
        assert b"invalid email or password" in wrong_password
        assert b"invalid email or password" in unknown_user

    def test_pages_render(self, signed_in: Client, project):
        for path in ("/", "/projects", f"/projects/{project.id}", "/runs", "/policies"):
            status, headers, body = signed_in.get(path)
            assert status == 200, path
            assert b"MARKNA Security Gate" in body

    def test_a_form_post_without_a_csrf_token_is_refused(self, signed_in: Client):
        status, _, body = signed_in.post_form(
            "/projects", {"slug": "no-token", "name": "No Token"}
        )
        assert status == 403
        assert b"token did not match" in body

    def test_a_form_post_with_the_csrf_token_succeeds(self, signed_in: Client):
        token = signed_in.csrf()
        assert token
        status, headers, _ = signed_in.post_form(
            "/projects", {"slug": "with-token", "name": "With Token", "csrf_token": token}
        )
        assert status == 303
        assert "/projects/prj_" in headers["Location"]

    def test_the_run_page_shows_findings_after_the_worker_runs(
        self, signed_in: Client, project, config, services, admin
    ):
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        Worker(open_unit_of_work(config.database_path), config).drain()
        status, _, body = signed_in.get(f"/runs/{run.id}")
        assert status == 200
        assert b"BLOCK" in body
        assert b"Evidence coverage" in body
        assert b"arch-manifest" in body

    def test_logout_clears_the_session(self, signed_in: Client):
        token = signed_in.csrf()
        status, headers, _ = signed_in.post_form("/logout", {"csrf_token": token})
        assert status == 303 and headers["Location"] == "/login"
        assert signed_in.get("/")[0] == 303
