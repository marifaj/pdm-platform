"""H1, H4, M2, M3 — authentication and API-token authorisation.

These are HTTP-level tests: they drive the WSGI application through the same
client the browser and CI would use, because the properties being asserted are
properties of the deployed surface, not of a service method.

H1  multi-organisation login without a singleton assumption
H4  API tokens carry explicit roles and never exceed their issuer
M2  an unknown address costs the same work as a known one
M3  guessing is bounded
"""

from __future__ import annotations

import time
from typing import Tuple

import pytest

from markna_server.app import Application
from markna_server.domain import Organization
from markna_server.identity import Principal, Role, build_chain
from markna_server.service import PermissionDenied, Services, TooManyAttempts, ValidationError
from markna_server.storage import get_or_create_secret

from conftest import Client, make_user, principal_for

PASSWORD = "correct-horse-battery-staple"


def second_org(services: Services) -> Organization:
    return services.organizations.bootstrap("contoso", "Contoso Ltd")


def app_for(services: Services, config) -> Application:
    providers = build_chain(services.uow.identity, config.auth_providers, config.auth_options)
    return Application(
        services, providers=providers, secret_key=get_or_create_secret(services.uow.db)
    )


@pytest.fixture
def two_orgs(services: Services, organization: Organization, config) -> Tuple[Application, Organization]:
    """Two tenants, each with its own admin, sharing one deployment."""
    other = second_org(services)
    make_user(services, organization, email="alice@markna.test", password=PASSWORD)
    make_user(services, other, email="bob@contoso.test", password=PASSWORD)
    return app_for(services, config), other


# --------------------------------------------------------------------- H1


class TestMultiOrganisationLogin:
    def test_a_user_in_the_first_organisation_can_sign_in(self, two_orgs):
        app, _ = two_orgs
        client = Client(app)
        status, headers, _ = client.login("alice@markna.test", PASSWORD)
        assert status == 303
        assert "Set-Cookie" in headers

    def test_a_user_in_the_second_organisation_can_sign_in(self, two_orgs):
        """The regression: with a singleton lookup this user did not exist."""
        app, _ = two_orgs
        client = Client(app)
        status, headers, _ = client.login("bob@contoso.test", PASSWORD)
        assert status == 303, "a second organisation's user must be able to authenticate"
        assert "Set-Cookie" in headers

    def test_each_session_lands_in_its_own_tenant(self, two_orgs, services, organization):
        app, other = two_orgs
        alice, bob = Client(app), Client(app)
        alice.login("alice@markna.test", PASSWORD)
        bob.login("bob@contoso.test", PASSWORD)
        status_a, projects_a = alice.get_json("/api/v1/projects")
        status_b, projects_b = bob.get_json("/api/v1/projects")
        assert status_a == 200 and status_b == 200
        assert projects_a != projects_b or projects_a["items"] == []

    def test_one_tenants_project_is_invisible_to_the_other(self, two_orgs, services, organization):
        app, other = two_orgs
        owner = principal_for(
            make_user(services, organization, email="owner@markna.test", roles={Role.ADMIN})
        )
        project = services.projects.create(
            owner, slug="secret", name="Secret", source={"local_path": "demo"}
        )
        bob = Client(app)
        bob.login("bob@contoso.test", PASSWORD)
        status, _ = bob.get_json(f"/api/v1/projects/{project.id}")
        assert status == 404, "a cross-tenant read must not even confirm existence"

    def test_the_same_address_in_two_tenants_is_refused_without_a_slug(
        self, services, organization, config
    ):
        other = second_org(services)
        make_user(services, organization, email="shared@example.com", password=PASSWORD)
        make_user(services, other, email="shared@example.com", password=PASSWORD)
        client = Client(app_for(services, config))
        status, _, body = client.login("shared@example.com", PASSWORD)
        assert status == 401
        assert b"organization" in body.lower()

    def test_the_slug_disambiguates_a_shared_address(self, services, organization, config):
        other = second_org(services)
        make_user(services, organization, email="shared@example.com", password=PASSWORD)
        make_user(services, other, email="shared@example.com", password=PASSWORD)
        client = Client(app_for(services, config))
        status, _, _ = client.post_form(
            "/login",
            {"email": "shared@example.com", "password": PASSWORD, "organization": "contoso"},
        )
        assert status == 303

    def test_a_wrong_slug_does_not_authenticate(self, two_orgs):
        app, _ = two_orgs
        client = Client(app)
        status, _, _ = client.post_form(
            "/login",
            {"email": "bob@contoso.test", "password": PASSWORD, "organization": "markna"},
        )
        assert status == 401

    def test_the_login_form_offers_the_organisation_field_only_when_needed(
        self, services, organization, config
    ):
        single = Client(app_for(services, config))
        _, _, body = single.get("/login")
        assert b'name="organization"' not in body
        second_org(services)
        multi = Client(app_for(services, config))
        _, _, body = multi.get("/login")
        assert b'name="organization"' in body


# --------------------------------------------------------------------- M2


class TestInvalidLoginPath:
    def test_an_unknown_address_and_a_wrong_password_read_identically(self, two_orgs):
        app, _ = two_orgs
        unknown = Client(app).login("nobody@nowhere.test", PASSWORD)
        wrong = Client(app).login("alice@markna.test", "not-the-password")
        assert unknown[0] == wrong[0] == 401
        # The page echoes back the address that was submitted; everything else,
        # including the error text, must be byte-identical.
        normalise = lambda body, email: body.replace(email.encode(), b"ADDRESS")
        assert normalise(unknown[2], "nobody@nowhere.test") == normalise(
            wrong[2], "alice@markna.test"
        ), "the response must not disclose whether the account exists"

    def test_an_unknown_address_still_costs_a_verification(self, services, organization):
        """A decoy hash is verified so the timing of the two paths matches."""
        known, unknown = [], []
        make_user(services, organization, email="timed@example.com", password=PASSWORD)
        for _ in range(3):
            for label, email in (("known", "timed@example.com"), ("unknown", "ghost@example.com")):
                services.uow.identity.clear_auth_failures(email)
                start = time.perf_counter()
                with pytest.raises(ValidationError):
                    services.auth.login(email, "wrong-password")
                (known if label == "known" else unknown).append(time.perf_counter() - start)
        # Not a timing-attack test — a smoke test that the unknown path is not
        # the near-zero early return it used to be.
        assert min(unknown) > min(known) / 4


# --------------------------------------------------------------------- M3


class TestLoginThrottling:
    def test_repeated_failures_are_eventually_refused(self, services, organization, config):
        make_user(services, organization, email="target@example.com", password=PASSWORD)
        client = Client(app_for(services, config))
        statuses = [
            client.login("target@example.com", f"guess-{n}")[0]
            for n in range(config.login_max_failures + 2)
        ]
        assert statuses[-1] == 429, "guessing must be bounded"
        assert 429 not in statuses[: config.login_max_failures]

    def test_the_throttle_survives_a_correct_password(self, services, organization, config):
        """Once locked, the right password does not unlock it inside the window."""
        make_user(services, organization, email="target@example.com", password=PASSWORD)
        client = Client(app_for(services, config))
        for n in range(config.login_max_failures + 1):
            client.login("target@example.com", f"guess-{n}")
        with pytest.raises(TooManyAttempts):
            services.auth.login("target@example.com", PASSWORD)

    def test_a_success_clears_the_counter(self, services, organization):
        make_user(services, organization, email="ok@example.com", password=PASSWORD)
        for n in range(2):
            with pytest.raises(ValidationError):
                services.auth.login("ok@example.com", f"guess-{n}")
        services.auth.login("ok@example.com", PASSWORD)
        since = 0.0
        assert services.uow.identity.count_recent_auth_failures("email:ok@example.com", since) == 0

    def test_throttling_cannot_be_configured_away(self, config):
        from markna_server.config import ServerConfig

        with pytest.raises(ValueError):
            ServerConfig(login_max_failures=0).validate()


# --------------------------------------------------------------------- H4


class TestApiTokenPrivilege:
    def test_a_token_defaults_to_its_issuers_roles(self, services, admin: Principal):
        token, _ = services.auth.issue_api_token(admin, "default")
        assert token.roles == set(admin.roles)

    def test_a_viewer_may_mint_a_read_only_token(self, services, viewer: Principal):
        """CI needs a token; CI does not need to be an administrator."""
        token, secret = services.auth.issue_api_token(viewer, "ci", roles=["viewer"])
        assert token.roles == {Role.VIEWER} and secret

    def test_a_viewer_may_not_mint_an_admin_token(self, services, viewer: Principal):
        with pytest.raises(PermissionDenied):
            services.auth.issue_api_token(viewer, "escalate", roles=["admin"])

    def test_a_maintainer_may_not_mint_an_admin_token(self, services, maintainer: Principal):
        with pytest.raises(PermissionDenied):
            services.auth.issue_api_token(maintainer, "escalate", roles=["admin"])

    def test_a_maintainer_may_mint_a_lesser_token(self, services, maintainer: Principal):
        token, _ = services.auth.issue_api_token(maintainer, "ci", roles=["viewer"])
        assert token.roles == {Role.VIEWER}

    def test_a_token_with_no_role_is_refused(self, services, admin: Principal):
        with pytest.raises(ValidationError):
            services.auth.issue_api_token(admin, "empty", roles=[])

    def test_an_unknown_role_is_refused(self, services, admin: Principal):
        with pytest.raises(ValidationError):
            services.auth.issue_api_token(admin, "bogus", roles=["superuser"])

    def test_a_read_only_token_cannot_write_over_http(self, app, services, admin: Principal):
        _, secret = services.auth.issue_api_token(admin, "ci", roles=["viewer"])
        client = Client(app)
        client.headers["authorization"] = f"Bearer {secret}"
        status, _ = client.post_json(
            "/api/v1/projects", {"slug": "new", "name": "New", "source": {"local_path": "demo"}}
        )
        assert status == 403, "a viewer-scoped token must not create projects"

    def test_a_read_only_token_can_still_read(self, app, services, admin: Principal, project):
        _, secret = services.auth.issue_api_token(admin, "ci", roles=["viewer"])
        client = Client(app)
        client.headers["authorization"] = f"Bearer {secret}"
        status, payload = client.get_json("/api/v1/projects")
        assert status == 200 and payload["items"]

    def test_a_maintainer_token_can_write(self, app, services, admin: Principal):
        _, secret = services.auth.issue_api_token(admin, "deploy", roles=["maintainer"])
        client = Client(app)
        client.headers["authorization"] = f"Bearer {secret}"
        status, _ = client.post_json(
            "/api/v1/projects", {"slug": "built", "name": "Built", "source": {"local_path": "demo"}}
        )
        assert status in (200, 201)

    def test_a_token_is_downgraded_when_its_owner_is(self, services, app, admin: Principal):
        """The token records roles, but the live user still bounds them."""
        _, secret = services.auth.issue_api_token(admin, "was-admin", roles=["admin"])
        user = services.uow.identity.get_user(admin.id)
        user.roles = {Role.VIEWER}
        services.uow.identity.update_user(user)
        client = Client(app)
        client.headers["authorization"] = f"Bearer {secret}"
        status, _ = client.post_json(
            "/api/v1/projects", {"slug": "x", "name": "X", "source": {"local_path": "demo"}}
        )
        assert status in (401, 403), "a demoted owner must not leave an admin token behind"

    def test_a_viewer_only_sees_its_own_tokens(self, services, admin: Principal, viewer: Principal):
        services.auth.issue_api_token(admin, "admin-token")
        services.auth.issue_api_token(viewer, "viewer-token", roles=["viewer"])
        viewer_names = {token.name for token in services.auth.list_api_tokens(viewer)}
        admin_names = {token.name for token in services.auth.list_api_tokens(admin)}
        assert "admin-token" not in viewer_names
        assert {"admin-token", "viewer-token"} <= admin_names

    def test_a_viewer_cannot_revoke_another_users_token(
        self, services, admin: Principal, viewer: Principal
    ):
        token, _ = services.auth.issue_api_token(admin, "admin-token")
        with pytest.raises(PermissionDenied):
            services.auth.revoke_api_token(viewer, token.id)

    def test_a_viewer_can_revoke_its_own_token(self, services, viewer: Principal):
        token, _ = services.auth.issue_api_token(viewer, "mine", roles=["viewer"])
        assert services.auth.revoke_api_token(viewer, token.id)

    def test_a_revoked_token_stops_working(self, app, services, admin: Principal):
        token, secret = services.auth.issue_api_token(admin, "temp")
        client = Client(app)
        client.headers["authorization"] = f"Bearer {secret}"
        assert client.get_json("/api/v1/projects")[0] == 200
        services.auth.revoke_api_token(admin, token.id)
        assert client.get_json("/api/v1/projects")[0] == 401
