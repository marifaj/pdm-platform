"""Tenant isolation.

v1.0 provisions one organisation. These tests create several, because the point
is that nothing in the code assumes otherwise — the isolation has to be real
before it is needed, or it never will be.
"""

from __future__ import annotations

import pytest

from markna_server.domain import (
    Organization,
    PermissionDenied,
    Project,
    TenantIsolationError,
    TenantScope,
)
from markna_server.identity import AccessControl, Permission, Role
from markna_server.service import Services

from conftest import make_user, principal_for


@pytest.fixture
def two_organizations(services: Services, organization: Organization):
    """The single-organisation deployment, plus a second one that must stay invisible."""
    other = services.organizations.bootstrap("other", "Other Company")
    return organization, other


class TestScopeObject:
    def test_scope_requires_an_organization_id(self):
        with pytest.raises(ValueError):
            TenantScope("not-an-id")

    def test_scope_recognises_its_own_records(self, organization: Organization):
        project = Project(organization_id=organization.id, slug="a", name="A")
        assert organization.scope.owns(project)

    def test_scope_rejects_another_tenants_record(self, two_organizations):
        first, second = two_organizations
        project = Project(organization_id=second.id, slug="a", name="A")
        assert not first.scope.owns(project)


class TestRepositoryIsolation:
    def test_a_project_is_invisible_to_the_other_organization(self, services, two_organizations):
        first, second = two_organizations
        admin = principal_for(make_user(services, first, email="a@first"))
        project = services.projects.create(admin, slug="secret", name="Secret")

        assert services.uow.projects.get(second.scope, project.id) is None
        assert services.uow.projects.get_by_slug(second.scope, "secret") is None
        assert services.uow.projects.list(second.scope) == []

    def test_the_same_slug_may_exist_in_both_organizations(self, services, two_organizations):
        first, second = two_organizations
        first_admin = principal_for(make_user(services, first, email="a@first"))
        second_admin = principal_for(make_user(services, second, email="a@second"))
        services.projects.create(first_admin, slug="platform", name="Platform")
        services.projects.create(second_admin, slug="platform", name="Platform")
        assert len(services.uow.projects.list(first.scope)) == 1
        assert len(services.uow.projects.list(second.scope)) == 1

    def test_writing_into_another_organization_is_refused(self, services, two_organizations):
        first, second = two_organizations
        stray = Project(organization_id=first.id, slug="stray", name="Stray")
        with pytest.raises(TenantIsolationError):
            services.uow.projects.create(second.scope, stray)

    def test_runs_and_findings_are_scoped(self, services, two_organizations, monkeypatch):
        first, second = two_organizations
        admin = principal_for(make_user(services, first, email="a@first"))
        project = services.projects.create(
            admin, slug="demo", name="Demo", architecture_manifest="demo/architecture.yaml"
        )
        run = services.runs.enqueue(admin, project.id)
        assert services.uow.runs.get(second.scope, run.id) is None
        assert services.uow.runs.list(second.scope) == []
        assert services.uow.findings.list_for_run(second.scope, run.id) == []

    def test_audit_events_are_scoped(self, services, two_organizations):
        first, second = two_organizations
        admin = principal_for(make_user(services, first, email="a@first"))
        services.projects.create(admin, slug="demo", name="Demo")
        assert services.uow.audit.list(first.scope)
        assert services.uow.audit.list(second.scope) == []


class TestServiceIsolation:
    def test_a_principal_cannot_act_in_another_organization(self, services, two_organizations):
        first, second = two_organizations
        access = AccessControl()
        principal = principal_for(make_user(services, first, email="a@first"))
        with pytest.raises(TenantIsolationError):
            access.require(principal, Permission.PROJECT_READ, organization_id=second.id)

    def test_reading_another_tenants_project_by_id_reports_not_found(
        self, services, two_organizations
    ):
        first, second = two_organizations
        first_admin = principal_for(make_user(services, first, email="a@first"))
        second_admin = principal_for(make_user(services, second, email="a@second"))
        project = services.projects.create(first_admin, slug="demo", name="Demo")

        from markna_server.domain import NotFound

        with pytest.raises(NotFound):
            services.projects.get(second_admin, project.id)

    def test_isolation_failures_render_as_not_found(self):
        """A tenant probe must not be distinguishable from a missing record."""
        assert TenantIsolationError("x").status == 404
        assert TenantIsolationError("x").code == "not_found"


class TestRoles:
    def test_viewer_cannot_write(self, services, viewer):
        with pytest.raises(PermissionDenied):
            services.projects.create(viewer, slug="nope", name="Nope")

    def test_viewer_cannot_queue_a_run(self, services, admin, viewer):
        project = services.projects.create(
            admin, slug="demo", name="Demo", architecture_manifest="demo/architecture.yaml"
        )
        with pytest.raises(PermissionDenied):
            services.runs.enqueue(viewer, project.id)

    def test_maintainer_can_queue_and_mint_a_token_within_its_own_role(self, services, maintainer):
        project = services.projects.create(
            maintainer, slug="demo", name="Demo", architecture_manifest="demo/architecture.yaml"
        )
        assert services.runs.enqueue(maintainer, project.id)
        token, _ = services.auth.issue_api_token(maintainer, "ci", roles=["viewer"])
        assert token.roles == {Role.VIEWER}
        with pytest.raises(PermissionDenied):
            services.auth.issue_api_token(maintainer, "escalate", roles=["admin"])

    def test_admin_has_every_permission(self, admin):
        assert set(admin.permissions) == set(Permission)


class TestNoSingletonAssumptions:
    def test_many_projects_coexist_in_one_organization(self, services, admin):
        for index in range(5):
            services.projects.create(admin, slug=f"project-{index}", name=f"Project {index}")
        assert len(services.projects.list(admin)) == 5

    def test_many_runs_coexist_for_one_project(self, services, admin, project):
        runs = [services.runs.enqueue(admin, project.id, layers=["architecture"]) for _ in range(3)]
        assert len({run.id for run in runs}) == 3
        assert len(services.runs.list(admin, project_id=project.id)) == 3

    def test_organization_count_is_not_capped_by_the_domain(self, services):
        for index in range(3):
            services.organizations.bootstrap(f"org-{index}", f"Org {index}")
        assert services.organizations.count() >= 3
