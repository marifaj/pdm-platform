"""Application services: validation, policy resolution and the run lifecycle."""

from __future__ import annotations

import pytest

from markna_server.domain import (
    Conflict,
    NotFound,
    PermissionDenied,
    RunStatus,
    TriggerKind,
    ValidationError,
)
from markna.models import Layer

from conftest import make_user, principal_for


class TestProjectValidation:
    def test_slug_is_normalised_and_checked(self, services, admin):
        project = services.projects.create(admin, slug="Mixed-Case", name="Mixed")
        assert project.slug == "mixed-case"
        with pytest.raises(ValidationError):
            services.projects.create(admin, slug="not a slug", name="Bad")

    def test_paths_are_confined_to_the_workspace(self, services, admin):
        with pytest.raises(ValidationError, match="workspace"):
            services.projects.create(
                admin, slug="escape", name="Escape", source={"local_path": "../../etc/passwd"}
            )
        with pytest.raises(ValidationError, match="workspace"):
            services.projects.create(
                admin, slug="escape2", name="Escape", architecture_manifest="/etc/hosts"
            )

    def test_duplicate_slug_within_an_organization_conflicts(self, services, admin):
        services.projects.create(admin, slug="demo2", name="One")
        with pytest.raises(Conflict):
            services.projects.create(admin, slug="demo2", name="Two")

    def test_archived_projects_are_hidden_by_default(self, services, admin, project):
        services.projects.archive(admin, project.id)
        assert services.projects.list(admin) == []
        assert len(services.projects.list(admin, include_archived=True)) == 1


class TestTargets:
    def test_a_target_requires_a_recorded_authorisation(self, services, admin, project):
        with pytest.raises(ValidationError, match="authorized_by"):
            services.projects.add_target(
                admin, project.id, name="uat", url="https://uat.example.com", authorized_by=" "
            )

    def test_a_target_requires_an_http_url(self, services, admin, project):
        with pytest.raises(ValidationError, match="http"):
            services.projects.add_target(
                admin, project.id, name="uat", url="ssh://uat.example.com", authorized_by="me"
            )

    def test_adding_a_target_is_audited_with_who_authorised_it(self, services, admin, project):
        services.projects.add_target(
            admin,
            project.id,
            name="uat",
            url="https://uat.example.com",
            authorized_by="J. Smith",
            authorization_reference="CHG-1",
        )
        events = services.uow.audit.list(admin.scope)
        created = next(event for event in events if event.action == "target.create")
        assert created.detail["authorized_by"] == "J. Smith"
        assert created.actor_label == admin.display_name


class TestPolicyResolution:
    def test_falls_back_to_the_engine_default(self, services, admin, project):
        stored, engine_policy = services.policies.resolve_for_project(admin.scope, project)
        assert stored is None
        assert engine_policy.name == "markna-default-v1"

    def test_organization_default_is_used_when_no_project_policy_exists(
        self, services, admin, project
    ):
        services.policies.create_version(
            admin, name="org-standard", document={"block_on": ["critical"]}, is_default=True
        )
        stored, engine_policy = services.policies.resolve_for_project(admin.scope, project)
        assert stored is not None and stored.name == "org-standard"
        assert [severity.value for severity in engine_policy.block_on] == ["critical"]

    def test_a_project_policy_beats_the_organization_default(self, services, admin, project):
        services.policies.create_version(
            admin, name="org-standard", document={"block_on": ["critical"]}, is_default=True
        )
        services.policies.create_version(
            admin, name="project-strict", document={"block_on": ["critical", "high", "medium"]},
            project_id=project.id,
        )
        stored, engine_policy = services.policies.resolve_for_project(admin.scope, project)
        assert stored.name == "project-strict"
        assert len(engine_policy.block_on) == 3

    def test_versions_increment_and_are_immutable(self, services, admin):
        first = services.policies.create_version(admin, name="standard", document={})
        second = services.policies.create_version(
            admin, name="standard", document={"warn_on": ["low"]}
        )
        assert (first.version, second.version) == (1, 2)
        assert services.policies.get(admin, first.id).document == {}

    def test_an_invalid_policy_document_is_rejected(self, services, admin):
        with pytest.raises(ValidationError, match="invalid policy document"):
            services.policies.create_version(admin, name="broken", document={"blok_on": ["high"]})

    def test_a_run_records_the_policy_version_that_judged_it(self, services, admin, project):
        policy = services.policies.create_version(
            admin, name="pinned", document={"block_on": ["critical"]}, project_id=project.id
        )
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        assert run.policy_id == policy.id
        assert run.policy_name == "pinned"


class TestRunEnqueue:
    def test_defaults_to_every_layer_the_project_can_support(self, services, admin, project):
        run = services.runs.enqueue(admin, project.id)
        assert set(run.layers) == {Layer.ARCHITECTURE, Layer.CODE}

    def test_a_layer_without_inputs_is_refused(self, services, admin):
        bare = services.projects.create(admin, slug="bare", name="Bare",
                                        architecture_manifest="demo/architecture.yaml")
        with pytest.raises(ValidationError, match="code"):
            services.runs.enqueue(admin, bare.id, layers=["code"])

    def test_a_project_with_no_inputs_cannot_be_assessed(self, services, admin):
        empty = services.projects.create(admin, slug="empty", name="Empty")
        with pytest.raises(ValidationError, match="nothing to assess"):
            services.runs.enqueue(admin, empty.id)

    def test_an_archived_project_cannot_be_assessed(self, services, admin, project):
        services.projects.archive(admin, project.id)
        with pytest.raises(ValidationError, match="archived"):
            services.runs.enqueue(admin, project.id)

    def test_the_single_target_is_selected_automatically(self, services, admin, project):
        target = services.projects.add_target(
            admin, project.id, name="uat", url="https://uat.example.com", authorized_by="me"
        )
        run = services.runs.enqueue(admin, project.id, layers=["environment"])
        assert run.target_id == target.id
        assert run.environment_url == target.url

    def test_several_targets_require_an_explicit_choice(self, services, admin, project):
        for name in ("uat", "demo"):
            services.projects.add_target(
                admin, project.id, name=name, url=f"https://{name}.example.com", authorized_by="me"
            )
        with pytest.raises(ValidationError, match="target_id is required"):
            services.runs.enqueue(admin, project.id, layers=["environment"])

    def test_an_unknown_target_is_not_found(self, services, admin, project):
        services.projects.add_target(
            admin, project.id, name="uat", url="https://uat.example.com", authorized_by="me"
        )
        with pytest.raises(NotFound):
            services.runs.enqueue(
                admin, project.id, layers=["environment"], target_id="tgt_000000000000000000"
            )

    def test_the_trigger_records_who_asked(self, services, admin, project):
        run = services.runs.enqueue(
            admin, project.id, layers=["architecture"], trigger_kind=TriggerKind.API
        )
        assert run.trigger.kind is TriggerKind.API
        assert run.trigger.actor_id == admin.id

    def test_a_queued_run_has_done_no_work(self, services, admin, project):
        run = services.runs.enqueue(admin, project.id, layers=["architecture"])
        assert run.status is RunStatus.QUEUED
        assert run.verdict is None and run.started_at is None
        assert services.uow.findings.count_for_run(admin.scope, run.id) == 0


class TestAuthService:
    def test_login_rejects_a_wrong_password(self, services, organization):
        make_user(services, organization, email="p@example.com", password="correct-horse-battery")
        with pytest.raises(ValidationError, match="invalid email or password"):
            services.auth.login(organization.id, "p@example.com", "wrong-password-here")

    def test_login_rejects_an_unknown_user_with_the_same_message(self, services, organization):
        with pytest.raises(ValidationError, match="invalid email or password"):
            services.auth.login(organization.id, "nobody@example.com", "whatever-password")

    def test_login_creates_a_session_and_records_it(self, services, organization):
        make_user(services, organization, email="p@example.com", password="correct-horse-battery")
        user, session = services.auth.login(
            organization.id, "p@example.com", "correct-horse-battery"
        )
        assert services.uow.identity.get_session(session.id) is not None
        assert user.last_login_at is not None
        assert any(event.action == "auth.login" for event in services.uow.audit.list(user.to_principal("x").scope))

    def test_logout_destroys_the_session(self, services, organization):
        make_user(services, organization, email="p@example.com", password="correct-horse-battery")
        _, session = services.auth.login(organization.id, "p@example.com", "correct-horse-battery")
        services.auth.logout(session.id)
        assert services.uow.identity.get_session(session.id) is None

    def test_a_short_password_is_refused(self, services, admin):
        with pytest.raises(ValueError):
            services.auth.create_user(
                admin, email="short@example.com", display_name="Short",
                password="tooshort", roles=["viewer"],
            )

    def test_only_an_admin_may_create_users(self, services, maintainer):
        with pytest.raises(PermissionDenied):
            services.auth.create_user(
                maintainer, email="x@example.com", display_name="X",
                password=None, roles=["viewer"],
            )


class TestSummary:
    def test_summary_counts_only_the_callers_organization(self, services, admin, project):
        services.runs.enqueue(admin, project.id, layers=["architecture"])
        other = services.organizations.bootstrap("other", "Other")
        other_admin = principal_for(make_user(services, other, email="a@other"))
        other_project = services.projects.create(
            other_admin, slug="p", name="P", architecture_manifest="demo/architecture.yaml"
        )
        services.runs.enqueue(other_admin, other_project.id)

        assert services.runs.summary(admin)["runs"] == 1
        assert services.runs.summary(other_admin)["runs"] == 1
