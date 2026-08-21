"""Application services.

Everything a user can do goes through here, whether it arrived over the JSON API
or from the web UI. Two consequences worth stating explicitly:

* **Access control happens once.** Each method starts by asking
  :class:`~markna_server.identity.AccessControl` for a scope, and every
  repository call then uses that scope. A handler cannot forget.
* **Nothing here runs a scanner.** Queueing a run writes a row. The worker
  process picks it up. This module does not import the engine's runner, and a
  test asserts that it never starts to.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from markna.models import Layer, Verdict, utc_now
from markna.policy import DEFAULT_POLICY_NAME, Policy as EnginePolicy, PolicyError

from .config import ConfigError, ServerConfig
from .domain import (
    AssessmentRun,
    AuditEvent,
    Conflict,
    CoverageRecord,
    EnvironmentKind,
    EnvironmentTarget,
    FindingRecord,
    DomainError,
    NotFound,
    Organization,
    Policy,
    Project,
    ReportArtifact,
    RunStatus,
    PermissionDenied,
    RunTrigger,
    ScannerRunRecord,
    SourceRepository,
    TenantScope,
    TriggerKind,
    ValidationError,
)
from .identity import (
    AccessControl,
    ApiToken,
    Permission,
    Principal,
    Role,
    Session,
    User,
    expiry_from_now,
    generate_api_token,
    hash_api_token,
    permissions_for,
    hash_password,
    verify_password,
)
from .storage import UnitOfWork


class TooManyAttempts(DomainError):
    """Raised when credential guessing is being throttled."""

    status = 429
    code = "too_many_attempts"


_DECOY_HASH: Optional[str] = None


def _decoy_password_hash() -> str:
    """A real hash to verify against when no account matched.

    Computed once, then reused: the point is to spend the same CPU as a genuine
    verification, not to be secret.
    """
    global _DECOY_HASH
    if _DECOY_HASH is None:
        _DECOY_HASH = hash_password("markna-decoy-password-value")
    return _DECOY_HASH


def _window_start(seconds: int) -> str:
    moment = datetime.now(timezone.utc) - timedelta(seconds=max(1, seconds))
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_role_names(names: Sequence[str]) -> set:
    parsed = set()
    for name in names or []:
        try:
            parsed.add(Role(str(name).strip().lower()))
        except ValueError as exc:
            raise ValidationError(f"unknown role: {name!r}") from exc
    return parsed


@dataclass
class RunDetail:
    """Everything one run page or one API response needs, fetched together."""

    run: AssessmentRun
    project: Project
    findings: List[FindingRecord]
    scanner_runs: List[ScannerRunRecord]
    coverage: List[CoverageRecord]
    reports: List[ReportArtifact]
    total_findings: int


class _Service:
    def __init__(self, uow: UnitOfWork, config: ServerConfig, access: AccessControl) -> None:
        self.uow = uow
        self.config = config
        self.access = access

    def _audit(
        self,
        principal: Principal,
        action: str,
        subject_type: str,
        subject_id: str,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.uow.audit.record(
            AuditEvent(
                organization_id=principal.organization_id,
                actor_id=principal.id,
                actor_label=principal.display_name or principal.email,
                action=action,
                subject_type=subject_type,
                subject_id=subject_id,
                detail=detail or {},
            )
        )


# ------------------------------------------------------------- organisations


class OrganizationService(_Service):
    """Organisation lifecycle.

    v1.0 provisions exactly one and offers no UI to create more — that decision
    lives here, in one method, rather than being spread through the code as an
    assumption that only one can exist.
    """

    def bootstrap(self, slug: str, name: str) -> Organization:
        if self.uow.organizations.get_by_slug(slug):
            raise Conflict(f"an organization with slug {slug!r} already exists")
        organization = Organization(slug=slug, name=name)
        self.uow.organizations.create(organization)
        return organization

    def get(self, principal: Principal) -> Organization:
        self.access.require(principal, Permission.ORG_READ)
        organization = self.uow.organizations.get(principal.organization_id)
        if organization is None:
            raise NotFound("organization", principal.organization_id)
        return organization

    def count(self) -> int:
        return self.uow.organizations.count()


# ------------------------------------------------------------------ projects


class ProjectService(_Service):
    def list(self, principal: Principal, *, include_archived: bool = False) -> List[Project]:
        scope = self.access.require(principal, Permission.PROJECT_READ)
        return self.uow.projects.list(scope, include_archived=include_archived)

    def get(self, principal: Principal, project_id: str) -> Project:
        scope = self.access.require(principal, Permission.PROJECT_READ)
        project = self.uow.projects.get(scope, project_id)
        if project is None:
            raise NotFound("project", project_id)
        return project

    def get_by_slug(self, principal: Principal, slug: str) -> Project:
        scope = self.access.require(principal, Permission.PROJECT_READ)
        project = self.uow.projects.get_by_slug(scope, slug)
        if project is None:
            raise NotFound("project", slug)
        return project

    def create(
        self,
        principal: Principal,
        *,
        slug: str,
        name: str,
        description: str = "",
        source: Optional[Dict[str, Any]] = None,
        architecture_documents: Optional[Sequence[str]] = None,
        architecture_manifest: Optional[str] = None,
    ) -> Project:
        scope = self.access.require(principal, Permission.PROJECT_WRITE)
        if self.uow.projects.get_by_slug(scope, slug.lower()):
            raise Conflict(f"a project with slug {slug!r} already exists in this organization")

        repository = SourceRepository.from_dict(source or {})
        if repository.local_path:
            repository.local_path = self._checked_path(repository.local_path, "source.local_path")
        documents = [
            self._checked_path(document, "architecture_documents")
            for document in (architecture_documents or [])
        ]
        manifest = (
            self._checked_path(architecture_manifest, "architecture_manifest")
            if architecture_manifest
            else None
        )

        project = Project(
            organization_id=scope.organization_id,
            slug=slug,
            name=name,
            description=description,
            source=repository,
            architecture_documents=documents,
            architecture_manifest=manifest,
        )
        self.uow.projects.create(scope, project)
        self._audit(principal, "project.create", "project", project.id, {"slug": project.slug})
        return project

    def update(self, principal: Principal, project_id: str, changes: Dict[str, Any]) -> Project:
        scope = self.access.require(principal, Permission.PROJECT_WRITE)
        project = self.uow.projects.get(scope, project_id)
        if project is None:
            raise NotFound("project", project_id)

        if "name" in changes:
            project.name = str(changes["name"]).strip() or project.name
        if "description" in changes:
            project.description = str(changes["description"])
        if "source" in changes:
            repository = SourceRepository.from_dict(changes["source"] or {})
            if repository.local_path:
                repository.local_path = self._checked_path(
                    repository.local_path, "source.local_path"
                )
            project.source = repository
        if "architecture_documents" in changes:
            project.architecture_documents = [
                self._checked_path(document, "architecture_documents")
                for document in (changes["architecture_documents"] or [])
            ]
        if "architecture_manifest" in changes:
            manifest = changes["architecture_manifest"]
            project.architecture_manifest = (
                self._checked_path(manifest, "architecture_manifest") if manifest else None
            )
        self.uow.projects.update(scope, project)
        self._audit(principal, "project.update", "project", project.id, {"fields": sorted(changes)})
        return project

    def archive(self, principal: Principal, project_id: str) -> Project:
        scope = self.access.require(principal, Permission.PROJECT_WRITE)
        project = self.uow.projects.get(scope, project_id)
        if project is None:
            raise NotFound("project", project_id)
        project.archived_at = utc_now()
        self.uow.projects.update(scope, project)
        self._audit(principal, "project.archive", "project", project.id)
        return project

    # -- environment targets ----------------------------------------------

    def list_targets(self, principal: Principal, project_id: str) -> List[EnvironmentTarget]:
        scope = self.access.require(principal, Permission.PROJECT_READ)
        return self.uow.targets.list_for_project(scope, project_id)

    def add_target(
        self,
        principal: Principal,
        project_id: str,
        *,
        name: str,
        url: str,
        kind: str = "uat",
        authorized_by: str,
        authorization_reference: Optional[str] = None,
        authorization_expires: Optional[str] = None,
        scope_hosts: Optional[Sequence[str]] = None,
        allow_private_targets: bool = False,
    ) -> EnvironmentTarget:
        scope = self.access.require(principal, Permission.PROJECT_WRITE)
        if self.uow.projects.get(scope, project_id) is None:
            raise NotFound("project", project_id)
        try:
            environment_kind = EnvironmentKind(kind)
        except ValueError as exc:
            raise ValidationError(f"unknown environment kind {kind!r}") from exc

        target = EnvironmentTarget(
            organization_id=scope.organization_id,
            project_id=project_id,
            name=name,
            url=url,
            kind=environment_kind,
            authorized_by=authorized_by,
            authorization_reference=authorization_reference,
            authorization_expires=authorization_expires,
            scope_hosts=list(scope_hosts or []),
            allow_private_targets=allow_private_targets,
        )
        self.uow.targets.create(scope, target)
        # Recording who authorised a probe target is the point of the audit log.
        self._audit(
            principal,
            "target.create",
            "target",
            target.id,
            {"url": target.url, "authorized_by": target.authorized_by},
        )
        return target

    def delete_target(self, principal: Principal, target_id: str) -> bool:
        scope = self.access.require(principal, Permission.PROJECT_WRITE)
        deleted = self.uow.targets.delete(scope, target_id)
        if deleted:
            self._audit(principal, "target.delete", "target", target_id)
        return deleted

    # -- helpers -----------------------------------------------------------

    def _checked_path(self, candidate: str, field_name: str) -> str:
        try:
            resolved = self.config.resolve_in_workspace(candidate)
        except ConfigError as exc:
            raise ValidationError(f"{field_name}: {exc}") from exc
        return str(resolved)


# ------------------------------------------------------------------ policies


class PolicyService(_Service):
    """Policy versioning and resolution.

    Resolution order is project policy → organisation default → the engine's
    built-in default. Callers ask for the resolved policy; they never implement
    the fallback themselves.
    """

    def list(self, principal: Principal, *, project_id: Optional[str] = None) -> List[Policy]:
        scope = self.access.require(principal, Permission.POLICY_READ)
        return self.uow.policies.list(scope, project_id=project_id)

    def get(self, principal: Principal, policy_id: str) -> Policy:
        scope = self.access.require(principal, Permission.POLICY_READ)
        policy = self.uow.policies.get(scope, policy_id)
        if policy is None:
            raise NotFound("policy", policy_id)
        return policy

    def create_version(
        self,
        principal: Principal,
        *,
        name: str,
        document: Dict[str, Any],
        project_id: Optional[str] = None,
        is_default: bool = False,
    ) -> Policy:
        scope = self.access.require(principal, Permission.POLICY_WRITE)
        if project_id and self.uow.projects.get(scope, project_id) is None:
            raise NotFound("project", project_id)
        self._validate_document(document)

        policy = Policy(
            organization_id=scope.organization_id,
            project_id=project_id,
            name=name,
            version=self.uow.policies.next_version(scope, project_id, name),
            document=document,
            is_default=is_default and project_id is None,
            created_by=principal.id,
        )
        self.uow.policies.create(scope, policy)
        self._audit(
            principal,
            "policy.create",
            "policy",
            policy.id,
            {"name": policy.name, "version": policy.version, "scope": policy.scope_label},
        )
        return policy

    def resolve_for_project(self, scope: TenantScope, project: Project) -> Tuple[Optional[Policy], EnginePolicy]:
        """The policy a run of ``project`` would be judged by."""
        stored: Optional[Policy] = None
        if project.default_policy_id:
            stored = self.uow.policies.get(scope, project.default_policy_id)
        if stored is None:
            stored = self.uow.policies.latest_for_project(scope, project.id)
        if stored is None:
            stored = self.uow.policies.organization_default(scope)
        if stored is None:
            return None, EnginePolicy()
        return stored, EnginePolicy.from_dict(dict(stored.document))

    @staticmethod
    def _validate_document(document: Dict[str, Any]) -> None:
        if not isinstance(document, dict):
            raise ValidationError("policy document must be a mapping")
        try:
            EnginePolicy.from_dict(dict(document))
        except PolicyError as exc:
            raise ValidationError(f"invalid policy document: {exc}") from exc


# ---------------------------------------------------------------------- runs


class RunService(_Service):
    """Queue, list and inspect assessment runs.

    ``enqueue`` deliberately does no work beyond validation and a row insert:
    the boundary between the user-facing process and scanner execution is the
    ``queued`` status, not a function call.
    """

    def __init__(
        self,
        uow: UnitOfWork,
        config: ServerConfig,
        access: AccessControl,
        policies: PolicyService,
    ) -> None:
        super().__init__(uow, config, access)
        self.policies = policies

    def enqueue(
        self,
        principal: Principal,
        project_id: str,
        *,
        layers: Optional[Sequence[str]] = None,
        target_id: Optional[str] = None,
        trigger_kind: TriggerKind = TriggerKind.MANUAL,
    ) -> AssessmentRun:
        scope = self.access.require(principal, Permission.RUN_CREATE)
        project = self.uow.projects.get(scope, project_id)
        if project is None:
            raise NotFound("project", project_id)
        if project.is_archived:
            raise ValidationError("archived projects cannot be assessed")

        targets = self.uow.targets.list_for_project(scope, project.id)
        available = project.supported_layers(targets)
        requested = self._requested_layers(layers, available)

        target: Optional[EnvironmentTarget] = None
        if Layer.ENVIRONMENT in requested:
            target = self._resolve_target(scope, project, targets, target_id)

        stored_policy, engine_policy = self.policies.resolve_for_project(scope, project)
        run = AssessmentRun(
            organization_id=scope.organization_id,
            project_id=project.id,
            policy_id=stored_policy.id if stored_policy else None,
            # The name an operator recognises. policy_id pins the exact version;
            # the engine document's own `name` field is an internal detail.
            policy_name=(stored_policy.name if stored_policy else engine_policy.name)
            or DEFAULT_POLICY_NAME,
            target_id=target.id if target else None,
            status=RunStatus.QUEUED,
            layers=requested,
            trigger=RunTrigger(
                kind=trigger_kind,
                actor_id=principal.id,
                actor_label=principal.display_name or principal.email,
            ),
            environment_url=target.url if target else None,
        )
        self.uow.runs.create(scope, run)
        self._audit(
            principal,
            "run.enqueue",
            "run",
            run.id,
            {
                "project_id": project.id,
                "layers": [layer.value for layer in requested],
                "environment_url": run.environment_url,
            },
        )
        return run

    def list(
        self,
        principal: Principal,
        *,
        project_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[AssessmentRun]:
        scope = self.access.require(principal, Permission.RUN_READ)
        run_status = RunStatus(status) if status else None
        return self.uow.runs.list(
            scope, project_id=project_id, status=run_status, limit=limit, offset=offset
        )

    def get(self, principal: Principal, run_id: str) -> AssessmentRun:
        scope = self.access.require(principal, Permission.RUN_READ)
        run = self.uow.runs.get(scope, run_id)
        if run is None:
            raise NotFound("run", run_id)
        return run

    def detail(
        self,
        principal: Principal,
        run_id: str,
        *,
        severity: Optional[str] = None,
        layer: Optional[str] = None,
        blocking_only: bool = False,
        limit: int = 500,
    ) -> RunDetail:
        scope = self.access.require(principal, Permission.RUN_READ)
        run = self.uow.runs.get(scope, run_id)
        if run is None:
            raise NotFound("run", run_id)
        project = self.uow.projects.get(scope, run.project_id)
        if project is None:
            raise NotFound("project", run.project_id)
        return RunDetail(
            run=run,
            project=project,
            findings=self.uow.findings.list_for_run(
                scope,
                run_id,
                severity=severity,
                layer=layer,
                blocking_only=blocking_only,
                limit=limit,
            ),
            scanner_runs=self.uow.run_details.scanner_runs_for(scope, run_id),
            coverage=self.uow.run_details.coverage_for(scope, run_id),
            reports=self.uow.reports.list_for_run(scope, run_id),
            total_findings=self.uow.findings.count_for_run(scope, run_id),
        )

    def cancel(self, principal: Principal, run_id: str) -> bool:
        scope = self.access.require(principal, Permission.RUN_CANCEL)
        cancelled = self.uow.runs.cancel(scope, run_id)
        if cancelled:
            self._audit(principal, "run.cancel", "run", run_id)
        return cancelled

    def summary(self, principal: Principal) -> Dict[str, Any]:
        """Counts for the dashboard, all within the caller's organisation."""
        scope = self.access.require(principal, Permission.RUN_READ)
        projects = self.uow.projects.list(scope)
        recent = self.uow.runs.list(scope, limit=200)
        by_verdict: Dict[str, int] = {verdict.value: 0 for verdict in Verdict}
        by_status: Dict[str, int] = {status.value: 0 for status in RunStatus}
        for run in recent:
            by_status[run.status.value] += 1
            if run.verdict:
                by_verdict[run.verdict.value] += 1
        return {
            "projects": len(projects),
            "runs": len(recent),
            "by_status": by_status,
            "by_verdict": by_verdict,
            "blocking_findings": sum(run.blocking_count() for run in recent),
        }

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _requested_layers(layers: Optional[Sequence[str]], available: Sequence[Layer]) -> List[Layer]:
        if not available:
            raise ValidationError(
                "this project has nothing to assess: add an architecture manifest or document, "
                "a source path, or an environment target"
            )
        if not layers:
            return list(available)
        try:
            requested = [Layer(value) for value in layers]
        except ValueError as exc:
            raise ValidationError(f"unknown layer: {exc}") from exc
        missing = [layer for layer in requested if layer not in available]
        if missing:
            raise ValidationError(
                "the project has no input for layer(s): "
                + ", ".join(layer.value for layer in missing)
            )
        return requested

    @staticmethod
    def _resolve_target(
        scope: TenantScope,
        project: Project,
        targets: Sequence[EnvironmentTarget],
        target_id: Optional[str],
    ) -> EnvironmentTarget:
        if target_id:
            chosen = next((target for target in targets if target.id == target_id), None)
            if chosen is None:
                raise NotFound("target", target_id)
        elif len(targets) == 1:
            chosen = targets[0]
        else:
            raise ValidationError(
                "target_id is required: this project has "
                f"{len(targets)} environment targets"
            )
        return chosen


# -------------------------------------------------------------------- reports


class ReportService(_Service):
    def get(self, principal: Principal, run_id: str, fmt: str) -> ReportArtifact:
        scope = self.access.require(principal, Permission.REPORT_READ)
        report = self.uow.reports.get_by_format(scope, run_id, fmt)
        if report is None:
            raise NotFound("report", f"{run_id}/{fmt}")
        return report

    def list(self, principal: Principal, run_id: str) -> List[ReportArtifact]:
        scope = self.access.require(principal, Permission.REPORT_READ)
        return self.uow.reports.list_for_run(scope, run_id)


# ---------------------------------------------------------------------- auth


class AuthService(_Service):
    """Sessions, local accounts and API tokens.

    The provider chain decides *how* a caller is recognised; this service owns
    the credentials the built-in providers read. An external identity provider
    replaces ``login``, not the rest of the application.
    """

    def login(
        self,
        email: str,
        password: str,
        *,
        organization_slug: Optional[str] = None,
        user_agent: str = "",
        client_key: str = "",
    ) -> Tuple[User, Session]:
        """Authenticate by address, across every organisation in the deployment.

        Three properties this has to hold at once:

        * **No singleton assumption.** The caller supplies an address, not an
          organisation. Accounts are found across tenants and the matching one
          decides the scope.
        * **Constant work on failure.** An unknown address costs the same
          password verification as a known one, so response time does not
          disclose whether an account exists.
        * **Bounded guessing.** Failures are counted per address and per client;
          past the threshold the attempt is refused before any verification.
        """
        email = (email or "").strip().lower()
        self._enforce_login_throttle(email, client_key)

        candidates = self.uow.identity.find_users_by_email(email)
        if organization_slug:
            organization = self.uow.organizations.get_by_slug(organization_slug.strip().lower())
            candidates = [
                user for user in candidates
                if organization is not None and user.organization_id == organization.id
            ]
        usable = [user for user in candidates if user.is_active and user.password_hash]

        # Verify against every candidate, and against a decoy when there are
        # none, so the work done is the same either way.
        matches = [user for user in usable if verify_password(password, user.password_hash or "")]
        if not usable:
            verify_password(password, _decoy_password_hash())

        if not matches:
            self._record_login_failure(email, client_key)
            raise ValidationError("invalid email or password")
        if len(matches) > 1:
            # Only reachable by someone who already proved the password in more
            # than one organisation, so naming the ambiguity discloses nothing.
            raise ValidationError(
                "this address is valid in more than one organization; sign in with the "
                "organization slug as well"
            )

        user = matches[0]
        # Clear the per-address counter only. Clearing the per-client counter
        # would let an attacker who holds one valid account reset the budget it
        # is spending against every other address.
        self.uow.identity.clear_auth_failures(f"email:{email}")
        self.uow.identity.record_auth_attempt(f"email:{email}", successful=True)
        session = self.uow.identity.create_session(
            Session(
                organization_id=user.organization_id,
                user_id=user.id,
                expires_at=expiry_from_now(self.config.session_hours),
                user_agent=user_agent,
            )
        )
        user.last_login_at = utc_now()
        self.uow.identity.update_user(user)
        self.uow.audit.record(
            AuditEvent(
                organization_id=user.organization_id,
                actor_id=user.id,
                actor_label=user.email,
                action="auth.login",
                subject_type="user",
                subject_id=user.id,
                detail={"method": "local-password"},
            )
        )
        return user, session

    # -- throttling --------------------------------------------------------

    def _enforce_login_throttle(self, email: str, client_key: str) -> None:
        since = _window_start(self.config.login_window_seconds)
        limit = self.config.login_max_failures
        for key in filter(None, (f"email:{email}", f"client:{client_key}" if client_key else "")):
            if self.uow.identity.count_recent_auth_failures(key, since) >= limit:
                raise TooManyAttempts(
                    "too many failed sign-in attempts; wait "
                    f"{self.config.login_window_seconds // 60} minutes and try again"
                )

    def _record_login_failure(self, email: str, client_key: str) -> None:
        self.uow.identity.record_auth_attempt(f"email:{email}", successful=False)
        if client_key:
            self.uow.identity.record_auth_attempt(f"client:{client_key}", successful=False)

    def logout(self, session_id: str) -> None:
        self.uow.identity.delete_session(session_id)

    def create_user(
        self,
        principal: Principal,
        *,
        email: str,
        display_name: str,
        password: Optional[str],
        roles: Sequence[str],
    ) -> User:
        scope = self.access.require(principal, Permission.ORG_ADMIN)
        if self.uow.identity.find_user_by_email(scope.organization_id, email):
            raise Conflict(f"a user with email {email!r} already exists")
        user = User(
            organization_id=scope.organization_id,
            email=email.lower(),
            display_name=display_name,
            roles={Role(role) for role in roles} or {Role.VIEWER},
            password_hash=hash_password(password) if password else None,
        )
        self.uow.identity.create_user(user)
        self._audit(principal, "user.create", "user", user.id, {"email": user.email})
        return user

    def list_users(self, principal: Principal) -> List[User]:
        scope = self.access.require(principal, Permission.ORG_ADMIN)
        return self.uow.identity.list_users(scope)

    def issue_api_token(
        self,
        principal: Principal,
        name: str,
        *,
        roles: Optional[Sequence[str]] = None,
        expires_at: Optional[str] = None,
    ) -> Tuple[ApiToken, str]:
        """Create a token. The plaintext is returned once and never stored.

        A token is a credential that outlives the session that made it, so it
        gets its own explicit roles — and those can never exceed the issuer's.
        A viewer can mint a read-only token for CI; nobody can mint a token that
        is more powerful than themselves.
        """
        scope = self.access.require(principal, Permission.TOKEN_CREATE)
        requested = _parse_role_names(roles) if roles is not None else set(principal.roles)
        # Compared on permissions, not role names: a maintainer legitimately
        # mints a viewer token even though it does not hold the "viewer" label.
        excessive = permissions_for(requested) - principal.permissions
        if excessive:
            raise PermissionDenied(
                "a token cannot be granted privileges its issuer does not hold: "
                + ", ".join(sorted(permission.value for permission in excessive)),
                detail={"requested": sorted(role.value for role in requested)},
            )
        if not requested:
            raise ValidationError("a token needs at least one role")

        secret = generate_api_token()
        token = ApiToken(
            organization_id=scope.organization_id,
            user_id=principal.id,
            name=name,
            roles=requested,
            token_hash=hash_api_token(secret),
            expires_at=expires_at,
        )
        self.uow.identity.create_api_token(token)
        self._audit(
            principal,
            "token.create",
            "token",
            token.id,
            {"name": name, "roles": sorted(role.value for role in requested)},
        )
        return token, secret

    def revoke_api_token(self, principal: Principal, token_id: str) -> bool:
        scope = self.access.require(principal, Permission.TOKEN_CREATE)
        if not principal.has(Permission.TOKEN_MANAGE):
            owned = {token.id for token in self.uow.identity.list_api_tokens(scope)
                     if token.user_id == principal.id}
            if token_id not in owned:
                raise PermissionDenied("only an administrator may revoke another user's token")
        revoked = self.uow.identity.revoke_api_token(scope, token_id)
        if revoked:
            self._audit(principal, "token.revoke", "token", token_id)
        return revoked

    def list_api_tokens(self, principal: Principal) -> List[ApiToken]:
        """Administrators see the organisation's tokens; everyone else sees theirs."""
        scope = self.access.require(principal, Permission.TOKEN_CREATE)
        tokens = self.uow.identity.list_api_tokens(scope)
        if principal.has(Permission.TOKEN_MANAGE):
            return tokens
        return [token for token in tokens if token.user_id == principal.id]


# ------------------------------------------------------------------ container


@dataclass
class Services:
    """The service surface, constructed once per process."""

    uow: UnitOfWork
    config: ServerConfig
    access: AccessControl
    organizations: OrganizationService
    projects: ProjectService
    policies: PolicyService
    runs: RunService
    reports: ReportService
    auth: AuthService

    @classmethod
    def build(cls, uow: UnitOfWork, config: ServerConfig) -> "Services":
        access = AccessControl()
        policies = PolicyService(uow, config, access)
        return cls(
            uow=uow,
            config=config,
            access=access,
            organizations=OrganizationService(uow, config, access),
            projects=ProjectService(uow, config, access),
            policies=policies,
            runs=RunService(uow, config, access, policies),
            reports=ReportService(uow, config, access),
            auth=AuthService(uow, config, access),
        )
