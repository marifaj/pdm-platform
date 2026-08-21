"""Repository protocols.

Two rules hold across every protocol here:

1. **Every method that touches tenant data takes a**
   :class:`~markna_server.domain.TenantScope` **first.** Not as a filter the
   caller may forget, but as the only way to express the query. A repository
   method with no scope parameter is either organisation-management or a bug.
2. **Repositories return domain entities, never rows.** Nothing above this layer
   knows the storage engine, so replacing SQLite with PostgreSQL — or splitting
   reports into object storage — is a new implementation of these protocols.
"""

from __future__ import annotations

from typing import List, Optional, Protocol, Sequence

from ..domain import (
    AssessmentRun,
    AuditEvent,
    CoverageRecord,
    EnvironmentTarget,
    FindingRecord,
    Organization,
    Policy,
    Project,
    ReportArtifact,
    RunStatus,
    ScannerRunRecord,
    TenantScope,
)
from ..identity import ApiToken, Session, User


class OrganizationRepository(Protocol):
    """The only repository not scoped to a tenant — it defines them."""

    def create(self, organization: Organization) -> Organization: ...

    def get(self, organization_id: str) -> Optional[Organization]: ...

    def get_by_slug(self, slug: str) -> Optional[Organization]: ...

    def list(self) -> List[Organization]: ...

    def count(self) -> int: ...


class ProjectRepository(Protocol):
    def create(self, scope: TenantScope, project: Project) -> Project: ...

    def update(self, scope: TenantScope, project: Project) -> Project: ...

    def get(self, scope: TenantScope, project_id: str) -> Optional[Project]: ...

    def get_by_slug(self, scope: TenantScope, slug: str) -> Optional[Project]: ...

    def list(self, scope: TenantScope, *, include_archived: bool = False) -> List[Project]: ...


class TargetRepository(Protocol):
    def create(self, scope: TenantScope, target: EnvironmentTarget) -> EnvironmentTarget: ...

    def get(self, scope: TenantScope, target_id: str) -> Optional[EnvironmentTarget]: ...

    def list_for_project(self, scope: TenantScope, project_id: str) -> List[EnvironmentTarget]: ...

    def delete(self, scope: TenantScope, target_id: str) -> bool: ...


class PolicyRepository(Protocol):
    def create(self, scope: TenantScope, policy: Policy) -> Policy: ...

    def get(self, scope: TenantScope, policy_id: str) -> Optional[Policy]: ...

    def list(self, scope: TenantScope, *, project_id: Optional[str] = None) -> List[Policy]: ...

    def latest_for_project(self, scope: TenantScope, project_id: str) -> Optional[Policy]: ...

    def organization_default(self, scope: TenantScope) -> Optional[Policy]: ...

    def next_version(self, scope: TenantScope, project_id: Optional[str], name: str) -> int: ...


class RunRepository(Protocol):
    def create(self, scope: TenantScope, run: AssessmentRun) -> AssessmentRun: ...

    def get(self, scope: TenantScope, run_id: str) -> Optional[AssessmentRun]: ...

    def list(
        self,
        scope: TenantScope,
        *,
        project_id: Optional[str] = None,
        status: Optional[RunStatus] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[AssessmentRun]: ...

    def update(self, scope: TenantScope, run: AssessmentRun) -> AssessmentRun: ...

    def cancel(self, scope: TenantScope, run_id: str) -> bool: ...

    # --- worker-facing ----------------------------------------------------
    # The worker is the one component that legitimately reads across tenants:
    # it is infrastructure, not a user. These two methods are the entire
    # cross-tenant surface, which is what makes it auditable.

    def claim_next_queued(self, worker_id: str) -> Optional[AssessmentRun]: ...

    def complete(self, run: AssessmentRun) -> AssessmentRun: ...


class FindingRepository(Protocol):
    def add_all(self, scope: TenantScope, findings: Sequence[FindingRecord]) -> int: ...

    def list_for_run(
        self,
        scope: TenantScope,
        run_id: str,
        *,
        severity: Optional[str] = None,
        layer: Optional[str] = None,
        blocking_only: bool = False,
        include_suppressed: bool = False,
        limit: int = 500,
        offset: int = 0,
    ) -> List[FindingRecord]: ...

    def count_for_run(self, scope: TenantScope, run_id: str) -> int: ...

    def get(self, scope: TenantScope, finding_id: str) -> Optional[FindingRecord]: ...


class RunDetailRepository(Protocol):
    """Scanner-execution and coverage records: the evidence that a run is complete."""

    def add_scanner_runs(self, scope: TenantScope, records: Sequence[ScannerRunRecord]) -> int: ...

    def add_coverage(self, scope: TenantScope, records: Sequence[CoverageRecord]) -> int: ...

    def scanner_runs_for(self, scope: TenantScope, run_id: str) -> List[ScannerRunRecord]: ...

    def coverage_for(self, scope: TenantScope, run_id: str) -> List[CoverageRecord]: ...


class ReportRepository(Protocol):
    def add(self, scope: TenantScope, report: ReportArtifact) -> ReportArtifact: ...

    def get(self, scope: TenantScope, report_id: str) -> Optional[ReportArtifact]: ...

    def get_by_format(self, scope: TenantScope, run_id: str, fmt: str) -> Optional[ReportArtifact]: ...

    def list_for_run(self, scope: TenantScope, run_id: str) -> List[ReportArtifact]: ...


class AuditRepository(Protocol):
    def record(self, event: AuditEvent) -> AuditEvent: ...

    def list(self, scope: TenantScope, *, limit: int = 100) -> List[AuditEvent]: ...


class IdentityRepository(Protocol):
    """User, token and session management, plus the read side authentication needs.

    Satisfies :class:`markna_server.identity.IdentityStore`; the wider surface
    here is the management side the admin CLI and API use.
    """

    def create_user(self, user: User) -> User: ...

    def update_user(self, user: User) -> User: ...

    def get_user(self, user_id: str) -> Optional[User]: ...

    def find_user_by_email(self, organization_id: str, email: str) -> Optional[User]: ...

    def find_user_by_subject(self, issuer: str, subject: str) -> Optional[User]: ...

    def list_users(self, scope: TenantScope) -> List[User]: ...

    def create_api_token(self, token: ApiToken) -> ApiToken: ...

    def find_api_token(self, token_hash: str) -> Optional[ApiToken]: ...

    def touch_api_token(self, token_id: str) -> None: ...

    def revoke_api_token(self, scope: TenantScope, token_id: str) -> bool: ...

    def list_api_tokens(self, scope: TenantScope) -> List[ApiToken]: ...

    def create_session(self, session: Session) -> Session: ...

    def get_session(self, session_id: str) -> Optional[Session]: ...

    def delete_session(self, session_id: str) -> None: ...

    def purge_expired_sessions(self) -> int: ...


class UnitOfWork(Protocol):
    """The set of repositories a request or job operates through."""

    organizations: OrganizationRepository
    projects: ProjectRepository
    targets: TargetRepository
    policies: PolicyRepository
    runs: RunRepository
    findings: FindingRepository
    run_details: RunDetailRepository
    reports: ReportRepository
    audit: AuditRepository
    identity: IdentityRepository

    def close(self) -> None: ...
