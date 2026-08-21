"""Domain entities.

Six concepts, kept deliberately separate because conflating any two of them is
what makes an internal tool impossible to turn into a product later:

* **Organization** — the tenant boundary. Every other record belongs to exactly
  one. v1.0 provisions a single organisation, but nothing here assumes that.
* **Project** — one assessed system: its source, its policy, its environments.
  An organisation has many; nothing assumes there is only one.
* **AssessmentRun** — one execution of the gate against one project, at a point
  in time, under one policy version.
* **Finding** — one observation, belonging to a run, carrying the same schema
  the engine produces.
* **Policy** — the release rules. Versioned, resolvable at organisation or
  project level.
* **Report** — a rendered artefact of a run, in one format.

Severity, Layer and Verdict are imported from the scanner engine rather than
redefined: the engine's finding schema *is* the domain's finding schema, and two
copies of an enum is two chances to drift.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from markna.models import Layer, Location, Severity, Verdict, utc_now

from . import ids
from .errors import ValidationError

__all__ = [
    "AssessmentRun",
    "AuditEvent",
    "CoverageRecord",
    "EnvironmentKind",
    "EnvironmentTarget",
    "FindingRecord",
    "Layer",
    "Organization",
    "Policy",
    "Project",
    "ReportArtifact",
    "RunStatus",
    "RunTrigger",
    "ScannerRunRecord",
    "Severity",
    "SourceRepository",
    "TenantScope",
    "TriggerKind",
    "Verdict",
]


# --------------------------------------------------------------------- tenancy


@dataclass(frozen=True)
class TenantScope:
    """The organisation a unit of work is confined to.

    Every repository method takes one. A query that cannot be expressed within a
    scope is a query that should not exist: it is the mechanism that keeps a
    single-organisation deployment honest about being multi-tenant-shaped.
    """

    organization_id: str

    def __post_init__(self) -> None:
        ids.require_id(self.organization_id, ids.ORGANIZATION, "organization_id")

    def owns(self, entity: Any) -> bool:
        return getattr(entity, "organization_id", None) == self.organization_id


# ---------------------------------------------------------------- organisation


@dataclass
class Organization:
    """The tenant. v1.0 has one; the schema and the queries do not care."""

    id: str = field(default_factory=lambda: ids.new_id(ids.ORGANIZATION))
    slug: str = ""
    name: str = ""
    created_at: str = field(default_factory=utc_now)
    is_active: bool = True
    #: Free-form organisation settings. Deliberately not a column-per-setting:
    #: v1.0 needs almost none, and a JSON blob avoids a migration per idea.
    settings: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.slug = _validate_slug(self.slug, "organization.slug")
        if not self.name.strip():
            raise ValidationError("organization.name is required")

    @property
    def scope(self) -> TenantScope:
        return TenantScope(self.id)


# --------------------------------------------------------------------- project


class EnvironmentKind(str, enum.Enum):
    PRODUCTION = "production"
    UAT = "uat"
    DEMO = "demo"
    STAGING = "staging"
    DEVELOPMENT = "development"


@dataclass
class SourceRepository:
    """Where a project's source lives.

    ``local_path`` is what the worker actually scans in v1.0 — a path on the
    server that an operator has already provisioned. ``url``, ``provider`` and
    ``default_branch`` are recorded metadata so that adding a checkout step
    later is a worker change and not a schema change.
    """

    local_path: Optional[str] = None
    url: Optional[str] = None
    provider: Optional[str] = None          # github | gitlab | azure-devops | ...
    default_branch: str = "main"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "local_path": self.local_path,
            "url": self.url,
            "provider": self.provider,
            "default_branch": self.default_branch,
        }

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "SourceRepository":
        data = data or {}
        return cls(
            local_path=data.get("local_path"),
            url=data.get("url"),
            provider=data.get("provider"),
            default_branch=data.get("default_branch") or "main",
        )


@dataclass
class EnvironmentTarget:
    """A deployed environment a project may be assessed against.

    The authorisation fields are the same ones the engine requires before it
    will send a single request; storing them here means a run inherits a
    reviewed, recorded permission rather than someone typing one at the CLI.
    """

    id: str = field(default_factory=lambda: ids.new_id(ids.TARGET))
    organization_id: str = ""
    project_id: str = ""
    name: str = ""
    url: str = ""
    kind: EnvironmentKind = EnvironmentKind.UAT
    authorized_by: str = ""
    authorization_reference: Optional[str] = None
    authorization_expires: Optional[str] = None      # YYYY-MM-DD
    scope_hosts: List[str] = field(default_factory=list)
    allow_private_targets: bool = False
    created_at: str = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValidationError("target.name is required")
        if not self.url.startswith(("http://", "https://")):
            raise ValidationError("target.url must be an http(s) URL")
        if not self.authorized_by.strip():
            raise ValidationError(
                "target.authorized_by is required: record who permitted this environment "
                "to be probed before storing it"
            )

    def authorization_dict(self) -> Dict[str, Any]:
        """The shape :class:`markna.authorization.Authorization` expects."""
        return {
            "authorized_by": self.authorized_by,
            "reference": self.authorization_reference,
            "expires": self.authorization_expires,
            "scope_hosts": list(self.scope_hosts),
            "allow_private_targets": self.allow_private_targets,
        }


@dataclass
class Project:
    """One assessed system inside an organisation."""

    id: str = field(default_factory=lambda: ids.new_id(ids.PROJECT))
    organization_id: str = ""
    slug: str = ""
    name: str = ""
    description: str = ""
    source: SourceRepository = field(default_factory=SourceRepository)
    architecture_documents: List[str] = field(default_factory=list)
    architecture_manifest: Optional[str] = None
    default_policy_id: Optional[str] = None
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    archived_at: Optional[str] = None

    def __post_init__(self) -> None:
        ids.require_id(self.organization_id, ids.ORGANIZATION, "project.organization_id")
        self.slug = _validate_slug(self.slug, "project.slug")
        if not self.name.strip():
            raise ValidationError("project.name is required")

    @property
    def is_archived(self) -> bool:
        return self.archived_at is not None

    def supported_layers(self, targets: List[EnvironmentTarget]) -> List[Layer]:
        """Which layers this project currently has the inputs to assess."""
        layers: List[Layer] = []
        if self.architecture_documents or self.architecture_manifest:
            layers.append(Layer.ARCHITECTURE)
        if self.source.local_path:
            layers.append(Layer.CODE)
        if targets:
            layers.append(Layer.ENVIRONMENT)
        return layers


# ---------------------------------------------------------------------- policy


@dataclass
class Policy:
    """A versioned release policy.

    ``project_id`` is nullable on purpose. A policy with no project is the
    organisation default; a policy with one is that project's override. The
    resolution order lives in the service layer, not in the caller.
    """

    id: str = field(default_factory=lambda: ids.new_id(ids.POLICY))
    organization_id: str = ""
    project_id: Optional[str] = None
    name: str = ""
    version: int = 1
    document: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now)
    created_by: Optional[str] = None
    is_default: bool = False

    def __post_init__(self) -> None:
        ids.require_id(self.organization_id, ids.ORGANIZATION, "policy.organization_id")
        if not self.name.strip():
            raise ValidationError("policy.name is required")
        if not isinstance(self.document, dict):
            raise ValidationError("policy.document must be a mapping")

    @property
    def scope_label(self) -> str:
        return "project" if self.project_id else "organization"


# ------------------------------------------------------------------------ runs


class RunStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED)


class TriggerKind(str, enum.Enum):
    MANUAL = "manual"
    API = "api"
    SCHEDULE = "schedule"
    WEBHOOK = "webhook"


@dataclass
class RunTrigger:
    """Who or what asked for this run."""

    kind: TriggerKind = TriggerKind.MANUAL
    actor_id: Optional[str] = None
    actor_label: str = "unknown"

    def to_dict(self) -> Dict[str, Any]:
        return {"kind": self.kind.value, "actor_id": self.actor_id, "actor_label": self.actor_label}

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "RunTrigger":
        data = data or {}
        return cls(
            kind=TriggerKind(data.get("kind", "manual")),
            actor_id=data.get("actor_id"),
            actor_label=data.get("actor_label", "unknown"),
        )


@dataclass
class AssessmentRun:
    """One execution of the gate.

    The run row is also the work queue entry: ``status`` moves queued → running
    → terminal, and a worker claims it with a conditional update. That keeps the
    API process free of any scheduling logic and needs no broker.
    """

    id: str = field(default_factory=lambda: ids.new_id(ids.RUN))
    organization_id: str = ""
    project_id: str = ""
    policy_id: Optional[str] = None
    policy_name: str = ""
    target_id: Optional[str] = None
    status: RunStatus = RunStatus.QUEUED
    layers: List[Layer] = field(default_factory=list)
    trigger: RunTrigger = field(default_factory=RunTrigger)
    verdict: Optional[Verdict] = None
    summary: Dict[str, Any] = field(default_factory=dict)
    verdict_reasons: List[str] = field(default_factory=list)
    engine_assessment_id: Optional[str] = None
    git_commit: Optional[str] = None
    git_branch: Optional[str] = None
    environment_url: Optional[str] = None
    error: Optional[str] = None
    worker_id: Optional[str] = None
    created_at: str = field(default_factory=utc_now)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None

    def __post_init__(self) -> None:
        ids.require_id(self.organization_id, ids.ORGANIZATION, "run.organization_id")
        ids.require_id(self.project_id, ids.PROJECT, "run.project_id")
        if not self.layers:
            raise ValidationError("run.layers must name at least one layer")

    @property
    def duration_seconds(self) -> Optional[float]:
        if not (self.started_at and self.finished_at):
            return None
        from datetime import datetime

        fmt = "%Y-%m-%dT%H:%M:%SZ"
        try:
            start = datetime.strptime(self.started_at, fmt)
            finish = datetime.strptime(self.finished_at, fmt)
        except ValueError:
            return None
        return (finish - start).total_seconds()

    def blocking_count(self) -> int:
        return int(self.summary.get("blocking_findings", 0))


@dataclass
class FindingRecord:
    """A persisted engine finding, scoped to its run, project and organisation.

    ``engine_id`` is the fingerprint-derived id the engine assigned; it is kept
    so a report regenerated from storage matches the one the engine produced,
    and so a policy suppression written against it keeps working.
    """

    id: str = field(default_factory=lambda: ids.new_id(ids.FINDING))
    organization_id: str = ""
    project_id: str = ""
    run_id: str = ""
    engine_id: str = ""
    fingerprint: str = ""
    source: str = ""
    layer: Layer = Layer.CODE
    severity: Severity = Severity.INFO
    title: str = ""
    explanation: str = ""
    evidence: str = ""
    location: Location = field(default_factory=Location)
    remediation: str = ""
    blocking: bool = False
    detected_at: str = field(default_factory=utc_now)
    rule_id: Optional[str] = None
    references: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    cwe: List[str] = field(default_factory=list)
    confidence: str = "high"
    ai_generated: bool = False
    suppressed: bool = False
    suppression_reason: Optional[str] = None


@dataclass
class ScannerRunRecord:
    """Which scanner ran, what happened, and how long it took."""

    organization_id: str = ""
    run_id: str = ""
    name: str = ""
    layer: Layer = Layer.CODE
    status: str = "skipped"
    capabilities: List[str] = field(default_factory=list)
    deterministic: bool = True
    tool: Optional[str] = None
    tool_version: Optional[str] = None
    command: Optional[str] = None
    duration_seconds: float = 0.0
    findings_count: int = 0
    message: Optional[str] = None


@dataclass
class CoverageRecord:
    """Whether a policy-required capability was actually exercised."""

    organization_id: str = ""
    run_id: str = ""
    layer: Layer = Layer.CODE
    capability: str = ""
    required: bool = True
    satisfied: bool = False
    satisfied_by: List[str] = field(default_factory=list)


@dataclass
class ReportArtifact:
    """A rendered report for a run, in one format."""

    id: str = field(default_factory=lambda: ids.new_id(ids.REPORT))
    organization_id: str = ""
    project_id: str = ""
    run_id: str = ""
    format: str = "json"
    content_type: str = "application/json"
    size_bytes: int = 0
    content: str = ""
    created_at: str = field(default_factory=utc_now)


# ----------------------------------------------------------------------- audit


@dataclass
class AuditEvent:
    """An append-only record of who did what.

    Needed in v1.0 for the environment layer alone: probing a deployed system is
    an action that has to be attributable after the fact.
    """

    id: str = field(default_factory=lambda: ids.new_id(ids.AUDIT))
    organization_id: str = ""
    actor_id: Optional[str] = None
    actor_label: str = "system"
    action: str = ""
    subject_type: str = ""
    subject_id: str = ""
    detail: Dict[str, Any] = field(default_factory=dict)
    at: str = field(default_factory=utc_now)


# ------------------------------------------------------------------- utilities


def _validate_slug(value: str, field_name: str) -> str:
    slug = (value or "").strip().lower()
    if not slug:
        raise ValidationError(f"{field_name} is required")
    if not all(char.isalnum() or char == "-" for char in slug):
        raise ValidationError(f"{field_name} may contain only letters, digits and hyphens")
    if not (slug[0].isalnum() and slug[-1].isalnum()):
        raise ValidationError(f"{field_name} must start and end with a letter or digit")
    if len(slug) > 64:
        raise ValidationError(f"{field_name} must be 64 characters or fewer")
    return slug
