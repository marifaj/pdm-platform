"""Entity to JSON.

Kept apart from the handlers so the API's wire format is one file to review, and
so the web UI can reuse the same shapes. Two rules:

* ``organization_id`` is echoed but never accepted — the caller's principal
  decides the tenant, never the request body.
* Report *content* is only serialised by the endpoint that serves one report;
  listings carry metadata, so a run page never ships four full reports.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

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
    ScannerRunRecord,
)
from ..identity import ApiToken, Principal, User


def organization(entity: Organization) -> Dict[str, Any]:
    return {
        "id": entity.id,
        "slug": entity.slug,
        "name": entity.name,
        "created_at": entity.created_at,
        "is_active": entity.is_active,
    }


def project(entity: Project, targets: Optional[Sequence[EnvironmentTarget]] = None) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "id": entity.id,
        "organization_id": entity.organization_id,
        "slug": entity.slug,
        "name": entity.name,
        "description": entity.description,
        "source": entity.source.to_dict(),
        "architecture_documents": list(entity.architecture_documents),
        "architecture_manifest": entity.architecture_manifest,
        "default_policy_id": entity.default_policy_id,
        "created_at": entity.created_at,
        "updated_at": entity.updated_at,
        "archived_at": entity.archived_at,
    }
    if targets is not None:
        data["targets"] = [target(item) for item in targets]
        data["assessable_layers"] = [
            layer.value for layer in entity.supported_layers(list(targets))
        ]
    return data


def target(entity: EnvironmentTarget) -> Dict[str, Any]:
    return {
        "id": entity.id,
        "organization_id": entity.organization_id,
        "project_id": entity.project_id,
        "name": entity.name,
        "url": entity.url,
        "kind": entity.kind.value,
        "authorization": {
            "authorized_by": entity.authorized_by,
            "reference": entity.authorization_reference,
            "expires": entity.authorization_expires,
            "scope_hosts": list(entity.scope_hosts),
            "allow_private_targets": entity.allow_private_targets,
        },
        "created_at": entity.created_at,
    }


def policy(entity: Policy, *, include_document: bool = True) -> Dict[str, Any]:
    data = {
        "id": entity.id,
        "organization_id": entity.organization_id,
        "project_id": entity.project_id,
        "scope": entity.scope_label,
        "name": entity.name,
        "version": entity.version,
        "is_default": entity.is_default,
        "created_at": entity.created_at,
        "created_by": entity.created_by,
    }
    if include_document:
        data["document"] = entity.document
    return data


def run(entity: AssessmentRun) -> Dict[str, Any]:
    return {
        "id": entity.id,
        "organization_id": entity.organization_id,
        "project_id": entity.project_id,
        "policy_id": entity.policy_id,
        "policy_name": entity.policy_name,
        "target_id": entity.target_id,
        "status": entity.status.value,
        "verdict": entity.verdict.value if entity.verdict else None,
        "layers": [layer.value for layer in entity.layers],
        "trigger": entity.trigger.to_dict(),
        "summary": entity.summary,
        "verdict_reasons": list(entity.verdict_reasons),
        "engine_assessment_id": entity.engine_assessment_id,
        "git_commit": entity.git_commit,
        "git_branch": entity.git_branch,
        "environment_url": entity.environment_url,
        "error": entity.error,
        "created_at": entity.created_at,
        "started_at": entity.started_at,
        "finished_at": entity.finished_at,
        "duration_seconds": entity.duration_seconds,
    }


def finding(entity: FindingRecord) -> Dict[str, Any]:
    return {
        "id": entity.id,
        "engine_id": entity.engine_id,
        "fingerprint": entity.fingerprint,
        "run_id": entity.run_id,
        "project_id": entity.project_id,
        "source": entity.source,
        "layer": entity.layer.value,
        "severity": entity.severity.value,
        "title": entity.title,
        "explanation": entity.explanation,
        "evidence": entity.evidence,
        "location": entity.location.to_dict(),
        "remediation": entity.remediation,
        "blocking": entity.blocking,
        "timestamp": entity.detected_at,
        "rule_id": entity.rule_id,
        "references": list(entity.references),
        "tags": list(entity.tags),
        "cwe": list(entity.cwe),
        "confidence": entity.confidence,
        "ai_generated": entity.ai_generated,
        "suppressed": entity.suppressed,
        "suppression_reason": entity.suppression_reason,
    }


def scanner_run(entity: ScannerRunRecord) -> Dict[str, Any]:
    return {
        "name": entity.name,
        "layer": entity.layer.value,
        "status": entity.status,
        "capabilities": list(entity.capabilities),
        "deterministic": entity.deterministic,
        "tool": entity.tool,
        "tool_version": entity.tool_version,
        "command": entity.command,
        "duration_seconds": entity.duration_seconds,
        "findings_count": entity.findings_count,
        "message": entity.message,
    }


def coverage(entity: CoverageRecord) -> Dict[str, Any]:
    return {
        "layer": entity.layer.value,
        "capability": entity.capability,
        "required": entity.required,
        "satisfied": entity.satisfied,
        "satisfied_by": list(entity.satisfied_by),
    }


def report(entity: ReportArtifact, *, include_content: bool = False) -> Dict[str, Any]:
    data = {
        "id": entity.id,
        "run_id": entity.run_id,
        "format": entity.format,
        "content_type": entity.content_type,
        "size_bytes": entity.size_bytes,
        "created_at": entity.created_at,
    }
    if include_content:
        data["content"] = entity.content
    return data


def api_token(entity: ApiToken) -> Dict[str, Any]:
    return {
        "id": entity.id,
        "name": entity.name,
        "user_id": entity.user_id,
        "created_at": entity.created_at,
        "expires_at": entity.expires_at,
        "last_used_at": entity.last_used_at,
        "revoked_at": entity.revoked_at,
        "is_usable": entity.is_usable,
    }


def user(entity: User) -> Dict[str, Any]:
    return {
        "id": entity.id,
        "organization_id": entity.organization_id,
        "email": entity.email,
        "display_name": entity.display_name,
        "roles": sorted(role.value for role in entity.roles),
        "is_active": entity.is_active,
        "subject": entity.subject,
        "issuer": entity.issuer,
        "created_at": entity.created_at,
        "last_login_at": entity.last_login_at,
    }


def principal(entity: Principal) -> Dict[str, Any]:
    return {**entity.to_dict(), "permissions": sorted(p.value for p in entity.permissions)}


def audit_event(entity: AuditEvent) -> Dict[str, Any]:
    return {
        "id": entity.id,
        "actor_label": entity.actor_label,
        "action": entity.action,
        "subject_type": entity.subject_type,
        "subject_id": entity.subject_id,
        "detail": entity.detail,
        "at": entity.at,
    }


def collection(items: Sequence[Any], serializer, **kwargs) -> Dict[str, Any]:
    rendered: List[Dict[str, Any]] = [serializer(item, **kwargs) for item in items]
    return {"count": len(rendered), "items": rendered}
