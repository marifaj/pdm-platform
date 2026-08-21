"""JSON API, version 1.

Every route is tenant-scoped through the caller's principal: no endpoint accepts
an ``organization_id``, and none can be made to return another organisation's
records by manipulating a path. Adding a second organisation later means adding
a way to *select* one, not rewriting these handlers.

Queueing a run returns 202 with the queued run. Nothing here executes a scanner.
"""

from __future__ import annotations

from typing import Any, Dict

from ..domain import TriggerKind
from . import serializers
from .wsgi import BadRequest, Context, Response, Router, json_response, text_response

router = Router()
PREFIX = "/api/v1"

#: Routes that answer without a principal. Everything else requires one.
PUBLIC_PATHS = {f"{PREFIX}/health"}


@router.get(f"{PREFIX}/health")
def health(ctx: Context) -> Response:
    """Liveness for a load balancer. Deliberately says nothing about tenants."""
    return json_response({"status": "ok", "service": "markna-security-gate"})


@router.get(f"{PREFIX}/me")
def whoami(ctx: Context) -> Response:
    return json_response(serializers.principal(ctx.user))


@router.get(f"{PREFIX}/organization")
def organization(ctx: Context) -> Response:
    return json_response(serializers.organization(ctx.services.organizations.get(ctx.user)))


@router.get(f"{PREFIX}/summary")
def summary(ctx: Context) -> Response:
    return json_response(ctx.services.runs.summary(ctx.user))


# ------------------------------------------------------------------ projects


@router.get(f"{PREFIX}/projects")
def list_projects(ctx: Context) -> Response:
    projects = ctx.services.projects.list(
        ctx.user, include_archived=ctx.request.get_bool("include_archived")
    )
    return json_response(serializers.collection(projects, serializers.project))


@router.post(f"{PREFIX}/projects")
def create_project(ctx: Context) -> Response:
    body = ctx.request.json()
    project = ctx.services.projects.create(
        ctx.user,
        slug=_required(body, "slug"),
        name=_required(body, "name"),
        description=body.get("description", ""),
        source=body.get("source"),
        architecture_documents=body.get("architecture_documents"),
        architecture_manifest=body.get("architecture_manifest"),
    )
    return json_response(serializers.project(project), status=201)


@router.get(f"{PREFIX}/projects/{{project_id}}")
def get_project(ctx: Context) -> Response:
    project = ctx.services.projects.get(ctx.user, ctx.param("project_id"))
    targets = ctx.services.projects.list_targets(ctx.user, project.id)
    return json_response(serializers.project(project, targets))


@router.patch(f"{PREFIX}/projects/{{project_id}}")
def update_project(ctx: Context) -> Response:
    project = ctx.services.projects.update(ctx.user, ctx.param("project_id"), ctx.request.json())
    return json_response(serializers.project(project))


@router.post(f"{PREFIX}/projects/{{project_id}}/archive")
def archive_project(ctx: Context) -> Response:
    project = ctx.services.projects.archive(ctx.user, ctx.param("project_id"))
    return json_response(serializers.project(project))


# ----------------------------------------------------------------- targets


@router.get(f"{PREFIX}/projects/{{project_id}}/targets")
def list_targets(ctx: Context) -> Response:
    targets = ctx.services.projects.list_targets(ctx.user, ctx.param("project_id"))
    return json_response(serializers.collection(targets, serializers.target))


@router.post(f"{PREFIX}/projects/{{project_id}}/targets")
def create_target(ctx: Context) -> Response:
    body = ctx.request.json()
    authorization = body.get("authorization") or {}
    target = ctx.services.projects.add_target(
        ctx.user,
        ctx.param("project_id"),
        name=_required(body, "name"),
        url=_required(body, "url"),
        kind=body.get("kind", "uat"),
        authorized_by=_required(authorization, "authorized_by", prefix="authorization."),
        authorization_reference=authorization.get("reference"),
        authorization_expires=authorization.get("expires"),
        scope_hosts=authorization.get("scope_hosts"),
        allow_private_targets=bool(authorization.get("allow_private_targets", False)),
    )
    return json_response(serializers.target(target), status=201)


@router.delete(f"{PREFIX}/targets/{{target_id}}")
def delete_target(ctx: Context) -> Response:
    deleted = ctx.services.projects.delete_target(ctx.user, ctx.param("target_id"))
    return json_response({"deleted": deleted}, status=200 if deleted else 404)


# ----------------------------------------------------------------- policies


@router.get(f"{PREFIX}/policies")
def list_policies(ctx: Context) -> Response:
    project_id = ctx.request.get("project_id") or None
    policies = ctx.services.policies.list(ctx.user, project_id=project_id)
    return json_response(
        serializers.collection(policies, serializers.policy, include_document=False)
    )


@router.post(f"{PREFIX}/policies")
def create_policy(ctx: Context) -> Response:
    body = ctx.request.json()
    policy = ctx.services.policies.create_version(
        ctx.user,
        name=_required(body, "name"),
        document=body.get("document") or {},
        project_id=body.get("project_id"),
        is_default=bool(body.get("is_default", False)),
    )
    return json_response(serializers.policy(policy), status=201)


@router.get(f"{PREFIX}/policies/{{policy_id}}")
def get_policy(ctx: Context) -> Response:
    return json_response(serializers.policy(ctx.services.policies.get(ctx.user, ctx.param("policy_id"))))


# --------------------------------------------------------------------- runs


@router.post(f"{PREFIX}/projects/{{project_id}}/runs")
def enqueue_run(ctx: Context) -> Response:
    body = ctx.request.json()
    run = ctx.services.runs.enqueue(
        ctx.user,
        ctx.param("project_id"),
        layers=body.get("layers"),
        target_id=body.get("target_id"),
        trigger_kind=TriggerKind.API,
    )
    # 202: accepted for execution by a worker, not executed here.
    return json_response(serializers.run(run), status=202)


@router.get(f"{PREFIX}/runs")
def list_runs(ctx: Context) -> Response:
    runs = ctx.services.runs.list(
        ctx.user,
        project_id=ctx.request.get("project_id") or None,
        status=ctx.request.get("status") or None,
        limit=ctx.request.get_int("limit", 50),
        offset=ctx.request.get_int("offset", 0),
    )
    return json_response(serializers.collection(runs, serializers.run))


@router.get(f"{PREFIX}/runs/{{run_id}}")
def get_run(ctx: Context) -> Response:
    detail = ctx.services.runs.detail(ctx.user, ctx.param("run_id"), limit=0 or 500)
    payload: Dict[str, Any] = serializers.run(detail.run)
    payload["project"] = {"id": detail.project.id, "slug": detail.project.slug, "name": detail.project.name}
    payload["coverage"] = [serializers.coverage(entry) for entry in detail.coverage]
    payload["scanner_runs"] = [serializers.scanner_run(entry) for entry in detail.scanner_runs]
    payload["reports"] = [serializers.report(entry) for entry in detail.reports]
    payload["findings_total"] = detail.total_findings
    return json_response(payload)


@router.post(f"{PREFIX}/runs/{{run_id}}/cancel")
def cancel_run(ctx: Context) -> Response:
    cancelled = ctx.services.runs.cancel(ctx.user, ctx.param("run_id"))
    if not cancelled:
        return json_response(
            {"error": {"code": "conflict", "message": "only a queued run can be cancelled"}},
            status=409,
        )
    return json_response({"cancelled": True})


@router.get(f"{PREFIX}/runs/{{run_id}}/findings")
def list_findings(ctx: Context) -> Response:
    detail = ctx.services.runs.detail(
        ctx.user,
        ctx.param("run_id"),
        severity=ctx.request.get("severity") or None,
        layer=ctx.request.get("layer") or None,
        blocking_only=ctx.request.get_bool("blocking"),
        limit=ctx.request.get_int("limit", 500),
    )
    payload = serializers.collection(detail.findings, serializers.finding)
    payload["total_unsuppressed"] = detail.total_findings
    return json_response(payload)


# ------------------------------------------------------------------ reports


@router.get(f"{PREFIX}/runs/{{run_id}}/reports")
def list_reports(ctx: Context) -> Response:
    reports = ctx.services.reports.list(ctx.user, ctx.param("run_id"))
    return json_response(serializers.collection(reports, serializers.report))


@router.get(f"{PREFIX}/runs/{{run_id}}/reports/{{fmt}}")
def get_report(ctx: Context) -> Response:
    report = ctx.services.reports.get(ctx.user, ctx.param("run_id"), ctx.param("fmt"))
    # Served as the report's own content type so `curl -O` produces a usable file.
    return text_response(report.content, content_type=report.content_type)


# ------------------------------------------------------------------- tokens


@router.get(f"{PREFIX}/tokens")
def list_tokens(ctx: Context) -> Response:
    tokens = ctx.services.auth.list_api_tokens(ctx.user)
    return json_response(serializers.collection(tokens, serializers.api_token))


@router.post(f"{PREFIX}/tokens")
def create_token(ctx: Context) -> Response:
    body = ctx.request.json()
    token, secret = ctx.services.auth.issue_api_token(
        ctx.user,
        _required(body, "name"),
        roles=body.get("roles"),
        expires_at=body.get("expires_at"),
    )
    payload = serializers.api_token(token)
    # The only time the plaintext exists outside the caller's hands.
    payload["token"] = secret
    payload["note"] = "Store this now: it is not recoverable."
    return json_response(payload, status=201)


@router.delete(f"{PREFIX}/tokens/{{token_id}}")
def revoke_token(ctx: Context) -> Response:
    revoked = ctx.services.auth.revoke_api_token(ctx.user, ctx.param("token_id"))
    return json_response({"revoked": revoked}, status=200 if revoked else 404)


# -------------------------------------------------------------------- users


@router.get(f"{PREFIX}/users")
def list_users(ctx: Context) -> Response:
    users = ctx.services.auth.list_users(ctx.user)
    return json_response(serializers.collection(users, serializers.user))


@router.post(f"{PREFIX}/users")
def create_user(ctx: Context) -> Response:
    body = ctx.request.json()
    user = ctx.services.auth.create_user(
        ctx.user,
        email=_required(body, "email"),
        display_name=body.get("display_name", ""),
        password=body.get("password"),
        roles=body.get("roles") or ["viewer"],
    )
    return json_response(serializers.user(user), status=201)


# ------------------------------------------------------------------ helpers


def _required(body: Dict[str, Any], field: str, *, prefix: str = "") -> Any:
    value = body.get(field)
    if value in (None, ""):
        raise BadRequest(f"{prefix}{field} is required")
    return value
