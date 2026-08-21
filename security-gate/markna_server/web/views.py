"""Server-rendered UI.

The same services the API calls, rendered as HTML. No JavaScript and no external
assets, which is why the application can ship the strict Content-Security-Policy
in :mod:`markna_server.api.wsgi`.

Every state-changing route is a POST carrying a session-bound CSRF token.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .. import __version__
from ..domain import DomainError, EnvironmentKind, TriggerKind, ValidationError
from ..service import TooManyAttempts
from ..identity import SESSION_COOKIE, Permission
from ..api.wsgi import (
    Context,
    Request,
    Response,
    Router,
    csrf_token,
    csrf_valid,
    html_response,
    redirect,
    text_response,
)

TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"

_environment = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    autoescape=select_autoescape(["html"]),
    trim_blocks=True,
    lstrip_blocks=True,
)

router = Router()

#: Pages reachable without a session.
PUBLIC_PATHS = {"/login", "/healthz"}


class CsrfError(DomainError):
    status = 403
    code = "csrf_failed"


# ------------------------------------------------------------------ plumbing


def render(ctx: Context, template: str, status: int = 200, **variables: Any) -> Response:
    organization = variables.pop("organization", None)
    if organization is None and ctx.principal is not None:
        organization = ctx.services.organizations.get(ctx.principal)
    common = {
        "principal": ctx.principal,
        "organization": organization,
        "version": __version__,
        "csrf_token": _token_for(ctx),
        "message": ctx.request.get("msg") or None,
        "message_kind": "error" if ctx.request.get("err") else "info",
    }
    if ctx.request.get("err"):
        common["message"] = ctx.request.get("err")
    common.update(variables)
    return html_response(_environment.get_template(template).render(**common), status=status)


def render_error(ctx: Context, status: int, title: str, message: str) -> Response:
    return render(ctx, "error.html", status=status, status_code=status, title=title, message=message)


def _token_for(ctx: Context) -> str:
    session_id = ctx.request.cookies.get(SESSION_COOKIE, "")
    return csrf_token(ctx.secret_key, session_id) if session_id else ""


def _require_csrf(ctx: Context) -> Dict[str, str]:
    form = ctx.request.form()
    session_id = ctx.request.cookies.get(SESSION_COOKIE, "")
    if not csrf_valid(ctx.secret_key, session_id, form.get("csrf_token", "")):
        raise CsrfError("the form token did not match this session; reload the page and retry")
    return form


def _redirect_with(path: str, *, message: str = "", error: str = "") -> Response:
    from urllib.parse import urlencode

    query = {key: value for key, value in (("msg", message), ("err", error)) if value}
    return redirect(f"{path}?{urlencode(query)}" if query else path)


def _can(ctx: Context, permission: Permission) -> bool:
    return ctx.principal is not None and ctx.principal.has(permission)


def _organization_count(ctx: Context) -> int:
    return ctx.services.organizations.count()


def _login_organization(ctx: Context):
    """The organisation to *name* on the sign-in page, if naming one is safe.

    Cosmetic only: with a single organisation the page can greet the user by
    name, and with several it stays anonymous. Authentication itself no longer
    depends on this — sign-in resolves the account from the address across every
    organisation, so more than one tenant does not break login.
    """
    organizations = ctx.services.uow.organizations.list()
    return organizations[0] if len(organizations) == 1 else None


# --------------------------------------------------------------------- auth


@router.get("/healthz")
def healthz(ctx: Context) -> Response:
    """Liveness probe for the web surface. Says nothing about tenants."""
    return text_response("ok\n")


@router.get("/login")
def login_form(ctx: Context) -> Response:
    if ctx.principal is not None:
        return redirect("/")
    return render(
        ctx,
        "login.html",
        organization=_login_organization(ctx),
        email=ctx.request.get("email"),
        show_organization=_organization_count(ctx) > 1,
        error=ctx.request.get("err") or None,
    )


@router.post("/login")
def login(ctx: Context) -> Response:
    form = ctx.request.form()
    email = form.get("email", "").strip().lower()
    slug = form.get("organization", "").strip().lower() or None
    try:
        _, session = ctx.services.auth.login(
            email,
            form.get("password", ""),
            organization_slug=slug,
            user_agent=ctx.request.headers.get("user-agent", ""),
            client_key=ctx.request.remote_addr,
        )
    except TooManyAttempts as exc:
        return render(
            ctx, "login.html", organization=_login_organization(ctx), email=email,
            show_organization=_organization_count(ctx) > 1, error=exc.message, status=429,
        )
    except ValidationError as exc:
        # Identical message for an unknown address and a wrong password.
        return render(
            ctx, "login.html", organization=_login_organization(ctx), email=email,
            show_organization=_organization_count(ctx) > 1, error=exc.message, status=401,
        )
    response = redirect("/")
    return response.set_cookie(
        SESSION_COOKIE,
        session.id,
        max_age=ctx.services.config.session_hours * 3600,
        secure=ctx.services.config.secure_cookies,
    )


@router.post("/logout")
def logout(ctx: Context) -> Response:
    session_id = ctx.request.cookies.get(SESSION_COOKIE, "")
    if session_id:
        _require_csrf(ctx)
        ctx.services.auth.logout(session_id)
    return redirect("/login").clear_cookie(
        SESSION_COOKIE, secure=ctx.services.config.secure_cookies
    )


# ---------------------------------------------------------------- dashboard


@router.get("/")
def dashboard(ctx: Context) -> Response:
    runs = ctx.services.runs.list(ctx.user, limit=15)
    projects = ctx.services.projects.list(ctx.user)
    return render(
        ctx,
        "dashboard.html",
        summary=ctx.services.runs.summary(ctx.user),
        runs=runs,
        project_names={project.id: project.name for project in projects},
    )


# ----------------------------------------------------------------- projects


@router.get("/projects")
def projects(ctx: Context) -> Response:
    rows: List[Dict[str, Any]] = []
    for project in ctx.services.projects.list(ctx.user):
        targets = ctx.services.projects.list_targets(ctx.user, project.id)
        project_runs = ctx.services.runs.list(ctx.user, project_id=project.id, limit=50)
        rows.append(
            {
                "project": project,
                "layers": [layer.value for layer in project.supported_layers(targets)],
                "latest": project_runs[0] if project_runs else None,
                "run_count": len(project_runs),
            }
        )
    return render(
        ctx,
        "projects.html",
        projects=rows,
        can_write=_can(ctx, Permission.PROJECT_WRITE),
        workspace_root=ctx.services.config.workspace_root,
    )


@router.post("/projects")
def create_project(ctx: Context) -> Response:
    form = _require_csrf(ctx)
    documents = [
        item.strip() for item in form.get("architecture_documents", "").split(",") if item.strip()
    ]
    try:
        project = ctx.services.projects.create(
            ctx.user,
            slug=form.get("slug", ""),
            name=form.get("name", ""),
            description=form.get("description", ""),
            source={
                "local_path": form.get("local_path") or None,
                "url": form.get("repo_url") or None,
            },
            architecture_documents=documents,
            architecture_manifest=form.get("architecture_manifest") or None,
        )
    except DomainError as exc:
        return _redirect_with("/projects", error=exc.message)
    return _redirect_with(f"/projects/{project.id}", message="Project created.")


@router.get("/projects/{project_id}")
def project_detail(ctx: Context) -> Response:
    project = ctx.services.projects.get(ctx.user, ctx.param("project_id"))
    targets = ctx.services.projects.list_targets(ctx.user, project.id)
    stored_policy, _ = ctx.services.policies.resolve_for_project(ctx.user.scope, project)
    return render(
        ctx,
        "project.html",
        project=project,
        targets=targets,
        policy=stored_policy,
        layers=[layer.value for layer in project.supported_layers(targets)],
        runs=ctx.services.runs.list(ctx.user, project_id=project.id, limit=25),
        environment_kinds=[kind.value for kind in EnvironmentKind],
        can_write=_can(ctx, Permission.PROJECT_WRITE),
        can_run=_can(ctx, Permission.RUN_CREATE),
    )


@router.post("/projects/{project_id}/targets")
def create_target(ctx: Context) -> Response:
    form = _require_csrf(ctx)
    project_id = ctx.param("project_id")
    try:
        ctx.services.projects.add_target(
            ctx.user,
            project_id,
            name=form.get("name", ""),
            url=form.get("url", ""),
            kind=form.get("kind", "uat"),
            authorized_by=form.get("authorized_by", ""),
            authorization_reference=form.get("authorization_reference") or None,
            authorization_expires=form.get("authorization_expires") or None,
            scope_hosts=[h.strip() for h in form.get("scope_hosts", "").split(",") if h.strip()],
            allow_private_targets=form.get("allow_private_targets") == "1",
        )
    except DomainError as exc:
        return _redirect_with(f"/projects/{project_id}", error=exc.message)
    return _redirect_with(f"/projects/{project_id}", message="Target added.")


@router.post("/targets/{target_id}/delete")
def delete_target(ctx: Context) -> Response:
    _require_csrf(ctx)
    ctx.services.projects.delete_target(ctx.user, ctx.param("target_id"))
    return _redirect_with("/projects", message="Target removed.")


# --------------------------------------------------------------------- runs


@router.post("/projects/{project_id}/runs")
def queue_run(ctx: Context) -> Response:
    form = _require_csrf(ctx)
    project_id = ctx.param("project_id")
    # A checkbox group arrives as repeated keys; Request.form keeps the last, so
    # parse the raw body to collect every selected layer.
    layers = _selected_layers(ctx.request)
    try:
        run = ctx.services.runs.enqueue(
            ctx.user,
            project_id,
            layers=layers or None,
            target_id=form.get("target_id") or None,
            trigger_kind=TriggerKind.MANUAL,
        )
    except DomainError as exc:
        return _redirect_with(f"/projects/{project_id}", error=exc.message)
    return _redirect_with(f"/runs/{run.id}", message="Run queued. A worker will pick it up.")


@router.get("/runs")
def runs(ctx: Context) -> Response:
    all_runs = ctx.services.runs.list(ctx.user, limit=100)
    projects = ctx.services.projects.list(ctx.user, include_archived=True)
    return render(
        ctx,
        "runs.html",
        runs=all_runs,
        project_names={project.id: project.name for project in projects},
    )


@router.get("/runs/{run_id}")
def run_detail(ctx: Context) -> Response:
    request = ctx.request
    detail = ctx.services.runs.detail(
        ctx.user,
        ctx.param("run_id"),
        severity=request.get("severity") or None,
        layer=request.get("layer") or None,
        blocking_only=request.get_bool("blocking"),
        limit=request.get_int("limit", 200),
    )
    return render(
        ctx,
        "run.html",
        detail=detail,
        filters={
            "severity": request.get("severity"),
            "layer": request.get("layer"),
            "blocking": request.get_bool("blocking"),
        },
        can_cancel=_can(ctx, Permission.RUN_CANCEL),
    )


@router.post("/runs/{run_id}/cancel")
def cancel_run(ctx: Context) -> Response:
    _require_csrf(ctx)
    run_id = ctx.param("run_id")
    cancelled = ctx.services.runs.cancel(ctx.user, run_id)
    return _redirect_with(
        f"/runs/{run_id}",
        message="Run cancelled." if cancelled else "",
        error="" if cancelled else "Only a queued run can be cancelled.",
    )


@router.get("/runs/{run_id}/reports/{fmt}")
def report(ctx: Context) -> Response:
    artifact = ctx.services.reports.get(ctx.user, ctx.param("run_id"), ctx.param("fmt"))
    response = text_response(artifact.content, content_type=artifact.content_type)
    if artifact.format != "html":
        # Everything but the HTML report is a file to keep, not a page to view.
        response.with_header(
            "Content-Disposition",
            f'attachment; filename="markna-{artifact.run_id}.{_extension(artifact.format)}"',
        )
    return response


# ----------------------------------------------------------------- policies


@router.get("/policies")
def policies(ctx: Context) -> Response:
    stored = ctx.services.policies.list(ctx.user)
    projects = ctx.services.projects.list(ctx.user, include_archived=True)
    return render(
        ctx,
        "policies.html",
        policies=stored,
        project_names={project.id: project.name for project in projects},
    )


# ------------------------------------------------------------------ helpers


def _selected_layers(request: Request) -> List[str]:
    from urllib.parse import parse_qs

    parsed = parse_qs(request.body.decode("utf-8", errors="replace"), keep_blank_values=False)
    return [value for value in parsed.get("layers", []) if value]


def _extension(fmt: str) -> str:
    return {"json": "json", "markdown": "md", "sarif": "sarif", "html": "html"}.get(fmt, "txt")
