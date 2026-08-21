"""Administrative CLI for the server.

Provisioning, the development server, and the worker. Everything a person can do
in the UI is also reachable here, because the first administrator has to exist
before anyone can sign in.

Commands that change data run as a local administrator principal: shell access
to the server is already administrative, and pretending otherwise would only add
a step, not a control. Every such action is written to the audit log with
``auth_method=cli``.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
from pathlib import Path
from typing import Optional, Sequence

from . import __version__
from .config import ConfigError, ServerConfig
from .domain import DomainError, Organization, TenantScope
from .identity import Principal, Role
from .service import Services
from .storage import open_unit_of_work

EXIT_OK = 0
EXIT_ERROR = 1


def local_admin(organization_id: str) -> Principal:
    """The bootstrap principal, used only before any user exists.

    It owns nothing: it exists so the very first ``create_user`` call has a
    caller. Once an administrator exists, :func:`cli_principal` acts as that
    person instead, so records created from the shell are attributable to a real
    account rather than to a placeholder.
    """
    return Principal(
        id="usr_cli",
        organization_id=organization_id,
        display_name="local administrator (bootstrap)",
        roles=frozenset({Role.ADMIN}),
        email="",
        auth_method="cli-bootstrap",
        is_service=True,
    )


def cli_principal(services: Services, organization: Organization) -> Principal:
    """Act as the organisation's first active administrator.

    Shell access to the server is already administrative, so the CLI does not
    ask for credentials — but anything it creates is owned by, and audited
    against, a real user.
    """
    for user in services.uow.identity.list_users(TenantScope(organization.id)):
        if user.is_active and Role.ADMIN in user.roles:
            principal = user.to_principal("cli")
            return Principal(
                id=principal.id,
                organization_id=principal.organization_id,
                display_name=f"{principal.display_name} (CLI)",
                roles=principal.roles,
                email=principal.email,
                auth_method="cli",
                subject=principal.subject,
                is_service=True,
            )
    raise ConfigError(
        "this organization has no active administrator; run `markna-server bootstrap` "
        "or create one with `markna-server user create --role admin`"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="markna-server",
        description="MARKNA Security Gate — server administration.",
    )
    parser.add_argument("--version", action="version", version=f"markna-server {__version__}")
    parser.add_argument("--config", metavar="FILE", help="server configuration file")
    parser.add_argument(
        "--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR")
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("initdb", help="create the database schema")

    bootstrap = subparsers.add_parser(
        "bootstrap", help="create the organisation and its first administrator"
    )
    bootstrap.add_argument("--org-slug", required=True)
    bootstrap.add_argument("--org-name", required=True)
    bootstrap.add_argument("--admin-email", required=True)
    bootstrap.add_argument(
        "--admin-password", help="omit to create the account without a local password"
    )

    serve = subparsers.add_parser("serve", help="run the development web server")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)

    worker = subparsers.add_parser("worker", help="run the scanner worker")
    worker.add_argument("--once", action="store_true", help="drain the queue and exit")
    worker.add_argument("--worker-id")

    user = subparsers.add_parser("user", help="manage users")
    user_sub = user.add_subparsers(dest="user_command", required=True)
    user_create = user_sub.add_parser("create")
    user_create.add_argument("--email", required=True)
    user_create.add_argument("--name", default="")
    user_create.add_argument("--password")
    user_create.add_argument(
        "--role", action="append", default=[], choices=[role.value for role in Role]
    )
    user_sub.add_parser("list")

    token = subparsers.add_parser("token", help="manage API tokens")
    token_sub = token.add_subparsers(dest="token_command", required=True)
    token_create = token_sub.add_parser("create")
    token_create.add_argument("--name", required=True)
    token_create.add_argument("--expires", help="YYYY-MM-DDTHH:MM:SSZ")
    token_sub.add_parser("list")
    token_revoke = token_sub.add_parser("revoke")
    token_revoke.add_argument("token_id")

    project = subparsers.add_parser("project", help="manage projects")
    project_sub = project.add_subparsers(dest="project_command", required=True)
    project_create = project_sub.add_parser("create")
    project_create.add_argument("--slug", required=True)
    project_create.add_argument("--name", required=True)
    project_create.add_argument("--description", default="")
    project_create.add_argument("--source-path", help="path under the workspace root")
    project_create.add_argument("--repo-url")
    project_create.add_argument("--arch-manifest")
    project_create.add_argument("--arch-doc", action="append", default=[])
    project_sub.add_parser("list")

    policy = subparsers.add_parser("policy", help="manage policies")
    policy_sub = policy.add_subparsers(dest="policy_command", required=True)
    policy_import = policy_sub.add_parser("import")
    policy_import.add_argument("file", help="policy YAML or JSON document")
    policy_import.add_argument("--name")
    policy_import.add_argument("--project-id")
    policy_import.add_argument("--default", action="store_true")
    policy_sub.add_parser("list")

    subparsers.add_parser("status", help="show configuration and queue state")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    try:
        config = ServerConfig.load(args.config)
    except ConfigError as exc:
        print(f"markna-server: {exc}", file=sys.stderr)
        return EXIT_ERROR

    try:
        return _dispatch(args, config)
    except DomainError as exc:
        print(f"markna-server: {exc.message}", file=sys.stderr)
        return EXIT_ERROR
    except ConfigError as exc:
        print(f"markna-server: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except KeyboardInterrupt:  # pragma: no cover - interactive
        return EXIT_OK


def _dispatch(args: argparse.Namespace, config: ServerConfig) -> int:
    if args.command == "initdb":
        return _initdb(config)
    if args.command == "bootstrap":
        return _bootstrap(args, config)
    if args.command == "serve":
        return _serve(args, config)
    if args.command == "worker":
        return _worker(args, config)
    if args.command == "user":
        return _user(args, config)
    if args.command == "token":
        return _token(args, config)
    if args.command == "project":
        return _project(args, config)
    if args.command == "policy":
        return _policy(args, config)
    if args.command == "status":
        return _status(config)
    return EXIT_ERROR


# ------------------------------------------------------------------ commands


def _initdb(config: ServerConfig) -> int:
    open_unit_of_work(config.database_path, initialise=True)
    Path(config.workspace_root).mkdir(parents=True, exist_ok=True)
    print(f"initialised {config.database_path}")
    print(f"workspace root {config.workspace_path}")
    return EXIT_OK


def _bootstrap(args: argparse.Namespace, config: ServerConfig) -> int:
    services = _services(config, initialise=True)
    if services.organizations.count() > 0:
        print(
            "markna-server: this deployment already has an organisation. v1.0 is "
            "single-organisation; see docs/architecture.md for the multi-tenant path.",
            file=sys.stderr,
        )
        return EXIT_ERROR

    organization = services.organizations.bootstrap(args.org_slug, args.org_name)
    admin = services.auth.create_user(
        local_admin(organization.id),
        email=args.admin_email,
        display_name=args.admin_email,
        password=args.admin_password,
        roles=[Role.ADMIN.value],
    )
    Path(config.workspace_root).mkdir(parents=True, exist_ok=True)
    print(f"organization {organization.id}  {organization.slug}  {organization.name}")
    print(f"administrator {admin.id}  {admin.email}")
    if not args.admin_password:
        print(
            "no password set: this account cannot sign in locally. Give it one with "
            "`markna-server user create`, or configure an identity provider."
        )
    return EXIT_OK


def _serve(args: argparse.Namespace, config: ServerConfig) -> int:
    from wsgiref.simple_server import make_server

    from .app import create_app

    host = args.host or config.host
    port = args.port or config.port
    app = create_app(config)
    print(f"MARKNA Security Gate {__version__} serving on http://{host}:{port}")
    print(f"  database   {config.database_path}")
    print(f"  auth       {', '.join(config.auth_providers)}")
    print("  scanners   run in a separate `markna-server worker` process")
    if not config.secure_cookies:
        print("  WARNING    secure_cookies is off; use it only for local HTTP development")
    with make_server(host, port, app) as server:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nstopped")
    return EXIT_OK


def _worker(args: argparse.Namespace, config: ServerConfig) -> int:
    # Imported here, not at module scope: this is the only entry point that
    # pulls the scanner engine into the process.
    from .execution import Worker

    uow = open_unit_of_work(config.database_path)
    worker = Worker(uow, config, worker_id=args.worker_id)
    if args.once:
        executed = worker.drain()
        print(f"executed {executed} run(s)")
        return EXIT_OK

    stop = threading.Event()
    for signal_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signal_name):
            signal.signal(getattr(signal, signal_name), lambda *_: stop.set())
    worker.run_forever(stop)
    return EXIT_OK


def _user(args: argparse.Namespace, config: ServerConfig) -> int:
    services = _services(config)
    organization = _only_organization(services)
    principal = cli_principal(services, organization)
    if args.user_command == "create":
        user = services.auth.create_user(
            principal,
            email=args.email,
            display_name=args.name or args.email,
            password=args.password,
            roles=args.role or [Role.VIEWER.value],
        )
        print(f"{user.id}  {user.email}  {', '.join(sorted(r.value for r in user.roles))}")
        return EXIT_OK
    for user in services.auth.list_users(principal):
        roles = ", ".join(sorted(role.value for role in user.roles))
        print(f"{user.id}  {user.email:<32} {roles:<24} active={user.is_active}")
    return EXIT_OK


def _token(args: argparse.Namespace, config: ServerConfig) -> int:
    services = _services(config)
    principal = cli_principal(services, _only_organization(services))
    if args.token_command == "create":
        token, secret = services.auth.issue_api_token(principal, args.name, expires_at=args.expires)
        print(f"{token.id}  {token.name}")
        print(secret)
        print("store this now: only its hash is kept")
        return EXIT_OK
    if args.token_command == "revoke":
        revoked = services.auth.revoke_api_token(principal, args.token_id)
        print("revoked" if revoked else "not found or already revoked")
        return EXIT_OK if revoked else EXIT_ERROR
    for token in services.auth.list_api_tokens(principal):
        state = "usable" if token.is_usable else "revoked/expired"
        print(f"{token.id}  {token.name:<28} {state:<16} last_used={token.last_used_at or 'never'}")
    return EXIT_OK


def _project(args: argparse.Namespace, config: ServerConfig) -> int:
    services = _services(config)
    principal = cli_principal(services, _only_organization(services))
    if args.project_command == "create":
        project = services.projects.create(
            principal,
            slug=args.slug,
            name=args.name,
            description=args.description,
            source={"local_path": args.source_path, "url": args.repo_url},
            architecture_documents=args.arch_doc,
            architecture_manifest=args.arch_manifest,
        )
        print(f"{project.id}  {project.slug}  {project.name}")
        return EXIT_OK
    for project in services.projects.list(principal):
        print(f"{project.id}  {project.slug:<24} {project.name}")
    return EXIT_OK


def _policy(args: argparse.Namespace, config: ServerConfig) -> int:
    services = _services(config)
    principal = cli_principal(services, _only_organization(services))
    if args.policy_command == "import":
        document = _read_document(Path(args.file))
        policy = services.policies.create_version(
            principal,
            name=args.name or document.get("name") or Path(args.file).stem,
            document=document,
            project_id=args.project_id,
            is_default=args.default,
        )
        print(f"{policy.id}  {policy.name} v{policy.version}  scope={policy.scope_label}")
        return EXIT_OK
    for policy in services.policies.list(principal):
        default = " (default)" if policy.is_default else ""
        print(f"{policy.id}  {policy.name} v{policy.version}  {policy.scope_label}{default}")
    return EXIT_OK


def _status(config: ServerConfig) -> int:
    services = _services(config)
    organizations = services.uow.organizations.list()
    print(f"markna-server {__version__}")
    print(f"  database        {config.database_path}")
    print(f"  workspace root  {config.workspace_path}")
    print(f"  auth providers  {', '.join(config.auth_providers)}")
    print(f"  report formats  {', '.join(config.report_formats)}")
    print(f"  AI layer        {'enabled' if config.ai_enabled else 'disabled'}")
    print(f"  organizations   {len(organizations)}")
    for organization in organizations:
        scope = TenantScope(organization.id)
        projects = services.uow.projects.list(scope)
        queued = services.uow.runs.list(scope, limit=500)
        by_status: dict = {}
        for run in queued:
            by_status[run.status.value] = by_status.get(run.status.value, 0) + 1
        states = ", ".join(f"{count} {status}" for status, count in sorted(by_status.items()))
        print(f"    {organization.slug:<20} {len(projects)} project(s)  runs: {states or 'none'}")
    return EXIT_OK


# ------------------------------------------------------------------ helpers


def _services(config: ServerConfig, *, initialise: bool = False) -> Services:
    uow = open_unit_of_work(config.database_path, initialise=initialise)
    return Services.build(uow, config)


def _only_organization(services: Services) -> Organization:
    organizations = services.uow.organizations.list()
    if not organizations:
        raise ConfigError("no organization exists yet; run `markna-server bootstrap` first")
    if len(organizations) > 1:
        raise ConfigError(
            "this deployment has more than one organization, so the CLI cannot infer which "
            "one to act on. v1.0 ships single-organisation; see docs/architecture.md."
        )
    return organizations[0]


def _read_document(path: Path) -> dict:
    if not path.is_file():
        raise ConfigError(f"policy file not found: {path}")
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text)
    import yaml

    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"policy file {path} must contain a mapping")
    return data


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
