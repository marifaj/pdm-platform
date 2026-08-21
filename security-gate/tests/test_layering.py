"""Architectural constraints, enforced rather than described.

Every rule here is one a reviewer would otherwise have to re-check by hand on
each change:

* the user-facing layers cannot execute scanners;
* the domain has no idea how it is stored or served;
* the scanner engine has never heard of organisations or projects;
* tenant-scoped repository methods take a scope, structurally.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Dict, Iterator, List, Set

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
SERVER = PACKAGE_ROOT / "markna_server"
ENGINE = PACKAGE_ROOT / "markna"


def modules_under(directory: Path) -> Iterator[Path]:
    for path in sorted(directory.rglob("*.py")):
        if "__pycache__" not in path.parts:
            yield path


def imports_of(path: Path) -> Set[str]:
    """Every module name imported by ``path``, resolved for relative imports."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    package_parts = path.relative_to(PACKAGE_ROOT).with_suffix("").parts
    found: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                if node.module:
                    found.add(node.module)
            else:
                base = list(package_parts[: len(package_parts) - node.level])
                found.add(".".join(base + ([node.module] if node.module else [])))
    return found


def module_label(path: Path) -> str:
    return ".".join(path.relative_to(PACKAGE_ROOT).with_suffix("").parts)


@pytest.fixture(scope="module")
def server_imports() -> Dict[str, Set[str]]:
    return {module_label(path): imports_of(path) for path in modules_under(SERVER)}


@pytest.fixture(scope="module")
def engine_imports() -> Dict[str, Set[str]]:
    return {module_label(path): imports_of(path) for path in modules_under(ENGINE)}


def _violations(imports: Dict[str, Set[str]], prefixes: List[str], forbidden: List[str]):
    offenders = []
    for module, imported in imports.items():
        if not any(module.startswith(prefix) for prefix in prefixes):
            continue
        for name in imported:
            if any(name == bad or name.startswith(bad + ".") for bad in forbidden):
                offenders.append((module, name))
    return offenders


class TestExecutionIsSeparated:
    """Scanner execution must not be reachable from a request handler."""

    def test_api_and_web_do_not_import_the_engine_runner(self, server_imports):
        offenders = _violations(
            server_imports,
            ["markna_server.api", "markna_server.web"],
            ["markna.runner", "markna.scanners", "markna_server.execution"],
        )
        assert not offenders, f"user-facing code must not execute scanners: {offenders}"

    def test_services_do_not_import_the_engine_runner(self, server_imports):
        offenders = _violations(
            server_imports,
            ["markna_server.service"],
            ["markna.runner", "markna.scanners", "markna_server.execution"],
        )
        assert not offenders, f"the service layer queues runs, it does not run them: {offenders}"

    def test_the_wsgi_application_does_not_import_execution(self, server_imports):
        offenders = _violations(
            server_imports, ["markna_server.app"], ["markna_server.execution", "markna.runner"]
        )
        assert not offenders, f"the web process must not pull in the engine: {offenders}"

    def test_only_the_execution_package_drives_the_engine(self, server_imports):
        drivers = {
            module
            for module, imported in server_imports.items()
            if any(name.startswith("markna.runner") for name in imported)
        }
        # The CLI imports it lazily inside the worker command, which is why it
        # does not appear here; the assertion is that no other module does.
        assert drivers == {"markna_server.execution.adapter"}, drivers

    def test_importing_the_app_does_not_import_the_engine_runner(self):
        """A belt-and-braces check on the real import graph, not just the AST."""
        import subprocess
        import sys

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; import markna_server.app as a; "
                "print('markna.runner' in sys.modules)",
            ],
            cwd=str(PACKAGE_ROOT),
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "False", "importing the app pulled in the scanner runner"


class TestLayerDirection:
    def test_the_domain_depends_on_nothing_above_it(self, server_imports):
        offenders = _violations(
            server_imports,
            ["markna_server.domain"],
            [
                "markna_server.storage",
                "markna_server.api",
                "markna_server.web",
                "markna_server.service",
                "markna_server.execution",
                "markna_server.app",
            ],
        )
        assert not offenders, f"the domain must not know how it is stored or served: {offenders}"

    def test_storage_does_not_depend_on_http_or_services(self, server_imports):
        offenders = _violations(
            server_imports,
            ["markna_server.storage"],
            ["markna_server.api", "markna_server.web", "markna_server.service"],
        )
        assert not offenders, offenders

    def test_identity_does_not_depend_on_http(self, server_imports):
        offenders = _violations(
            server_imports, ["markna_server.identity"], ["markna_server.api", "markna_server.web"]
        )
        assert not offenders, offenders

    def test_the_engine_knows_nothing_about_the_server(self, engine_imports):
        offenders = _violations(engine_imports, ["markna"], ["markna_server"])
        assert not offenders, f"the scanner engine must stay standalone: {offenders}"


class TestTenantScopingIsStructural:
    """Tenant-scoped repository methods must take a scope, not rely on discipline."""

    #: Repositories that are legitimately not tenant-scoped, with the reason.
    UNSCOPED = {
        "OrganizationRepository": "defines tenants; cannot be scoped to one",
        "IdentityRepository": "authentication happens before a tenant is known",
        "AuditRepository": "record() is called with an event that carries its own tenant",
        "UnitOfWork": "a container, not a repository",
    }
    #: Worker-facing methods: the worker is infrastructure and has no principal.
    WORKER_METHODS = {"claim_next_queued", "complete"}

    def test_every_scoped_repository_method_takes_a_scope_first(self):
        source = (SERVER / "storage" / "repositories.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        checked = 0
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef) or node.name in self.UNSCOPED:
                continue
            for method in node.body:
                if not isinstance(method, ast.FunctionDef) or method.name.startswith("_"):
                    continue
                if method.name in self.WORKER_METHODS:
                    continue
                arguments = [argument.arg for argument in method.args.args]
                assert arguments[:2] == ["self", "scope"], (
                    f"{node.name}.{method.name} must take a TenantScope as its first argument; "
                    f"got {arguments[:2]}"
                )
                checked += 1
        assert checked > 15, "the scoping check did not inspect enough methods to be meaningful"

    def test_the_worker_surface_is_small_and_named(self):
        """Cross-tenant reads exist only where documented."""
        source = (SERVER / "storage" / "repositories.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        run_repository = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.ClassDef) and node.name == "RunRepository"
        )
        unscoped = {
            method.name
            for method in run_repository.body
            if isinstance(method, ast.FunctionDef)
            and not method.name.startswith("_")
            and [argument.arg for argument in method.args.args][:2] != ["self", "scope"]
        }
        assert unscoped == self.WORKER_METHODS, unscoped

    def test_every_tenant_table_carries_an_organization_id(self):
        schema = (SERVER / "storage" / "schema.sql").read_text(encoding="utf-8")
        statements = schema.split("CREATE TABLE IF NOT EXISTS ")[1:]
        # auth_attempts is deliberately tenant-free: throttling has to happen
        # before an account — and therefore an organisation — has been
        # identified, and keying it by tenant would let an attacker sidestep the
        # limit by guessing against a different organisation.
        exempt = {"organizations", "schema_meta", "auth_attempts"}
        for statement in statements:
            table = statement.split("(", 1)[0].strip()
            if table in exempt:
                continue
            assert "organization_id" in statement, f"table {table} has no organization_id"


class TestNoHardcodedTenantOrProject:
    """Nothing may assume a particular organisation or project exists."""

    def test_no_module_hardcodes_an_identifier(self, server_imports):
        import re

        pattern = re.compile(r"['\"](?:org|prj)_[0-9a-f]{18}['\"]")
        for path in modules_under(SERVER):
            assert not pattern.search(path.read_text(encoding="utf-8")), (
                f"{module_label(path)} contains a hard-coded organisation or project id"
            )

    def test_single_organization_is_a_policy_not_an_assumption(self):
        """The one-organisation rule lives in the CLI, not in the data model."""
        cli_source = (SERVER / "cli.py").read_text(encoding="utf-8")
        assert "more than one organization" in cli_source
        for module in ("service.py", "app.py", "storage/sqlite.py"):
            source = (SERVER / module).read_text(encoding="utf-8")
            assert "organizations.list()[0]" not in source, (
                f"{module} picks 'the' organisation; that belongs in the CLI or the login page"
            )
