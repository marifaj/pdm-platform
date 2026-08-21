"""SQLite implementation of the repository protocols.

SQLite is the right default for a single-organisation, server-hosted deployment:
one file to back up, no separate service, and — with WAL enabled — enough
concurrency for a web process and a worker process side by side.

It is also the reason the protocols exist. Nothing above this module names
SQLite, so a PostgreSQL implementation is a sibling file and a configuration
change, which is the migration a multi-client deployment would need.
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
from pathlib import Path
from typing import Any, List, Optional, Sequence

from markna.models import Layer, Location, Severity, Verdict, utc_now

from ..domain import ids
from ..domain import (
    AssessmentRun,
    AuditEvent,
    Conflict,
    CoverageRecord,
    EnvironmentKind,
    EnvironmentTarget,
    FindingRecord,
    Organization,
    Policy,
    Project,
    ReportArtifact,
    RunStatus,
    RunTrigger,
    ScannerRunRecord,
    SourceRepository,
    TenantScope,
)
from ..identity import ApiToken, Role, Session, User

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"
SCHEMA_VERSION = "1"


# ------------------------------------------------------------------ database


class Database:
    """Connection management. One connection per thread, WAL for concurrency."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._local = threading.local()

    @property
    def connection(self) -> sqlite3.Connection:
        existing = getattr(self._local, "connection", None)
        if existing is not None:
            return existing
        if self.path != ":memory:":
            parent = Path(self.path).parent
            if str(parent):
                parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        if self.path != ":memory:":
            # WAL lets the worker write while the web process reads.
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
        self._local.connection = connection
        return connection

    def initialise(self) -> None:
        connection = self.connection
        connection.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        self._migrate()
        connection.execute(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('version', ?)",
            (SCHEMA_VERSION,),
        )

    def _migrate(self) -> None:
        """Additive column migrations for databases created by an earlier build.

        `CREATE TABLE IF NOT EXISTS` leaves an existing table untouched, so a
        new column has to be added explicitly. Every migration here is additive
        and idempotent; none drops or rewrites data.
        """
        for table, column, definition in (
            ("api_tokens", "roles", "TEXT NOT NULL DEFAULT 'viewer'"),
        ):
            existing = {
                row["name"] for row in self.connection.execute(f"PRAGMA table_info({table})")
            }
            if existing and column not in existing:
                self.connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
                )

    def close(self) -> None:
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            connection.close()
            self._local.connection = None

    # -- small helpers used by every repository ---------------------------

    def query(self, sql: str, params: Sequence[Any] = ()) -> List[sqlite3.Row]:
        return list(self.connection.execute(sql, tuple(params)))

    def one(self, sql: str, params: Sequence[Any] = ()) -> Optional[sqlite3.Row]:
        cursor = self.connection.execute(sql, tuple(params))
        return cursor.fetchone()

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        try:
            return self.connection.execute(sql, tuple(params))
        except sqlite3.IntegrityError as exc:
            raise Conflict(str(exc)) from exc

    def executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> int:
        if not rows:
            return 0
        try:
            cursor = self.connection.executemany(sql, [tuple(row) for row in rows])
        except sqlite3.IntegrityError as exc:
            raise Conflict(str(exc)) from exc
        return cursor.rowcount


def _parse_roles(raw: str) -> set:
    """Role names from a stored comma list, ignoring anything unrecognised."""
    known = {role.value: role for role in Role}
    parsed = {known[name] for name in str(raw or "").split(",") if name in known}
    return parsed or {Role.VIEWER}


def _dump(value: Any) -> str:
    return json.dumps(value, default=str, ensure_ascii=False)


def _load(text: Optional[str], default: Any) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return default


def _location_to_json(location: Location) -> str:
    return _dump(
        {
            "file": location.file,
            "line": location.line,
            "end_line": location.end_line,
            "url": location.url,
            "component": location.component,
        }
    )


def _location_from_json(text: Optional[str]) -> Location:
    data = _load(text, {}) or {}
    return Location(
        file=data.get("file"),
        line=data.get("line"),
        end_line=data.get("end_line"),
        url=data.get("url"),
        component=data.get("component"),
    )


# -------------------------------------------------------------- repositories


class SqliteOrganizationRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def create(self, organization: Organization) -> Organization:
        self.db.execute(
            "INSERT INTO organizations(id, slug, name, created_at, is_active, settings)"
            " VALUES (?,?,?,?,?,?)",
            (
                organization.id,
                organization.slug,
                organization.name,
                organization.created_at,
                int(organization.is_active),
                _dump(organization.settings),
            ),
        )
        return organization

    def get(self, organization_id: str) -> Optional[Organization]:
        return self._map(self.db.one("SELECT * FROM organizations WHERE id = ?", (organization_id,)))

    def get_by_slug(self, slug: str) -> Optional[Organization]:
        return self._map(self.db.one("SELECT * FROM organizations WHERE slug = ?", (slug,)))

    def list(self) -> List[Organization]:
        rows = self.db.query("SELECT * FROM organizations ORDER BY created_at")
        return [org for org in (self._map(row) for row in rows) if org is not None]

    def count(self) -> int:
        row = self.db.one("SELECT COUNT(*) AS n FROM organizations")
        return int(row["n"]) if row else 0

    @staticmethod
    def _map(row: Optional[sqlite3.Row]) -> Optional[Organization]:
        if row is None:
            return None
        return Organization(
            id=row["id"],
            slug=row["slug"],
            name=row["name"],
            created_at=row["created_at"],
            is_active=bool(row["is_active"]),
            settings=_load(row["settings"], {}),
        )


class SqliteProjectRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def create(self, scope: TenantScope, project: Project) -> Project:
        _assert_scope(scope, project)
        self.db.execute(
            "INSERT INTO projects(id, organization_id, slug, name, description, source,"
            " architecture_documents, architecture_manifest, default_policy_id,"
            " created_at, updated_at, archived_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                project.id,
                project.organization_id,
                project.slug,
                project.name,
                project.description,
                _dump(project.source.to_dict()),
                _dump(project.architecture_documents),
                project.architecture_manifest,
                project.default_policy_id,
                project.created_at,
                project.updated_at,
                project.archived_at,
            ),
        )
        return project

    def update(self, scope: TenantScope, project: Project) -> Project:
        _assert_scope(scope, project)
        project.updated_at = utc_now()
        self.db.execute(
            "UPDATE projects SET slug=?, name=?, description=?, source=?,"
            " architecture_documents=?, architecture_manifest=?, default_policy_id=?,"
            " updated_at=?, archived_at=? WHERE id=? AND organization_id=?",
            (
                project.slug,
                project.name,
                project.description,
                _dump(project.source.to_dict()),
                _dump(project.architecture_documents),
                project.architecture_manifest,
                project.default_policy_id,
                project.updated_at,
                project.archived_at,
                project.id,
                scope.organization_id,
            ),
        )
        return project

    def get(self, scope: TenantScope, project_id: str) -> Optional[Project]:
        return self._map(
            self.db.one(
                "SELECT * FROM projects WHERE id=? AND organization_id=?",
                (project_id, scope.organization_id),
            )
        )

    def get_by_slug(self, scope: TenantScope, slug: str) -> Optional[Project]:
        return self._map(
            self.db.one(
                "SELECT * FROM projects WHERE slug=? AND organization_id=?",
                (slug, scope.organization_id),
            )
        )

    def list(self, scope: TenantScope, *, include_archived: bool = False) -> List[Project]:
        sql = "SELECT * FROM projects WHERE organization_id=?"
        if not include_archived:
            sql += " AND archived_at IS NULL"
        sql += " ORDER BY name"
        rows = self.db.query(sql, (scope.organization_id,))
        return [p for p in (self._map(row) for row in rows) if p is not None]

    @staticmethod
    def _map(row: Optional[sqlite3.Row]) -> Optional[Project]:
        if row is None:
            return None
        return Project(
            id=row["id"],
            organization_id=row["organization_id"],
            slug=row["slug"],
            name=row["name"],
            description=row["description"],
            source=SourceRepository.from_dict(_load(row["source"], {})),
            architecture_documents=_load(row["architecture_documents"], []),
            architecture_manifest=row["architecture_manifest"],
            default_policy_id=row["default_policy_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            archived_at=row["archived_at"],
        )


class SqliteTargetRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def create(self, scope: TenantScope, target: EnvironmentTarget) -> EnvironmentTarget:
        _assert_scope(scope, target)
        self.db.execute(
            "INSERT INTO environment_targets(id, organization_id, project_id, name, url, kind,"
            " authorized_by, authorization_reference, authorization_expires, scope_hosts,"
            " allow_private_targets, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                target.id,
                target.organization_id,
                target.project_id,
                target.name,
                target.url,
                target.kind.value,
                target.authorized_by,
                target.authorization_reference,
                target.authorization_expires,
                _dump(target.scope_hosts),
                int(target.allow_private_targets),
                target.created_at,
            ),
        )
        return target

    def get(self, scope: TenantScope, target_id: str) -> Optional[EnvironmentTarget]:
        return self._map(
            self.db.one(
                "SELECT * FROM environment_targets WHERE id=? AND organization_id=?",
                (target_id, scope.organization_id),
            )
        )

    def list_for_project(self, scope: TenantScope, project_id: str) -> List[EnvironmentTarget]:
        rows = self.db.query(
            "SELECT * FROM environment_targets WHERE organization_id=? AND project_id=?"
            " ORDER BY name",
            (scope.organization_id, project_id),
        )
        return [t for t in (self._map(row) for row in rows) if t is not None]

    def delete(self, scope: TenantScope, target_id: str) -> bool:
        cursor = self.db.execute(
            "DELETE FROM environment_targets WHERE id=? AND organization_id=?",
            (target_id, scope.organization_id),
        )
        return cursor.rowcount > 0

    @staticmethod
    def _map(row: Optional[sqlite3.Row]) -> Optional[EnvironmentTarget]:
        if row is None:
            return None
        return EnvironmentTarget(
            id=row["id"],
            organization_id=row["organization_id"],
            project_id=row["project_id"],
            name=row["name"],
            url=row["url"],
            kind=EnvironmentKind(row["kind"]),
            authorized_by=row["authorized_by"],
            authorization_reference=row["authorization_reference"],
            authorization_expires=row["authorization_expires"],
            scope_hosts=_load(row["scope_hosts"], []),
            allow_private_targets=bool(row["allow_private_targets"]),
            created_at=row["created_at"],
        )


class SqlitePolicyRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def create(self, scope: TenantScope, policy: Policy) -> Policy:
        _assert_scope(scope, policy)
        self.db.execute(
            "INSERT INTO policies(id, organization_id, project_id, name, version, document,"
            " is_default, created_at, created_by) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                policy.id,
                policy.organization_id,
                policy.project_id,
                policy.name,
                policy.version,
                _dump(policy.document),
                int(policy.is_default),
                policy.created_at,
                policy.created_by,
            ),
        )
        if policy.is_default:
            self.db.execute(
                "UPDATE policies SET is_default=0 WHERE organization_id=? AND project_id IS NULL"
                " AND id<>?",
                (policy.organization_id, policy.id),
            )
        return policy

    def get(self, scope: TenantScope, policy_id: str) -> Optional[Policy]:
        return self._map(
            self.db.one(
                "SELECT * FROM policies WHERE id=? AND organization_id=?",
                (policy_id, scope.organization_id),
            )
        )

    def list(self, scope: TenantScope, *, project_id: Optional[str] = None) -> List[Policy]:
        if project_id is None:
            rows = self.db.query(
                "SELECT * FROM policies WHERE organization_id=? ORDER BY created_at DESC",
                (scope.organization_id,),
            )
        else:
            rows = self.db.query(
                "SELECT * FROM policies WHERE organization_id=? AND project_id=?"
                " ORDER BY version DESC",
                (scope.organization_id, project_id),
            )
        return [p for p in (self._map(row) for row in rows) if p is not None]

    def latest_for_project(self, scope: TenantScope, project_id: str) -> Optional[Policy]:
        return self._map(
            self.db.one(
                "SELECT * FROM policies WHERE organization_id=? AND project_id=?"
                " ORDER BY version DESC LIMIT 1",
                (scope.organization_id, project_id),
            )
        )

    def organization_default(self, scope: TenantScope) -> Optional[Policy]:
        return self._map(
            self.db.one(
                "SELECT * FROM policies WHERE organization_id=? AND project_id IS NULL"
                " ORDER BY is_default DESC, version DESC LIMIT 1",
                (scope.organization_id,),
            )
        )

    def next_version(self, scope: TenantScope, project_id: Optional[str], name: str) -> int:
        if project_id is None:
            row = self.db.one(
                "SELECT MAX(version) AS v FROM policies WHERE organization_id=?"
                " AND project_id IS NULL AND name=?",
                (scope.organization_id, name),
            )
        else:
            row = self.db.one(
                "SELECT MAX(version) AS v FROM policies WHERE organization_id=?"
                " AND project_id=? AND name=?",
                (scope.organization_id, project_id, name),
            )
        current = row["v"] if row and row["v"] is not None else 0
        return int(current) + 1

    @staticmethod
    def _map(row: Optional[sqlite3.Row]) -> Optional[Policy]:
        if row is None:
            return None
        return Policy(
            id=row["id"],
            organization_id=row["organization_id"],
            project_id=row["project_id"],
            name=row["name"],
            version=int(row["version"]),
            document=_load(row["document"], {}),
            is_default=bool(row["is_default"]),
            created_at=row["created_at"],
            created_by=row["created_by"],
        )


class SqliteRunRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    _COLUMNS = (
        "id, organization_id, project_id, policy_id, policy_name, target_id, status, layers,"
        " trigger, verdict, summary, verdict_reasons, engine_assessment_id, git_commit,"
        " git_branch, environment_url, error, worker_id, created_at, started_at, finished_at"
    )

    def create(self, scope: TenantScope, run: AssessmentRun) -> AssessmentRun:
        _assert_scope(scope, run)
        self.db.execute(
            f"INSERT INTO assessment_runs({self._COLUMNS})"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            self._values(run),
        )
        return run

    def get(self, scope: TenantScope, run_id: str) -> Optional[AssessmentRun]:
        return self._map(
            self.db.one(
                "SELECT * FROM assessment_runs WHERE id=? AND organization_id=?",
                (run_id, scope.organization_id),
            )
        )

    def list(
        self,
        scope: TenantScope,
        *,
        project_id: Optional[str] = None,
        status: Optional[RunStatus] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[AssessmentRun]:
        sql = "SELECT * FROM assessment_runs WHERE organization_id=?"
        params: List[Any] = [scope.organization_id]
        if project_id:
            sql += " AND project_id=?"
            params.append(project_id)
        if status:
            sql += " AND status=?"
            params.append(status.value)
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([max(1, min(limit, 500)), max(0, offset)])
        rows = self.db.query(sql, params)
        return [r for r in (self._map(row) for row in rows) if r is not None]

    def update(self, scope: TenantScope, run: AssessmentRun) -> AssessmentRun:
        _assert_scope(scope, run)
        return self._write(run, organization_id=scope.organization_id)

    def cancel(self, scope: TenantScope, run_id: str) -> bool:
        cursor = self.db.execute(
            "UPDATE assessment_runs SET status=?, finished_at=?"
            " WHERE id=? AND organization_id=? AND status=?",
            (RunStatus.CANCELLED.value, utc_now(), run_id, scope.organization_id,
             RunStatus.QUEUED.value),
        )
        return cursor.rowcount > 0

    # -- worker-facing -----------------------------------------------------

    def claim_next_queued(self, worker_id: str) -> Optional[AssessmentRun]:
        """Atomically take the oldest queued run.

        The conditional UPDATE is the lock: two workers racing produce one
        winner and one ``None``. Deliberately cross-tenant — the worker is
        infrastructure, and this is the only place that reads without a scope.
        """
        connection = self.db.connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            row = connection.execute(
                "SELECT id FROM assessment_runs WHERE status=? ORDER BY created_at LIMIT 1",
                (RunStatus.QUEUED.value,),
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            cursor = connection.execute(
                "UPDATE assessment_runs SET status=?, worker_id=?, started_at=?"
                " WHERE id=? AND status=?",
                (RunStatus.RUNNING.value, worker_id, utc_now(), row["id"],
                 RunStatus.QUEUED.value),
            )
            if cursor.rowcount == 0:
                connection.execute("COMMIT")
                return None
            claimed = connection.execute(
                "SELECT * FROM assessment_runs WHERE id=?", (row["id"],)
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        return self._map(claimed)

    def complete(self, run: AssessmentRun) -> AssessmentRun:
        """Write a finished run back. Used by the worker, which has no scope."""
        return self._write(run, organization_id=run.organization_id)

    # -- internals ---------------------------------------------------------

    def _write(self, run: AssessmentRun, *, organization_id: str) -> AssessmentRun:
        self.db.execute(
            "UPDATE assessment_runs SET policy_id=?, policy_name=?, target_id=?, status=?,"
            " layers=?, trigger=?, verdict=?, summary=?, verdict_reasons=?,"
            " engine_assessment_id=?, git_commit=?, git_branch=?, environment_url=?, error=?,"
            " worker_id=?, started_at=?, finished_at=? WHERE id=? AND organization_id=?",
            (
                run.policy_id,
                run.policy_name,
                run.target_id,
                run.status.value,
                _dump([layer.value for layer in run.layers]),
                _dump(run.trigger.to_dict()),
                run.verdict.value if run.verdict else None,
                _dump(run.summary),
                _dump(run.verdict_reasons),
                run.engine_assessment_id,
                run.git_commit,
                run.git_branch,
                run.environment_url,
                run.error,
                run.worker_id,
                run.started_at,
                run.finished_at,
                run.id,
                organization_id,
            ),
        )
        return run

    def _values(self, run: AssessmentRun) -> Sequence[Any]:
        return (
            run.id,
            run.organization_id,
            run.project_id,
            run.policy_id,
            run.policy_name,
            run.target_id,
            run.status.value,
            _dump([layer.value for layer in run.layers]),
            _dump(run.trigger.to_dict()),
            run.verdict.value if run.verdict else None,
            _dump(run.summary),
            _dump(run.verdict_reasons),
            run.engine_assessment_id,
            run.git_commit,
            run.git_branch,
            run.environment_url,
            run.error,
            run.worker_id,
            run.created_at,
            run.started_at,
            run.finished_at,
        )

    @staticmethod
    def _map(row: Optional[sqlite3.Row]) -> Optional[AssessmentRun]:
        if row is None:
            return None
        return AssessmentRun(
            id=row["id"],
            organization_id=row["organization_id"],
            project_id=row["project_id"],
            policy_id=row["policy_id"],
            policy_name=row["policy_name"],
            target_id=row["target_id"],
            status=RunStatus(row["status"]),
            layers=[Layer(value) for value in _load(row["layers"], [])],
            trigger=RunTrigger.from_dict(_load(row["trigger"], {})),
            verdict=Verdict(row["verdict"]) if row["verdict"] else None,
            summary=_load(row["summary"], {}),
            verdict_reasons=_load(row["verdict_reasons"], []),
            engine_assessment_id=row["engine_assessment_id"],
            git_commit=row["git_commit"],
            git_branch=row["git_branch"],
            environment_url=row["environment_url"],
            error=row["error"],
            worker_id=row["worker_id"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )


class SqliteFindingRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def add_all(self, scope: TenantScope, findings: Sequence[FindingRecord]) -> int:
        for finding in findings:
            _assert_scope(scope, finding)
        return self.db.executemany(
            "INSERT INTO findings(id, organization_id, project_id, run_id, engine_id,"
            " fingerprint, source, layer, severity, severity_rank, title, explanation, evidence,"
            " location, remediation, blocking, detected_at, rule_id, refs, tags, cwe,"
            " confidence, ai_generated, suppressed, suppression_reason)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    finding.id,
                    finding.organization_id,
                    finding.project_id,
                    finding.run_id,
                    finding.engine_id,
                    finding.fingerprint,
                    finding.source,
                    finding.layer.value,
                    finding.severity.value,
                    finding.severity.rank,
                    finding.title,
                    finding.explanation,
                    finding.evidence,
                    _location_to_json(finding.location),
                    finding.remediation,
                    int(finding.blocking),
                    finding.detected_at,
                    finding.rule_id,
                    _dump(finding.references),
                    _dump(finding.tags),
                    _dump(finding.cwe),
                    finding.confidence,
                    int(finding.ai_generated),
                    int(finding.suppressed),
                    finding.suppression_reason,
                )
                for finding in findings
            ],
        )

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
    ) -> List[FindingRecord]:
        sql = "SELECT * FROM findings WHERE organization_id=? AND run_id=?"
        params: List[Any] = [scope.organization_id, run_id]
        if severity:
            sql += " AND severity=?"
            params.append(severity)
        if layer:
            sql += " AND layer=?"
            params.append(layer)
        if blocking_only:
            sql += " AND blocking=1"
        if not include_suppressed:
            sql += " AND suppressed=0"
        sql += " ORDER BY severity_rank DESC, blocking DESC, source, id LIMIT ? OFFSET ?"
        params.extend([max(1, min(limit, 2000)), max(0, offset)])
        rows = self.db.query(sql, params)
        return [self._map(row) for row in rows]

    def count_for_run(self, scope: TenantScope, run_id: str) -> int:
        row = self.db.one(
            "SELECT COUNT(*) AS n FROM findings WHERE organization_id=? AND run_id=?"
            " AND suppressed=0",
            (scope.organization_id, run_id),
        )
        return int(row["n"]) if row else 0

    def get(self, scope: TenantScope, finding_id: str) -> Optional[FindingRecord]:
        row = self.db.one(
            "SELECT * FROM findings WHERE id=? AND organization_id=?",
            (finding_id, scope.organization_id),
        )
        return self._map(row) if row else None

    @staticmethod
    def _map(row: sqlite3.Row) -> FindingRecord:
        return FindingRecord(
            id=row["id"],
            organization_id=row["organization_id"],
            project_id=row["project_id"],
            run_id=row["run_id"],
            engine_id=row["engine_id"],
            fingerprint=row["fingerprint"],
            source=row["source"],
            layer=Layer(row["layer"]),
            severity=Severity(row["severity"]),
            title=row["title"],
            explanation=row["explanation"],
            evidence=row["evidence"],
            location=_location_from_json(row["location"]),
            remediation=row["remediation"],
            blocking=bool(row["blocking"]),
            detected_at=row["detected_at"],
            rule_id=row["rule_id"],
            references=_load(row["refs"], []),
            tags=_load(row["tags"], []),
            cwe=_load(row["cwe"], []),
            confidence=row["confidence"],
            ai_generated=bool(row["ai_generated"]),
            suppressed=bool(row["suppressed"]),
            suppression_reason=row["suppression_reason"],
        )


class SqliteRunDetailRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def add_scanner_runs(self, scope: TenantScope, records: Sequence[ScannerRunRecord]) -> int:
        for record in records:
            _assert_scope(scope, record)
        return self.db.executemany(
            "INSERT OR REPLACE INTO scanner_runs(organization_id, run_id, name, layer, status,"
            " capabilities, deterministic, tool, tool_version, command, duration_seconds,"
            " findings_count, message) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    record.organization_id,
                    record.run_id,
                    record.name,
                    record.layer.value,
                    record.status,
                    _dump(record.capabilities),
                    int(record.deterministic),
                    record.tool,
                    record.tool_version,
                    record.command,
                    record.duration_seconds,
                    record.findings_count,
                    record.message,
                )
                for record in records
            ],
        )

    def add_coverage(self, scope: TenantScope, records: Sequence[CoverageRecord]) -> int:
        for record in records:
            _assert_scope(scope, record)
        return self.db.executemany(
            "INSERT OR REPLACE INTO coverage(organization_id, run_id, layer, capability,"
            " required, satisfied, satisfied_by) VALUES (?,?,?,?,?,?,?)",
            [
                (
                    record.organization_id,
                    record.run_id,
                    record.layer.value,
                    record.capability,
                    int(record.required),
                    int(record.satisfied),
                    _dump(record.satisfied_by),
                )
                for record in records
            ],
        )

    def scanner_runs_for(self, scope: TenantScope, run_id: str) -> List[ScannerRunRecord]:
        rows = self.db.query(
            "SELECT * FROM scanner_runs WHERE organization_id=? AND run_id=? ORDER BY layer, name",
            (scope.organization_id, run_id),
        )
        return [
            ScannerRunRecord(
                organization_id=row["organization_id"],
                run_id=row["run_id"],
                name=row["name"],
                layer=Layer(row["layer"]),
                status=row["status"],
                capabilities=_load(row["capabilities"], []),
                deterministic=bool(row["deterministic"]),
                tool=row["tool"],
                tool_version=row["tool_version"],
                command=row["command"],
                duration_seconds=float(row["duration_seconds"]),
                findings_count=int(row["findings_count"]),
                message=row["message"],
            )
            for row in rows
        ]

    def coverage_for(self, scope: TenantScope, run_id: str) -> List[CoverageRecord]:
        rows = self.db.query(
            "SELECT * FROM coverage WHERE organization_id=? AND run_id=? ORDER BY layer, capability",
            (scope.organization_id, run_id),
        )
        return [
            CoverageRecord(
                organization_id=row["organization_id"],
                run_id=row["run_id"],
                layer=Layer(row["layer"]),
                capability=row["capability"],
                required=bool(row["required"]),
                satisfied=bool(row["satisfied"]),
                satisfied_by=_load(row["satisfied_by"], []),
            )
            for row in rows
        ]


class SqliteReportRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def add(self, scope: TenantScope, report: ReportArtifact) -> ReportArtifact:
        _assert_scope(scope, report)
        self.db.execute(
            "INSERT OR REPLACE INTO reports(id, organization_id, project_id, run_id, format,"
            " content_type, size_bytes, content, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                report.id,
                report.organization_id,
                report.project_id,
                report.run_id,
                report.format,
                report.content_type,
                report.size_bytes,
                report.content,
                report.created_at,
            ),
        )
        return report

    def get(self, scope: TenantScope, report_id: str) -> Optional[ReportArtifact]:
        return self._map(
            self.db.one(
                "SELECT * FROM reports WHERE id=? AND organization_id=?",
                (report_id, scope.organization_id),
            )
        )

    def get_by_format(self, scope: TenantScope, run_id: str, fmt: str) -> Optional[ReportArtifact]:
        return self._map(
            self.db.one(
                "SELECT * FROM reports WHERE organization_id=? AND run_id=? AND format=?",
                (scope.organization_id, run_id, fmt),
            )
        )

    def list_for_run(self, scope: TenantScope, run_id: str) -> List[ReportArtifact]:
        rows = self.db.query(
            "SELECT id, organization_id, project_id, run_id, format, content_type, size_bytes,"
            " '' AS content, created_at FROM reports WHERE organization_id=? AND run_id=?"
            " ORDER BY format",
            (scope.organization_id, run_id),
        )
        return [r for r in (self._map(row) for row in rows) if r is not None]

    @staticmethod
    def _map(row: Optional[sqlite3.Row]) -> Optional[ReportArtifact]:
        if row is None:
            return None
        return ReportArtifact(
            id=row["id"],
            organization_id=row["organization_id"],
            project_id=row["project_id"],
            run_id=row["run_id"],
            format=row["format"],
            content_type=row["content_type"],
            size_bytes=int(row["size_bytes"]),
            content=row["content"],
            created_at=row["created_at"],
        )


class SqliteAuditRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def record(self, event: AuditEvent) -> AuditEvent:
        self.db.execute(
            "INSERT INTO audit_events(id, organization_id, actor_id, actor_label, action,"
            " subject_type, subject_id, detail, at) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                event.id,
                event.organization_id,
                event.actor_id,
                event.actor_label,
                event.action,
                event.subject_type,
                event.subject_id,
                _dump(event.detail),
                event.at,
            ),
        )
        return event

    def list(self, scope: TenantScope, *, limit: int = 100) -> List[AuditEvent]:
        rows = self.db.query(
            "SELECT * FROM audit_events WHERE organization_id=? ORDER BY at DESC LIMIT ?",
            (scope.organization_id, max(1, min(limit, 1000))),
        )
        return [
            AuditEvent(
                id=row["id"],
                organization_id=row["organization_id"],
                actor_id=row["actor_id"],
                actor_label=row["actor_label"],
                action=row["action"],
                subject_type=row["subject_type"],
                subject_id=row["subject_id"],
                detail=_load(row["detail"], {}),
                at=row["at"],
            )
            for row in rows
        ]


class SqliteIdentityRepository:
    """Users, tokens and sessions. Also satisfies :class:`IdentityStore`."""

    def __init__(self, db: Database) -> None:
        self.db = db

    # -- users -------------------------------------------------------------

    def create_user(self, user: User) -> User:
        user.created_at = user.created_at or utc_now()
        self.db.execute(
            "INSERT INTO users(id, organization_id, email, display_name, roles, subject, issuer,"
            " password_hash, is_active, created_at, last_login_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                user.id,
                user.organization_id,
                user.email.lower(),
                user.display_name,
                ",".join(sorted(role.value for role in user.roles)),
                user.subject,
                user.issuer,
                user.password_hash,
                int(user.is_active),
                user.created_at,
                user.last_login_at,
            ),
        )
        return user

    def update_user(self, user: User) -> User:
        self.db.execute(
            "UPDATE users SET email=?, display_name=?, roles=?, subject=?, issuer=?,"
            " password_hash=?, is_active=?, last_login_at=? WHERE id=? AND organization_id=?",
            (
                user.email.lower(),
                user.display_name,
                ",".join(sorted(role.value for role in user.roles)),
                user.subject,
                user.issuer,
                user.password_hash,
                int(user.is_active),
                user.last_login_at,
                user.id,
                user.organization_id,
            ),
        )
        return user

    def get_user(self, user_id: str) -> Optional[User]:
        return self._map_user(self.db.one("SELECT * FROM users WHERE id=?", (user_id,)))

    def find_user_by_email(self, organization_id: str, email: str) -> Optional[User]:
        return self._map_user(
            self.db.one(
                "SELECT * FROM users WHERE organization_id=? AND email=?",
                (organization_id, email.lower()),
            )
        )

    def find_users_by_email(self, email: str) -> List[User]:
        """Every active-or-not account with this address, across organisations.

        Login needs this: a principal belongs to one organisation, but the
        person signing in only knows their address. Resolving here — rather
        than assuming the deployment has exactly one organisation — is what
        makes multi-tenant login work.
        """
        rows = self.db.query(
            "SELECT * FROM users WHERE email=? ORDER BY organization_id", (email.lower(),)
        )
        return [u for u in (self._map_user(row) for row in rows) if u is not None]

    def find_user_by_subject(self, issuer: str, subject: str) -> Optional[User]:
        return self._map_user(
            self.db.one(
                "SELECT * FROM users WHERE issuer=? AND subject=?", (issuer, subject)
            )
        )

    def list_users(self, scope: TenantScope) -> List[User]:
        rows = self.db.query(
            "SELECT * FROM users WHERE organization_id=? ORDER BY email",
            (scope.organization_id,),
        )
        return [u for u in (self._map_user(row) for row in rows) if u is not None]

    # -- API tokens --------------------------------------------------------

    def create_api_token(self, token: ApiToken) -> ApiToken:
        self.db.execute(
            "INSERT INTO api_tokens(id, organization_id, user_id, name, roles, token_hash,"
            " created_at, expires_at, last_used_at, revoked_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                token.id,
                token.organization_id,
                token.user_id,
                token.name,
                ",".join(sorted(role.value for role in token.roles)) or Role.VIEWER.value,
                token.token_hash,
                token.created_at,
                token.expires_at,
                token.last_used_at,
                token.revoked_at,
            ),
        )
        return token

    def find_api_token(self, token_hash: str) -> Optional[ApiToken]:
        return self._map_token(
            self.db.one("SELECT * FROM api_tokens WHERE token_hash=?", (token_hash,))
        )

    def touch_api_token(self, token_id: str) -> None:
        self.db.execute("UPDATE api_tokens SET last_used_at=? WHERE id=?", (utc_now(), token_id))

    def revoke_api_token(self, scope: TenantScope, token_id: str) -> bool:
        cursor = self.db.execute(
            "UPDATE api_tokens SET revoked_at=? WHERE id=? AND organization_id=?"
            " AND revoked_at IS NULL",
            (utc_now(), token_id, scope.organization_id),
        )
        return cursor.rowcount > 0

    def list_api_tokens(self, scope: TenantScope) -> List[ApiToken]:
        rows = self.db.query(
            "SELECT * FROM api_tokens WHERE organization_id=? ORDER BY created_at DESC",
            (scope.organization_id,),
        )
        return [t for t in (self._map_token(row) for row in rows) if t is not None]

    # -- authentication throttling ----------------------------------------

    def record_auth_attempt(self, scope_key: str, *, successful: bool) -> None:
        self.db.execute(
            "INSERT INTO auth_attempts(id, scope_key, at, successful) VALUES (?,?,?,?)",
            (ids.new_id(ids.AUDIT), scope_key.lower()[:200], utc_now(), int(successful)),
        )

    def count_recent_auth_failures(self, scope_key: str, since: str) -> int:
        row = self.db.one(
            "SELECT COUNT(*) AS n FROM auth_attempts WHERE scope_key=? AND at>=?"
            " AND successful=0",
            (scope_key.lower()[:200], since),
        )
        return int(row["n"]) if row else 0

    def clear_auth_failures(self, scope_key: str) -> None:
        self.db.execute(
            "DELETE FROM auth_attempts WHERE scope_key=? AND successful=0",
            (scope_key.lower()[:200],),
        )

    def purge_auth_attempts(self, before: str) -> int:
        cursor = self.db.execute("DELETE FROM auth_attempts WHERE at < ?", (before,))
        return cursor.rowcount

    # -- sessions ----------------------------------------------------------

    def create_session(self, session: Session) -> Session:
        session.id = session.id or secrets.token_urlsafe(32)
        self.db.execute(
            "INSERT INTO sessions(id, organization_id, user_id, created_at, expires_at,"
            " user_agent) VALUES (?,?,?,?,?,?)",
            (
                session.id,
                session.organization_id,
                session.user_id,
                session.created_at,
                session.expires_at,
                session.user_agent[:255],
            ),
        )
        return session

    def get_session(self, session_id: str) -> Optional[Session]:
        row = self.db.one("SELECT * FROM sessions WHERE id=?", (session_id,))
        if row is None:
            return None
        return Session(
            id=row["id"],
            organization_id=row["organization_id"],
            user_id=row["user_id"],
            created_at=row["created_at"],
            expires_at=row["expires_at"],
            user_agent=row["user_agent"],
        )

    def delete_session(self, session_id: str) -> None:
        self.db.execute("DELETE FROM sessions WHERE id=?", (session_id,))

    def purge_expired_sessions(self) -> int:
        cursor = self.db.execute("DELETE FROM sessions WHERE expires_at <= ?", (utc_now(),))
        return cursor.rowcount

    # -- mapping -----------------------------------------------------------

    @staticmethod
    def _map_user(row: Optional[sqlite3.Row]) -> Optional[User]:
        if row is None:
            return None
        roles = {
            Role(value)
            for value in str(row["roles"]).split(",")
            if value in {role.value for role in Role}
        }
        return User(
            id=row["id"],
            organization_id=row["organization_id"],
            email=row["email"],
            display_name=row["display_name"],
            roles=roles or {Role.VIEWER},
            subject=row["subject"],
            issuer=row["issuer"],
            password_hash=row["password_hash"],
            is_active=bool(row["is_active"]),
            created_at=row["created_at"],
            last_login_at=row["last_login_at"],
        )

    @staticmethod
    def _map_token(row: Optional[sqlite3.Row]) -> Optional[ApiToken]:
        if row is None:
            return None
        return ApiToken(
            id=row["id"],
            organization_id=row["organization_id"],
            user_id=row["user_id"],
            name=row["name"],
            roles=_parse_roles(row["roles"] if "roles" in row.keys() else ""),
            token_hash=row["token_hash"],
            created_at=row["created_at"],
            expires_at=row["expires_at"],
            last_used_at=row["last_used_at"],
            revoked_at=row["revoked_at"],
        )


class SqliteUnitOfWork:
    """Everything a request or a job needs, bound to one database."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self.organizations = SqliteOrganizationRepository(db)
        self.projects = SqliteProjectRepository(db)
        self.targets = SqliteTargetRepository(db)
        self.policies = SqlitePolicyRepository(db)
        self.runs = SqliteRunRepository(db)
        self.findings = SqliteFindingRepository(db)
        self.run_details = SqliteRunDetailRepository(db)
        self.reports = SqliteReportRepository(db)
        self.audit = SqliteAuditRepository(db)
        self.identity = SqliteIdentityRepository(db)

    def close(self) -> None:
        self.db.close()


def open_unit_of_work(path: str, *, initialise: bool = False) -> SqliteUnitOfWork:
    database = Database(path)
    if initialise or path == ":memory:" or not os.path.exists(path):
        database.initialise()
    return SqliteUnitOfWork(database)


def get_or_create_secret(db: Database, key: str = "secret_key") -> bytes:
    """A stable per-deployment secret, kept with the data it protects.

    Used to derive CSRF tokens. Storing it in the database rather than in a
    process variable means several web processes agree, and a restart does not
    invalidate every open form.
    """
    row = db.one("SELECT value FROM schema_meta WHERE key=?", (key,))
    if row is not None:
        return bytes.fromhex(row["value"])
    secret = secrets.token_bytes(32)
    db.execute(
        "INSERT OR REPLACE INTO schema_meta(key, value) VALUES (?,?)", (key, secret.hex())
    )
    return secret


def _assert_scope(scope: TenantScope, entity: Any) -> None:
    """Refuse to write a record into the wrong organisation.

    Repositories already filter reads by scope; this closes the write side, so a
    mismatched ``organization_id`` cannot be persisted at all.
    """
    owner = getattr(entity, "organization_id", None)
    if owner != scope.organization_id:
        from ..domain import TenantIsolationError

        raise TenantIsolationError(
            f"{type(entity).__name__} belongs to organization {owner!r}, "
            f"but the unit of work is scoped to {scope.organization_id!r}"
        )
