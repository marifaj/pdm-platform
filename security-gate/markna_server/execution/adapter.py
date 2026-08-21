"""The bridge between stored domain records and the scanner engine.

This is the only module in the server that imports :mod:`markna.runner`. It
translates in both directions:

* **In** — a project, its policy version and its authorised target become a
  :class:`markna.runner.RunConfig`. The engine still knows nothing about
  organisations or projects.
* **Out** — the engine's :class:`~markna.models.Assessment` becomes findings,
  scanner-execution records, coverage rows and rendered reports, each stamped
  with the owning organisation and project.

The worker runs in its own process, so an engine that hangs, leaks memory or
spawns a container cannot affect the web process.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from markna.authorization import Authorization
from markna.exec import ToolPath
from markna.models import Assessment, Layer, utc_now
from markna.policy import Policy as EnginePolicy
from markna.report import render_html, render_json, render_markdown, render_sarif
from markna.runner import ConfigurationError, RunConfig, Runner

from ..config import ServerConfig
from ..domain import (
    AssessmentRun,
    CoverageRecord,
    EnvironmentTarget,
    FindingRecord,
    NotFound,
    Policy,
    Project,
    ReportArtifact,
    RunStatus,
    ScannerRunRecord,
    TenantScope,
)
from ..storage import UnitOfWork

_RENDERERS = {
    "json": (render_json, "application/json"),
    "markdown": (render_markdown, "text/markdown; charset=utf-8"),
    "sarif": (render_sarif, "application/sarif+json"),
    "html": (render_html, "text/html; charset=utf-8"),
}


@dataclass
class ExecutionInputs:
    """Everything resolved from storage before the engine is touched."""

    project: Project
    policy: EnginePolicy
    stored_policy: Optional[Policy]
    target: Optional[EnvironmentTarget]
    scope: TenantScope


class RunExecutor:
    """Executes one queued run and persists everything it produced."""

    def __init__(self, uow: UnitOfWork, config: ServerConfig) -> None:
        self.uow = uow
        self.config = config

    # ------------------------------------------------------------------ main

    def execute(self, run: AssessmentRun) -> AssessmentRun:
        """Run the engine and persist the outcome. Never raises for scan failure.

        A scanner that fails is data. A *run* that fails — bad configuration, a
        deleted project — is recorded on the run row so the failure is visible
        in the UI rather than only in the worker's log.
        """
        try:
            inputs = self._resolve(run)
            run_config = self._build_run_config(run, inputs)
        except (NotFound, ConfigurationError, ValueError) as exc:
            return self._fail(run, f"{type(exc).__name__}: {exc}")

        workdir = Path(run_config.workdir)
        try:
            assessment = Runner(run_config).run()
        except Exception as exc:  # noqa: BLE001 - the worker must survive any engine failure
            return self._fail(run, f"engine error: {type(exc).__name__}: {exc}")
        finally:
            self._cleanup(workdir)

        return self._persist(run, inputs, assessment)

    # ------------------------------------------------------------- resolution

    def _resolve(self, run: AssessmentRun) -> ExecutionInputs:
        # The worker acts for the organisation that owns the run. This is the
        # documented infrastructure exception to "a scope comes from a
        # principal": the run row itself is the authority.
        scope = TenantScope(run.organization_id)
        project = self.uow.projects.get(scope, run.project_id)
        if project is None:
            raise NotFound("project", run.project_id)

        stored_policy: Optional[Policy] = None
        policy = EnginePolicy()
        if run.policy_id:
            stored_policy = self.uow.policies.get(scope, run.policy_id)
            if stored_policy is not None:
                policy = EnginePolicy.from_dict(dict(stored_policy.document))

        target: Optional[EnvironmentTarget] = None
        if run.target_id:
            target = self.uow.targets.get(scope, run.target_id)
            if target is None:
                raise NotFound("target", run.target_id)

        return ExecutionInputs(
            project=project,
            policy=policy,
            stored_policy=stored_policy,
            target=target,
            scope=scope,
        )

    def _build_run_config(self, run: AssessmentRun, inputs: ExecutionInputs) -> RunConfig:
        project = inputs.project
        layers = list(run.layers)

        project_path: Optional[Path] = None
        if Layer.CODE in layers:
            if not project.source.local_path:
                raise ConfigurationError(
                    "the code layer was requested but the project has no source path"
                )
            project_path = self.config.resolve_in_workspace(project.source.local_path)
            if not project_path.is_dir():
                raise ConfigurationError(f"source path does not exist: {project_path}")

        documents: List[Path] = []
        manifest: Optional[Path] = None
        if Layer.ARCHITECTURE in layers:
            documents = [
                self.config.resolve_in_workspace(document)
                for document in project.architecture_documents
            ]
            if project.architecture_manifest:
                manifest = self.config.resolve_in_workspace(project.architecture_manifest)

        authorization: Optional[Authorization] = None
        target_url: Optional[str] = None
        if Layer.ENVIRONMENT in layers:
            if inputs.target is None:
                raise ConfigurationError(
                    "the environment layer was requested but the run has no target"
                )
            authorization = Authorization.from_dict(inputs.target.authorization_dict())
            target_url = inputs.target.url

        settings: Dict[str, Dict[str, Any]] = {}
        if self.config.ai_enabled:
            settings["ai-advisory"] = {"enabled": True}
            if self.config.ai_model:
                settings["ai-advisory"]["model"] = self.config.ai_model

        return RunConfig(
            project_path=project_path,
            architecture_docs=documents,
            architecture_manifest_path=manifest,
            target_url=target_url,
            layers=layers,
            authorization=authorization,
            policy=inputs.policy,
            workdir=Path(self.config.worker_workdir).resolve() / run.id,
            tool_path=ToolPath.from_env(self.config.scanner_tool_path),
            timeout=self.config.scanner_timeout_seconds,
            settings=settings,
            offline=self.config.offline,
            project_name=project.name,
        )

    # ------------------------------------------------------------ persistence

    def _persist(
        self, run: AssessmentRun, inputs: ExecutionInputs, assessment: Assessment
    ) -> AssessmentRun:
        scope = inputs.scope
        summary = assessment.to_dict()["summary"]

        self.uow.findings.add_all(
            scope, [self._finding(run, finding) for finding in assessment.findings]
        )
        self.uow.run_details.add_scanner_runs(
            scope, [self._scanner_run(run, scanner_run) for scanner_run in assessment.runs]
        )
        self.uow.run_details.add_coverage(
            scope, [self._coverage(run, entry) for entry in assessment.coverage]
        )
        for report in self._render_reports(run, inputs, assessment):
            self.uow.reports.add(scope, report)

        run.status = RunStatus.SUCCEEDED
        run.verdict = assessment.verdict
        run.summary = summary
        run.verdict_reasons = list(assessment.verdict_reasons)
        run.engine_assessment_id = assessment.assessment_id
        run.git_commit = assessment.target.git_commit
        run.git_branch = assessment.target.git_branch
        if inputs.stored_policy is None:
            # No stored policy: the engine default judged this run, so record its name.
            run.policy_name = assessment.policy_name
        run.finished_at = utc_now()
        run.error = None
        return self.uow.runs.complete(run)

    def _render_reports(
        self, run: AssessmentRun, inputs: ExecutionInputs, assessment: Assessment
    ) -> List[ReportArtifact]:
        artifacts: List[ReportArtifact] = []
        for fmt in self.config.report_formats:
            renderer, content_type = _RENDERERS[fmt]
            content = renderer(assessment)
            if len(content.encode("utf-8")) > self.config.max_report_bytes:
                content = (
                    content[: self.config.max_report_bytes]
                    + "\n\n[report truncated: exceeded max_report_bytes]"
                )
            artifacts.append(
                ReportArtifact(
                    organization_id=run.organization_id,
                    project_id=run.project_id,
                    run_id=run.id,
                    format=fmt,
                    content_type=content_type,
                    size_bytes=len(content.encode("utf-8")),
                    content=content,
                )
            )
        return artifacts

    @staticmethod
    def _finding(run: AssessmentRun, finding) -> FindingRecord:
        return FindingRecord(
            organization_id=run.organization_id,
            project_id=run.project_id,
            run_id=run.id,
            engine_id=finding.id,
            fingerprint=finding.fingerprint,
            source=finding.source,
            layer=finding.layer,
            severity=finding.severity,
            title=finding.title,
            explanation=finding.explanation,
            evidence=finding.evidence,
            location=finding.location,
            remediation=finding.remediation,
            blocking=finding.blocking,
            detected_at=finding.timestamp,
            rule_id=finding.rule_id,
            references=list(finding.references),
            tags=list(finding.tags),
            cwe=list(finding.cwe),
            confidence=finding.confidence,
            ai_generated=finding.ai_generated,
            suppressed=finding.suppressed,
            suppression_reason=finding.suppression_reason,
        )

    @staticmethod
    def _scanner_run(run: AssessmentRun, scanner_run) -> ScannerRunRecord:
        return ScannerRunRecord(
            organization_id=run.organization_id,
            run_id=run.id,
            name=scanner_run.name,
            layer=scanner_run.layer,
            status=scanner_run.status.value,
            capabilities=list(scanner_run.capabilities),
            deterministic=scanner_run.deterministic,
            tool=scanner_run.tool,
            tool_version=scanner_run.tool_version,
            command=scanner_run.command,
            duration_seconds=scanner_run.duration_seconds,
            findings_count=scanner_run.findings_count,
            message=scanner_run.message,
        )

    @staticmethod
    def _coverage(run: AssessmentRun, entry) -> CoverageRecord:
        return CoverageRecord(
            organization_id=run.organization_id,
            run_id=run.id,
            layer=entry.layer,
            capability=entry.capability,
            required=entry.required,
            satisfied=entry.satisfied,
            satisfied_by=list(entry.satisfied_by),
        )

    # ---------------------------------------------------------------- failure

    def _fail(self, run: AssessmentRun, message: str) -> AssessmentRun:
        run.status = RunStatus.FAILED
        run.error = message[:4000]
        run.finished_at = utc_now()
        return self.uow.runs.complete(run)

    def _cleanup(self, workdir: Path) -> None:
        """Remove scanner scratch files; reports are already in the database."""
        try:
            if workdir.exists():
                shutil.rmtree(workdir, ignore_errors=True)
        except OSError:
            pass
