"""Assessment orchestration.

Decides which scanners run, runs them, records what happened to each one, and
hands the result to the policy engine. A scanner that fails never takes the gate
down: the failure becomes part of the evidence record and, through the coverage
rules, part of the verdict.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from .authorization import Authorization, AuthorizationError, Scope, ScopeError
from .confinement import confine_findings, escaping_symlink_finding, find_escaping_symlinks
from .exec import ToolPath, run_command
from .http import HttpClient
from .models import (
    Assessment,
    Layer,
    RunStatus,
    ScannerRun,
    Target,
    assign_ids,
    utc_now,
)
from .policy import Policy, cap_findings, load_structured_file
from .scanners.base import Scanner, ScannerContext, all_scanners

ProgressCallback = Callable[[str, str], None]


class ConfigurationError(ValueError):
    """Raised when the requested assessment cannot be set up."""


@dataclass
class RunConfig:
    """Everything the runner needs, resolved from the CLI or a config file."""

    project_path: Optional[Path] = None
    architecture_docs: List[Path] = field(default_factory=list)
    architecture_manifest_path: Optional[Path] = None
    target_url: Optional[str] = None
    layers: List[Layer] = field(default_factory=list)
    #: Every layer the target has inputs for. Defaults to `layers`; when the
    #: caller knows more (the server does), the difference is reported as an
    #: unassessed-layer finding rather than passing silently.
    available_layers: List[Layer] = field(default_factory=list)
    authorization: Optional[Authorization] = None
    policy: Policy = field(default_factory=Policy)
    workdir: Path = Path(".markna")
    tool_path: ToolPath = field(default_factory=ToolPath)
    timeout: int = 900
    settings: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    offline: bool = False
    verbose: bool = False
    only_scanners: List[str] = field(default_factory=list)
    skip_scanners: List[str] = field(default_factory=list)
    http_rate_limit: float = 0.2
    project_name: Optional[str] = None


class Runner:
    def __init__(self, config: RunConfig, progress: Optional[ProgressCallback] = None) -> None:
        self.config = config
        self.progress = progress or (lambda event, message: None)

    # --------------------------------------------------------------- entry point

    def run(self) -> Assessment:
        from . import __version__

        config = self.config
        if not config.layers:
            raise ConfigurationError("no layers selected; nothing to assess")

        context = self._build_context()
        target = self._build_target()
        assessment = Assessment(
            assessment_id=_assessment_id(),
            target=target,
            policy_name=config.policy.name,
            started_at=utc_now(),
            layers_requested=list(config.layers),
            layers_available=list(config.available_layers or config.layers),
            tool_version=__version__,
        )

        assessment.findings.extend(self._confinement_findings())

        deterministic, cross_layer = self._select_scanners()
        for scanner in deterministic:
            run, findings = self._execute(scanner, context)
            assessment.runs.append(run)
            assessment.findings.extend(findings)

        if cross_layer:
            # The AI layer cites deterministic findings by id, so ids must exist
            # (and be unique) before it runs. The policy engine re-assigns them
            # after the coverage findings are added; both passes agree.
            context.prior_findings = assign_ids(assessment.findings)
            for scanner in cross_layer:
                run, findings = self._execute(scanner, context)
                assessment.runs.append(run)
                assessment.findings.extend(findings)

        assessment.finished_at = utc_now()
        config.policy.apply(assessment)
        self.progress("verdict", f"{assessment.verdict.value}")
        return assessment

    def _confinement_findings(self) -> List[Any]:
        """Report symlinks that leave the project before anything reads them."""
        if Layer.CODE not in self.config.layers or not self.config.project_path:
            return []
        escaping = find_escaping_symlinks(self.config.project_path)
        if not escaping:
            return []
        self.progress(
            "warn", f"repository contains {len(escaping)} symlink(s) leaving the project root"
        )
        return [escaping_symlink_finding(self.config.project_path, escaping)]

    # ------------------------------------------------------------------ context

    def _build_context(self) -> ScannerContext:
        config = self.config
        workdir = config.workdir
        workdir.mkdir(parents=True, exist_ok=True)

        manifest: Optional[Dict[str, Any]] = None
        if config.architecture_manifest_path:
            path = config.architecture_manifest_path
            if not path.is_file():
                raise ConfigurationError(f"architecture manifest not found: {path}")
            loaded = load_structured_file(path)
            if not isinstance(loaded, dict):
                raise ConfigurationError(
                    f"architecture manifest must be a mapping at the top level: {path}"
                )
            manifest = loaded

        for document in config.architecture_docs:
            if not document.is_file():
                raise ConfigurationError(f"architecture document not found: {document}")

        scope: Optional[Scope] = None
        http: Optional[HttpClient] = None
        if Layer.ENVIRONMENT in config.layers and config.target_url:
            if config.authorization is None:
                raise ConfigurationError(
                    "environment testing requires --authorized-by (or an authorization block in "
                    "the config file): record who permitted this environment to be probed."
                )
            try:
                config.authorization.validate()
                scope = Scope.for_target(config.target_url, config.authorization)
            except (AuthorizationError, ScopeError) as exc:
                raise ConfigurationError(str(exc)) from exc
            http = HttpClient(scope, rate_limit_seconds=config.http_rate_limit)

        return ScannerContext(
            workdir=workdir,
            tool_path=config.tool_path,
            timeout=config.timeout,
            project_path=config.project_path,
            architecture_docs=list(config.architecture_docs),
            architecture_manifest=manifest,
            architecture_manifest_path=config.architecture_manifest_path,
            target_url=config.target_url,
            authorization=config.authorization,
            scope=scope,
            http=http,
            settings=config.settings,
            offline=config.offline,
            verbose=config.verbose,
        )

    def _build_target(self) -> Target:
        config = self.config
        commit = branch = None
        # `git rev-parse` works from any directory inside a work tree, so a
        # project that is a subdirectory of the repository is still traceable.
        if config.project_path:
            commit = self._git(["rev-parse", "HEAD"])
            branch = self._git(["rev-parse", "--abbrev-ref", "HEAD"])
        return Target(
            project_path=str(config.project_path) if config.project_path else None,
            architecture_docs=[str(path) for path in config.architecture_docs],
            architecture_manifest=(
                str(config.architecture_manifest_path)
                if config.architecture_manifest_path
                else None
            ),
            environment_url=config.target_url,
            name=config.project_name
            or (config.project_path.name if config.project_path else None),
            git_commit=commit,
            git_branch=branch,
        )

    def _git(self, args: Sequence[str]) -> Optional[str]:
        result = run_command(
            ["git", *args],
            cwd=self.config.project_path,
            timeout=30,
            tool_path=self.config.tool_path,
        )
        return result.stdout.strip() if result.ok and result.stdout.strip() else None

    # ----------------------------------------------------------------- scanners

    def _select_scanners(self) -> tuple:
        config = self.config
        deterministic: List[Scanner] = []
        cross_layer: List[Scanner] = []

        for scanner in all_scanners():
            if config.only_scanners and scanner.name not in config.only_scanners:
                continue
            if scanner.name in config.skip_scanners:
                continue
            if scanner.cross_layer:
                cross_layer.append(scanner)
            elif scanner.layer in config.layers:
                deterministic.append(scanner)

        unknown = set(config.only_scanners) - {s.name for s in all_scanners()}
        if unknown:
            raise ConfigurationError(
                f"unknown scanner name(s): {', '.join(sorted(unknown))}. "
                "Run `markna scanners` to list the registry."
            )
        return deterministic, cross_layer

    def _execute(self, scanner: Scanner, context: ScannerContext) -> tuple:
        run = ScannerRun(
            name=scanner.name,
            layer=scanner.layer,
            status=RunStatus.SKIPPED,
            capabilities=list(scanner.capabilities),
            deterministic=scanner.deterministic,
        )

        applicable, reason = scanner.applicable(context)
        if not applicable:
            run.message = reason
            self.progress("skip", f"{scanner.name}: {reason}")
            return run, []

        available, reason = scanner.available(context)
        if not available:
            run.status = RunStatus.UNAVAILABLE
            run.message = reason
            self.progress("unavailable", f"{scanner.name}: {reason}")
            return run, []

        run.tool = scanner.resolve_executable(context) or scanner.name
        self.progress("start", scanner.name)
        started = time.monotonic()
        context.commands = []
        try:
            run.tool_version = scanner.tool_version(context)
        except Exception:  # noqa: BLE001 - a version probe must never fail a run
            run.tool_version = None

        try:
            findings = list(scanner.scan(context))
            run.status = RunStatus.OK
        except ScopeError as exc:
            run.status = RunStatus.ERROR
            run.message = f"refused to leave the authorised scope: {exc}"
            findings = []
        except Exception as exc:  # noqa: BLE001 - isolate scanner failures
            run.status = RunStatus.ERROR
            run.message = f"{type(exc).__name__}: {exc}"
            findings = []

        run.duration_seconds = time.monotonic() - started
        run.command = "; ".join(context.commands) or None
        # A scanner may follow a symlink out of the repository. It does not get
        # to put what it found there into the report.
        if scanner.layer is Layer.CODE:
            findings = confine_findings(findings, self.config.project_path)
        findings = cap_findings(
            findings, self.config.policy.max_findings_per_scanner, scanner.name
        )
        run.findings_count = len(findings)

        if run.status is RunStatus.OK:
            self.progress(
                "done", f"{scanner.name}: {len(findings)} finding(s) in {run.duration_seconds:.1f}s"
            )
        else:
            self.progress("error", f"{scanner.name}: {run.message}")
        return run, findings


def _assessment_id() -> str:
    stamp = utc_now().replace("-", "").replace(":", "")
    return f"MKA-{stamp}-{uuid.uuid4().hex[:6].upper()}"
