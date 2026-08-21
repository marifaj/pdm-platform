"""Gate policy: what blocks a release, what merely warns, and what coverage is
required before the gate is allowed to say anything at all.

The policy engine is the only place that decides ``blocking``. Scanners report
facts; policy turns facts into a release decision. Two rules are structural
rather than configurable-by-accident:

* A coverage gap (a required deterministic scanner did not run) is itself a
  finding. A gate that skipped SAST cannot honestly return PASS.
* AI findings are advisory by default (``ai_findings_can_block: false``). An LLM
  opinion is not security evidence; it is a reasoning layer on top of it.
"""

from __future__ import annotations

import fnmatch
import json
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from .models import (
    Assessment,
    CoverageEntry,
    Finding,
    Layer,
    Location,
    RunStatus,
    Severity,
    Verdict,
    assign_ids,
    sort_findings,
)

DEFAULT_POLICY_NAME = "markna-default-v1"

#: Deterministic capabilities that must be exercised for each layer before the
#: gate can vouch for that layer. Keyed by layer, values are capability names
#: declared by scanners (see :attr:`markna.scanners.base.Scanner.capabilities`).
DEFAULT_REQUIRED_CAPABILITIES: Dict[str, List[str]] = {
    "architecture": ["architecture-review"],
    "code": ["sast", "dependency-vulnerabilities", "secrets", "sbom"],
    "environment": ["transport-security", "http-security-headers"],
}


@dataclass
class RuleMatch:
    """Predicate used by overrides and suppressions."""

    fingerprint: Optional[str] = None
    id: Optional[str] = None
    source: Optional[str] = None
    rule_id: Optional[str] = None
    layer: Optional[str] = None
    severity: Optional[str] = None
    title_regex: Optional[str] = None
    path_glob: Optional[str] = None

    def matches(self, finding: Finding) -> bool:
        if self.fingerprint and finding.fingerprint != self.fingerprint:
            return False
        if self.id and finding.id != self.id:
            return False
        if self.source and not fnmatch.fnmatch(finding.source, self.source):
            return False
        if self.rule_id and not fnmatch.fnmatch(finding.rule_id or "", self.rule_id):
            return False
        if self.layer and finding.layer.value != self.layer:
            return False
        if self.severity and finding.severity.value != self.severity:
            return False
        if self.title_regex and not re.search(self.title_regex, finding.title, re.I):
            return False
        if self.path_glob:
            path = finding.location.file or finding.location.url or ""
            if not fnmatch.fnmatch(path, self.path_glob):
                return False
        # An empty predicate must never match everything by accident.
        return any(
            value is not None
            for value in (
                self.fingerprint,
                self.id,
                self.source,
                self.rule_id,
                self.layer,
                self.severity,
                self.title_regex,
                self.path_glob,
            )
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuleMatch":
        known = {f for f in cls.__dataclass_fields__}  # noqa: F821 - dataclass attr
        unknown = set(data) - known
        if unknown:
            raise PolicyError(f"unknown match key(s): {', '.join(sorted(unknown))}")
        return cls(**data)


@dataclass
class SeverityOverride:
    match: RuleMatch
    severity: Severity
    reason: str = ""


@dataclass
class Suppression:
    match: RuleMatch
    reason: str
    expires: Optional[str] = None  # YYYY-MM-DD; an expired suppression is ignored

    def active(self, today: Optional[date] = None) -> bool:
        if not self.expires:
            return True
        today = today or date.today()
        try:
            expiry = date.fromisoformat(str(self.expires))
        except ValueError as exc:
            raise PolicyError(f"invalid suppression expiry {self.expires!r}: {exc}") from exc
        return expiry >= today


class PolicyError(ValueError):
    """Raised when a policy file is malformed."""


@dataclass
class Policy:
    name: str = DEFAULT_POLICY_NAME
    description: str = "MARKNA default pre-UAT security gate policy."
    block_on: List[Severity] = field(
        default_factory=lambda: [Severity.CRITICAL, Severity.HIGH]
    )
    warn_on: List[Severity] = field(
        default_factory=lambda: [Severity.MEDIUM, Severity.LOW]
    )
    ai_findings_can_block: bool = False
    required_capabilities: Dict[str, List[str]] = field(
        default_factory=lambda: {k: list(v) for k, v in DEFAULT_REQUIRED_CAPABILITIES.items()}
    )
    coverage_gap_severity: Severity = Severity.HIGH
    scanner_error_severity: Severity = Severity.MEDIUM
    max_findings_per_scanner: int = 1000
    severity_overrides: List[SeverityOverride] = field(default_factory=list)
    suppressions: List[Suppression] = field(default_factory=list)

    # ------------------------------------------------------------------ load

    @classmethod
    def load(cls, path: Optional[str]) -> "Policy":
        if not path:
            return cls()
        file_path = Path(path)
        if not file_path.is_file():
            raise PolicyError(f"policy file not found: {path}")
        raw = file_path.read_text(encoding="utf-8")
        data = _parse_structured(raw, file_path.suffix)
        if not isinstance(data, dict):
            raise PolicyError(f"policy file {path} must contain a mapping")
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Policy":
        policy = cls()
        known = {
            "name",
            "description",
            "block_on",
            "warn_on",
            "ai_findings_can_block",
            "required_capabilities",
            "coverage_gap_severity",
            "scanner_error_severity",
            "max_findings_per_scanner",
            "severity_overrides",
            "suppressions",
        }
        unknown = set(data) - known
        if unknown:
            raise PolicyError(f"unknown policy key(s): {', '.join(sorted(unknown))}")

        policy.name = str(data.get("name", policy.name))
        policy.description = str(data.get("description", policy.description))
        if "block_on" in data:
            policy.block_on = _severity_list(data["block_on"], "block_on")
        if "warn_on" in data:
            policy.warn_on = _severity_list(data["warn_on"], "warn_on")
        if "ai_findings_can_block" in data:
            policy.ai_findings_can_block = bool(data["ai_findings_can_block"])
        if "required_capabilities" in data:
            required = data["required_capabilities"] or {}
            if not isinstance(required, dict):
                raise PolicyError("required_capabilities must be a mapping of layer -> list")
            for layer_name, caps in required.items():
                if layer_name not in {layer.value for layer in Layer}:
                    raise PolicyError(f"unknown layer in required_capabilities: {layer_name}")
                if not isinstance(caps, (list, tuple)):
                    raise PolicyError(f"required_capabilities.{layer_name} must be a list")
            policy.required_capabilities = {k: list(v) for k, v in required.items()}
        if "coverage_gap_severity" in data:
            policy.coverage_gap_severity = Severity.parse(data["coverage_gap_severity"])
        if "scanner_error_severity" in data:
            policy.scanner_error_severity = Severity.parse(data["scanner_error_severity"])
        if "max_findings_per_scanner" in data:
            policy.max_findings_per_scanner = int(data["max_findings_per_scanner"])

        policy.severity_overrides = [
            SeverityOverride(
                match=RuleMatch.from_dict(dict(entry.get("match", {}))),
                severity=Severity.parse(entry.get("severity")),
                reason=str(entry.get("reason", "")),
            )
            for entry in data.get("severity_overrides", []) or []
        ]
        policy.suppressions = [
            Suppression(
                match=RuleMatch.from_dict(dict(entry.get("match", {}))),
                reason=str(entry.get("reason", "no reason recorded")),
                expires=entry.get("expires"),
            )
            for entry in data.get("suppressions", []) or []
        ]
        return policy

    # ------------------------------------------------------------------ apply

    def apply(self, assessment: Assessment) -> Assessment:
        """Score an assessment: overrides, suppressions, coverage, verdict."""
        assign_ids(assessment.findings)
        for finding in assessment.findings:
            self._apply_overrides(finding)
            self._apply_suppressions(finding)
            finding.blocking = self._is_blocking(finding)

        assessment.coverage = self._evaluate_coverage(assessment)
        assessment.findings.extend(self._coverage_findings(assessment))
        assessment.findings.extend(self._scanner_error_findings(assessment))

        assign_ids(assessment.findings)
        for finding in assessment.findings:
            if not finding.suppressed:
                finding.blocking = self._is_blocking(finding)

        assessment.findings = sort_findings(assessment.findings)
        assessment.verdict, assessment.verdict_reasons = self._verdict(assessment)
        assessment.policy_name = self.name
        return assessment

    # ------------------------------------------------------------- internals

    def _apply_overrides(self, finding: Finding) -> None:
        for override in self.severity_overrides:
            if override.match.matches(finding):
                if finding.severity is not override.severity:
                    note = f"severity {finding.severity.value} -> {override.severity.value} by policy"
                    if override.reason:
                        note = f"{note} ({override.reason})"
                    finding.tags.append("severity-overridden")
                    finding.explanation = f"{finding.explanation}\n\n[policy] {note}".strip()
                    finding.severity = override.severity
                return

    def _apply_suppressions(self, finding: Finding) -> None:
        for suppression in self.suppressions:
            if not suppression.match.matches(finding):
                continue
            if not suppression.active():
                finding.tags.append("suppression-expired")
                continue
            finding.suppressed = True
            finding.suppression_reason = suppression.reason
            finding.blocking = False
            return

    def _is_blocking(self, finding: Finding) -> bool:
        if finding.suppressed:
            return False
        if finding.ai_generated and not self.ai_findings_can_block:
            return False
        return finding.severity in self.block_on

    def _evaluate_coverage(self, assessment: Assessment) -> List[CoverageEntry]:
        entries: List[CoverageEntry] = []
        for layer in assessment.layers_requested:
            required = self.required_capabilities.get(layer.value, [])
            for capability in required:
                providers = [
                    run.name
                    for run in assessment.runs
                    if run.layer is layer
                    and run.succeeded
                    and run.deterministic
                    and capability in run.capabilities
                ]
                entries.append(
                    CoverageEntry(
                        layer=layer,
                        capability=capability,
                        satisfied=bool(providers),
                        satisfied_by=providers,
                        required=True,
                    )
                )
        return entries

    def _coverage_findings(self, assessment: Assessment) -> List[Finding]:
        findings: List[Finding] = []
        for entry in assessment.coverage:
            if entry.satisfied:
                continue
            attempts = [
                run
                for run in assessment.runs
                if run.layer is entry.layer and entry.capability in run.capabilities
            ]
            detail = (
                "; ".join(f"{run.name}: {run.status.value} ({run.message or 'no detail'})" for run in attempts)
                or "no scanner providing this capability was registered for the run"
            )
            findings.append(
                Finding(
                    source="markna:coverage",
                    layer=entry.layer,
                    severity=self.coverage_gap_severity,
                    title=f"No deterministic {entry.capability} evidence for the {entry.layer.value} layer",
                    explanation=(
                        f"Policy '{self.name}' requires the '{entry.capability}' capability to be "
                        f"exercised by a deterministic scanner on the {entry.layer.value} layer, but no "
                        "such scanner completed successfully. The gate therefore has no evidence about "
                        "this class of risk. An absent result is not a clean result."
                    ),
                    evidence=detail,
                    location=Location(component=f"{entry.layer.value}/{entry.capability}"),
                    remediation=(
                        "Install or enable a scanner providing this capability (see "
                        "'markna scanners' for the registry and installation hints), or relax the "
                        "requirement explicitly in the policy file so the gap is a recorded decision."
                    ),
                    rule_id=f"coverage/{entry.layer.value}/{entry.capability}",
                    tags=["coverage-gap"],
                )
            )
        return findings

    def _scanner_error_findings(self, assessment: Assessment) -> List[Finding]:
        findings: List[Finding] = []
        for run in assessment.runs:
            if run.status not in (RunStatus.ERROR, RunStatus.TIMEOUT):
                continue
            findings.append(
                Finding(
                    source="markna:runner",
                    layer=run.layer,
                    severity=self.scanner_error_severity,
                    title=f"Scanner '{run.name}' did not complete ({run.status.value})",
                    explanation=(
                        f"The {run.name} scanner failed during the assessment, so its results are "
                        "missing from this report. Findings it would have produced are unknown."
                    ),
                    evidence=(run.message or "no error detail captured")[:4000],
                    location=Location(component=run.name),
                    remediation=(
                        "Re-run the gate after fixing the scanner invocation. Check the command "
                        "recorded in scanner_runs[] of the JSON report."
                    ),
                    rule_id=f"runner/{run.status.value}",
                    tags=["scanner-error"],
                )
            )
        return findings

    def _verdict(self, assessment: Assessment) -> tuple:
        active = assessment.active_findings
        blocking = [f for f in active if f.blocking]
        reasons: List[str] = []

        if blocking:
            by_sev: Dict[str, int] = {}
            for finding in blocking:
                by_sev[finding.severity.value] = by_sev.get(finding.severity.value, 0) + 1
            summary = ", ".join(f"{count} {sev}" for sev, count in sorted(by_sev.items()))
            reasons.append(f"{len(blocking)} blocking finding(s): {summary}.")
            reasons.append(f"Blocking severities under policy '{self.name}': "
                           f"{', '.join(s.value for s in self.block_on)}.")
            return Verdict.BLOCK, reasons

        warn_severities = set(self.warn_on) | set(self.block_on)
        warning = [f for f in active if f.severity in warn_severities]
        if warning:
            advisory = [f for f in warning if f.ai_generated]
            reasons.append(f"{len(warning)} non-blocking finding(s) at or above warn threshold.")
            if advisory:
                reasons.append(
                    f"{len(advisory)} of them are AI advisory findings, which cannot block under "
                    "this policy and require human triage."
                )
            return Verdict.WARN, reasons

        reasons.append("No findings at or above the warn threshold.")
        satisfied = [entry for entry in assessment.coverage if entry.satisfied]
        reasons.append(
            f"{len(satisfied)}/{len(assessment.coverage)} required deterministic capabilities were "
            "covered."
        )
        return Verdict.PASS, reasons

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "block_on": [s.value for s in self.block_on],
            "warn_on": [s.value for s in self.warn_on],
            "ai_findings_can_block": self.ai_findings_can_block,
            "required_capabilities": {k: list(v) for k, v in self.required_capabilities.items()},
            "coverage_gap_severity": self.coverage_gap_severity.value,
            "scanner_error_severity": self.scanner_error_severity.value,
            "max_findings_per_scanner": self.max_findings_per_scanner,
        }


def _severity_list(value: Any, key: str) -> List[Severity]:
    if not isinstance(value, (list, tuple)):
        raise PolicyError(f"{key} must be a list of severities")
    return [Severity.parse(item) for item in value]


def _parse_structured(raw: str, suffix: str) -> Any:
    """Parse YAML when available, otherwise JSON. Policy files may be either."""
    if suffix.lower() in (".json",):
        return json.loads(raw)
    try:
        import yaml  # type: ignore
    except ImportError:  # pragma: no cover - depends on environment
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PolicyError(
                "PyYAML is not installed, so only JSON policy files can be read. "
                "Install PyYAML or supply a .json policy."
            ) from exc
    try:
        return yaml.safe_load(raw)
    except yaml.YAMLError as exc:  # pragma: no cover - malformed input
        raise PolicyError(f"could not parse policy file: {exc}") from exc


def load_structured_file(path: Path) -> Any:
    """Shared YAML/JSON loader used for policies and architecture manifests."""
    return _parse_structured(path.read_text(encoding="utf-8"), path.suffix)


def cap_findings(findings: Sequence[Finding], limit: int, scanner: str) -> List[Finding]:
    """Truncate a scanner's output, recording the truncation as a finding."""
    findings = list(findings)
    if limit <= 0 or len(findings) <= limit:
        return findings
    kept = sort_findings(findings)[:limit]
    dropped = len(findings) - len(kept)
    kept.append(
        Finding(
            source="markna:runner",
            layer=kept[0].layer if kept else Layer.CODE,
            severity=Severity.MEDIUM,
            title=f"Scanner '{scanner}' output truncated ({dropped} findings dropped)",
            explanation=(
                f"{scanner} produced {len(findings)} findings, above the policy limit of {limit}. "
                "The lowest-severity findings were dropped from this report; the report is therefore "
                "not a complete list."
            ),
            evidence=f"reported={len(findings)} kept={limit} dropped={dropped}",
            location=Location(component=scanner),
            remediation=(
                "Raise max_findings_per_scanner in the policy, or narrow the scan scope so the "
                "results are actionable."
            ),
            rule_id="runner/truncated",
            tags=["truncated"],
        )
    )
    return kept
