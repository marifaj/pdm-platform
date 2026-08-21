"""Core data model for MARKNA Security Gate.

Everything a scanner produces is normalised into a :class:`Finding`. Everything a
run produces is normalised into an :class:`Assessment`, which carries the gate
verdict (PASS / WARN / BLOCK), the findings, and — just as importantly — the
record of which scanners actually ran. A finding list without a coverage record
is not evidence, so :class:`ScannerRun` is a first-class citizen of the report.
"""

from __future__ import annotations

import enum
import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

SCHEMA_VERSION = "1.0"


def utc_now() -> str:
    """Current UTC time as a second-precision ISO-8601 timestamp."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Severity(str, enum.Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"

    @classmethod
    def parse(cls, value: Any, default: "Severity" = None) -> "Severity":
        """Best-effort parse of the many severity spellings scanners emit."""
        default = default if default is not None else cls.INFO
        if isinstance(value, Severity):
            return value
        if value is None:
            return default
        token = str(value).strip().lower()
        direct = _SEVERITY_ALIASES.get(token)
        if direct is not None:
            return direct
        # Numeric CVSS-style scores.
        try:
            score = float(token)
        except ValueError:
            return default
        if score >= 9.0:
            return cls.CRITICAL
        if score >= 7.0:
            return cls.HIGH
        if score >= 4.0:
            return cls.MEDIUM
        if score > 0.0:
            return cls.LOW
        return cls.INFO

    @property
    def rank(self) -> int:
        """Higher is worse. Useful for sorting and threshold comparisons."""
        return _SEVERITY_RANK[self]


_SEVERITY_RANK: Dict[Severity, int] = {
    Severity.INFO: 0,
    Severity.LOW: 1,
    Severity.MEDIUM: 2,
    Severity.HIGH: 3,
    Severity.CRITICAL: 4,
}

_SEVERITY_ALIASES: Dict[str, Severity] = {
    "critical": Severity.CRITICAL,
    "crit": Severity.CRITICAL,
    "blocker": Severity.CRITICAL,
    "high": Severity.HIGH,
    "error": Severity.HIGH,
    "severe": Severity.HIGH,
    "important": Severity.HIGH,
    "moderate": Severity.MEDIUM,
    "medium": Severity.MEDIUM,
    "warning": Severity.MEDIUM,
    "warn": Severity.MEDIUM,
    "low": Severity.LOW,
    "minor": Severity.LOW,
    "note": Severity.LOW,
    "info": Severity.INFO,
    "informational": Severity.INFO,
    "unknown": Severity.INFO,
    "none": Severity.INFO,
}


class Layer(str, enum.Enum):
    ARCHITECTURE = "architecture"
    CODE = "code"
    ENVIRONMENT = "environment"

    @property
    def abbrev(self) -> str:
        return {"architecture": "ARC", "code": "COD", "environment": "ENV"}[self.value]


class Verdict(str, enum.Enum):
    PASS = "PASS"
    WARN = "WARN"
    BLOCK = "BLOCK"


class RunStatus(str, enum.Enum):
    OK = "ok"
    UNAVAILABLE = "unavailable"  # tool not installed / not reachable
    SKIPPED = "skipped"          # deliberately not run (no target, not authorised)
    ERROR = "error"              # tool ran and failed
    TIMEOUT = "timeout"


@dataclass
class Location:
    """Where a finding lives. Any subset of the fields may be populated."""

    file: Optional[str] = None
    line: Optional[int] = None
    end_line: Optional[int] = None
    url: Optional[str] = None
    component: Optional[str] = None

    def describe(self) -> str:
        if self.file:
            base = self.file
            if self.line:
                base = f"{base}:{self.line}"
                if self.end_line and self.end_line != self.line:
                    base = f"{base}-{self.end_line}"
            return base
        if self.url:
            return self.url
        if self.component:
            return self.component
        return "n/a"

    def is_empty(self) -> bool:
        return not any((self.file, self.url, self.component))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file": self.file,
            "line": self.line,
            "end_line": self.end_line,
            "url": self.url,
            "component": self.component,
            "display": self.describe(),
        }


@dataclass
class Finding:
    """A single normalised security observation.

    ``blocking`` is intentionally *not* set by scanners. Scanners report severity
    and facts; the policy engine (:mod:`markna.policy`) decides what blocks a
    release. ``id`` is assigned by :func:`Finding.finalise` from the fingerprint
    so the same issue keeps the same identifier between runs.
    """

    source: str
    layer: Layer
    severity: Severity
    title: str
    explanation: str
    evidence: str = ""
    location: Location = field(default_factory=Location)
    remediation: str = ""
    blocking: bool = False
    timestamp: str = field(default_factory=utc_now)

    # Supporting metadata (not required by the spec, but needed for real use).
    id: str = ""
    fingerprint: str = ""
    rule_id: Optional[str] = None
    tool_version: Optional[str] = None
    references: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    cwe: List[str] = field(default_factory=list)
    confidence: str = "high"          # high | medium | low
    ai_generated: bool = False        # advisory reasoning, never sole evidence
    suppressed: bool = False
    suppression_reason: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None

    def compute_fingerprint(self) -> str:
        """Stable identity for a finding across runs of the same scanner."""
        parts = [
            self.source,
            self.layer.value,
            self.rule_id or self.title,
            self.location.file or "",
            self.location.url or "",
            self.location.component or "",
            str(self.location.line or ""),
            _normalise_for_fingerprint(self.evidence)[:200],
        ]
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]

    def finalise(
        self, seen: Optional[Dict[str, int]] = None, *, force: bool = False
    ) -> "Finding":
        """Assign fingerprint and id.

        Idempotent unless ``force`` is set. ``seen`` carries the ids already
        handed out in this assessment so two findings that fingerprint
        identically still get distinct ids — see :func:`assign_ids`.
        """
        if not self.fingerprint:
            self.fingerprint = self.compute_fingerprint()
        if self.id and not force:
            return self
        base = f"MK-{self.layer.abbrev}-{self.fingerprint[:10].upper()}"
        if seen is not None:
            count = seen.get(base, 0)
            seen[base] = count + 1
            self.id = base if count == 0 else f"{base}-{count + 1}"
        else:
            self.id = base
        return self

    def to_dict(self, include_raw: bool = False) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "id": self.id,
            "source": self.source,
            "layer": self.layer.value,
            "severity": self.severity.value,
            "title": self.title,
            "explanation": self.explanation,
            "evidence": self.evidence,
            "location": self.location.to_dict(),
            "remediation": self.remediation,
            "blocking": self.blocking,
            "timestamp": self.timestamp,
            "fingerprint": self.fingerprint,
            "rule_id": self.rule_id,
            "tool_version": self.tool_version,
            "references": list(self.references),
            "tags": list(self.tags),
            "cwe": list(self.cwe),
            "confidence": self.confidence,
            "ai_generated": self.ai_generated,
            "suppressed": self.suppressed,
            "suppression_reason": self.suppression_reason,
        }
        if include_raw and self.raw is not None:
            data["raw"] = self.raw
        return data


def _normalise_for_fingerprint(text: str) -> str:
    """Collapse whitespace and strip volatile numbers so ids stay stable."""
    return re.sub(r"\s+", " ", (text or "")).strip()


@dataclass
class ScannerRun:
    """Evidence that a scanner ran (or the reason it did not).

    A missing scanner is a coverage gap, and a coverage gap is a security finding
    in its own right — see :mod:`markna.policy`.
    """

    name: str
    layer: Layer
    status: RunStatus
    capabilities: List[str] = field(default_factory=list)
    tool: Optional[str] = None
    tool_version: Optional[str] = None
    deterministic: bool = True
    command: Optional[str] = None
    started_at: str = field(default_factory=utc_now)
    duration_seconds: float = 0.0
    findings_count: int = 0
    message: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        return self.status is RunStatus.OK

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "layer": self.layer.value,
            "status": self.status.value,
            "capabilities": list(self.capabilities),
            "tool": self.tool,
            "tool_version": self.tool_version,
            "deterministic": self.deterministic,
            "command": self.command,
            "started_at": self.started_at,
            "duration_seconds": round(self.duration_seconds, 3),
            "findings_count": self.findings_count,
            "message": self.message,
        }


@dataclass
class Target:
    """What was assessed."""

    project_path: Optional[str] = None
    architecture_docs: List[str] = field(default_factory=list)
    architecture_manifest: Optional[str] = None
    environment_url: Optional[str] = None
    name: Optional[str] = None
    git_commit: Optional[str] = None
    git_branch: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "project_path": self.project_path,
            "architecture_docs": list(self.architecture_docs),
            "architecture_manifest": self.architecture_manifest,
            "environment_url": self.environment_url,
            "git_commit": self.git_commit,
            "git_branch": self.git_branch,
        }


@dataclass
class CoverageEntry:
    """Whether a required deterministic capability was actually exercised."""

    layer: Layer
    capability: str
    satisfied: bool
    satisfied_by: List[str] = field(default_factory=list)
    required: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "layer": self.layer.value,
            "capability": self.capability,
            "required": self.required,
            "satisfied": self.satisfied,
            "satisfied_by": list(self.satisfied_by),
        }


@dataclass
class Assessment:
    """The complete result of one gate run."""

    assessment_id: str
    target: Target
    policy_name: str
    started_at: str
    finished_at: str = ""
    verdict: Verdict = Verdict.PASS
    verdict_reasons: List[str] = field(default_factory=list)
    layers_requested: List[Layer] = field(default_factory=list)
    #: Layers the target had the inputs for, whether or not they were run. The
    #: difference against layers_requested is reported, never left implicit.
    layers_available: List[Layer] = field(default_factory=list)
    findings: List[Finding] = field(default_factory=list)
    runs: List[ScannerRun] = field(default_factory=list)
    coverage: List[CoverageEntry] = field(default_factory=list)
    tool_version: str = ""
    schema_version: str = SCHEMA_VERSION

    @property
    def active_findings(self) -> List[Finding]:
        return [f for f in self.findings if not f.suppressed]

    @property
    def blocking_findings(self) -> List[Finding]:
        return [f for f in self.active_findings if f.blocking]

    def counts_by_severity(self, findings: Optional[Iterable[Finding]] = None) -> Dict[str, int]:
        source = list(self.active_findings if findings is None else findings)
        counts = {s.value: 0 for s in Severity}
        for finding in source:
            counts[finding.severity.value] += 1
        return counts

    def to_dict(self, include_raw: bool = False) -> Dict[str, Any]:
        active = self.active_findings
        return {
            "schema_version": self.schema_version,
            "assessment_id": self.assessment_id,
            "tool": "markna-security-gate",
            "tool_version": self.tool_version,
            "policy": self.policy_name,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "target": self.target.to_dict(),
            "layers": [layer.value for layer in self.layers_requested],
            "layers_available": [layer.value for layer in self.layers_available],
            "layers_not_assessed": sorted(
                layer.value
                for layer in (set(self.layers_available) - set(self.layers_requested))
            ),
            "verdict": self.verdict.value,
            "verdict_reasons": list(self.verdict_reasons),
            "summary": {
                "total_findings": len(active),
                "suppressed_findings": len(self.findings) - len(active),
                "blocking_findings": len(self.blocking_findings),
                "by_severity": self.counts_by_severity(),
                "by_layer": {
                    layer.value: len([f for f in active if f.layer is layer])
                    for layer in Layer
                },
                "deterministic_findings": len([f for f in active if not f.ai_generated]),
                "advisory_ai_findings": len([f for f in active if f.ai_generated]),
                "scanners_ok": len([r for r in self.runs if r.succeeded]),
                "scanners_total": len(self.runs),
            },
            "coverage": [entry.to_dict() for entry in self.coverage],
            "scanner_runs": [run.to_dict() for run in self.runs],
            "findings": [f.to_dict(include_raw=include_raw) for f in self.findings],
        }


def assign_ids(findings: Iterable[Finding]) -> List[Finding]:
    """Assign every finding a unique id, in list order.

    Ids are derived from the fingerprint, so they are stable between runs; the
    numeric suffix only appears when two findings genuinely fingerprint the same.
    Re-running this over a longer list keeps the earlier ids unchanged.
    """
    seen: Dict[str, int] = {}
    findings = list(findings)
    for finding in findings:
        finding.finalise(seen, force=True)
    return findings


def sort_findings(findings: Iterable[Finding]) -> List[Finding]:
    """Worst first, then blocking, then stable by layer/source/id."""
    return sorted(
        findings,
        key=lambda f: (
            -f.severity.rank,
            not f.blocking,
            f.ai_generated,
            f.layer.value,
            f.source,
            f.id,
        ),
    )
