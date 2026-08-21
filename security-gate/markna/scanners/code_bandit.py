"""Bandit adapter — Python-specific static analysis.

Bandit runs alongside Semgrep rather than instead of it: it is offline by
default, needs no rule downloads, and is a useful second opinion on Python code.
Overlapping findings from two engines are kept (each carries its own source), so
a reviewer can see when two independent tools agree.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from ..exec import probe_version
from ..models import Finding, Layer, Location, Severity
from ..redact import clean_evidence
from .base import Scanner, ScannerContext, register
from .util import DEFAULT_EXCLUDES, load_json, read_snippet, relative_path

_SEVERITY_MAP = {
    "HIGH": Severity.HIGH,
    "MEDIUM": Severity.MEDIUM,
    "LOW": Severity.LOW,
    "UNDEFINED": Severity.INFO,
}


@register
class BanditScanner(Scanner):
    name = "bandit"
    layer = Layer.CODE
    capabilities = ("sast",)
    description = "Bandit static analysis for Python source (offline, no rule downloads)."
    requires_executable = ("bandit",)
    install_hint = "Install with: pip install bandit"

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        if not any(ctx.project_path.rglob("*.py")):
            return False, "no Python source files in the project"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        return probe_version("bandit", tool_path=ctx.tool_path)

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        excludes = ctx.setting(self.name, "exclude", list(DEFAULT_EXCLUDES))
        command = [
            "bandit",
            "-r",
            str(project),
            "-f",
            "json",
            "-q",
            "-x",
            ",".join(f"*/{name}/*" for name in excludes),
        ]
        confidence = ctx.setting(self.name, "min_confidence")
        if confidence:
            command += ["--confidence-level", str(confidence)]

        result = self.exec(ctx, command, cwd=project)
        # 0 = no issues, 1 = issues found; 2 means bandit itself failed.
        if result.returncode not in (0, 1):
            raise RuntimeError(f"bandit failed: {result.failure_message()}")

        data = load_json(result.stdout)
        findings = [self._to_finding(item, project) for item in data.get("results", []) or []]
        for error in data.get("errors", []) or []:
            findings.append(
                Finding(
                    source=self.name,
                    layer=Layer.CODE,
                    severity=Severity.LOW,
                    title="Bandit could not analyse part of the source tree",
                    explanation=(
                        "Bandit reported an error for one or more files, which were therefore "
                        "not analysed."
                    ),
                    evidence=clean_evidence(str(error)),
                    location=Location(file=relative_path(error.get("filename"), project)),
                    remediation="Fix the syntax error or exclude the file deliberately.",
                    rule_id="bandit/scan-error",
                    tags=["scanner-warning"],
                )
            )
        return findings

    def _to_finding(self, item: Dict[str, Any], project: Path) -> Finding:
        rel_path = relative_path(item.get("filename"), project)
        line = item.get("line_number")
        line_range = item.get("line_range") or []
        end_line = max(line_range) if line_range else line
        cwe_id = (item.get("issue_cwe") or {}).get("id")

        severity = _SEVERITY_MAP.get(str(item.get("issue_severity", "")).upper(), Severity.LOW)
        confidence = str(item.get("issue_confidence", "MEDIUM")).lower()
        # A high-severity finding the tool is not confident about is not a
        # release blocker on its own; drop it one level.
        if severity is Severity.HIGH and confidence == "low":
            severity = Severity.MEDIUM

        absolute = project / rel_path if rel_path else None
        evidence = read_snippet(absolute, line, end_line, root=project) or ""

        return Finding(
            source=self.name,
            layer=Layer.CODE,
            severity=severity,
            title=f"{item.get('test_id', 'B000')}: {item.get('test_name', 'bandit finding')}",
            explanation=str(item.get("issue_text", "")).strip(),
            evidence=clean_evidence(evidence),
            location=Location(file=rel_path, line=line, end_line=end_line),
            remediation=(
                "Follow the Bandit guidance for this test: "
                f"{item.get('more_info', 'https://bandit.readthedocs.io/')}"
            ),
            rule_id=str(item.get("test_id", "bandit")),
            references=[str(item["more_info"])] if item.get("more_info") else [],
            cwe=[f"CWE-{cwe_id}"] if cwe_id else [],
            tags=["sast"],
            confidence=confidence,
            raw=item,
        )
