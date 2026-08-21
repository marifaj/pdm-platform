"""Infrastructure and deployment configuration analysis (Checkov).

Checkov covers Terraform, CloudFormation, Kubernetes, Helm, Dockerfiles,
docker-compose, GitHub Actions and more. It only runs when the project actually
contains files it understands, so a pure application repository does not pay for
an empty IaC scan.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..exec import probe_version
from ..models import Finding, Layer, Location, Severity
from ..redact import clean_evidence
from .base import Scanner, ScannerContext, register
from .util import DEFAULT_EXCLUDES, find_files, load_json_file, read_snippet, relative_path

#: File patterns that indicate there is infrastructure code worth scanning.
_IAC_PATTERNS = (
    "*.tf",
    "*.tf.json",
    "Dockerfile",
    "Dockerfile.*",
    "*.dockerfile",
    "docker-compose*.yml",
    "docker-compose*.yaml",
    "*.k8s.yaml",
    "Chart.yaml",
    "serverless.yml",
    "template.yaml",
    "template.yml",
    ".github/workflows/*.yml",
    ".github/workflows/*.yaml",
)

_SEVERITY_MAP = {
    "CRITICAL": Severity.CRITICAL,
    "HIGH": Severity.HIGH,
    "MEDIUM": Severity.MEDIUM,
    "LOW": Severity.LOW,
    "INFO": Severity.INFO,
}


@register
class CheckovScanner(Scanner):
    name = "checkov"
    layer = Layer.CODE
    capabilities = ("insecure-configuration",)
    description = "Checkov policy-as-code analysis of IaC, Dockerfiles and CI workflows."
    requires_executable = ("checkov",)
    install_hint = (
        "Install with: pipx install checkov (use a separate environment — checkov pins "
        "dependency versions that clash with cyclonedx-bom)"
    )

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        if not find_files(ctx.project_path, _IAC_PATTERNS, limit=1):
            return False, "no infrastructure-as-code or container files found"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        return probe_version("checkov", tool_path=ctx.tool_path)

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        output_dir = ctx.scanner_workdir(self.name)
        report = output_dir / "results_json.json"

        command = [
            "checkov",
            "--directory",
            str(project),
            "--output",
            "json",
            "--output-file-path",
            str(output_dir),
            "--compact",
            "--quiet",
            "--soft-fail",
        ]
        for exclude in ctx.setting(self.name, "exclude", list(DEFAULT_EXCLUDES)):
            command += ["--skip-path", str(exclude)]

        result = self.exec(ctx, command, cwd=project)
        if not report.exists():
            raise RuntimeError(f"checkov failed: {result.failure_message()}")

        data = load_json_file(report)
        reports = data if isinstance(data, list) else [data]
        findings: List[Finding] = []
        for entry in reports:
            if not isinstance(entry, dict):
                continue
            check_type = str(entry.get("check_type", "iac"))
            results = entry.get("results") or {}
            for check in results.get("failed_checks", []) or []:
                findings.append(self._to_finding(check, check_type, project))
        return findings

    def _to_finding(self, check: Dict[str, Any], check_type: str, project: Path) -> Finding:
        rel_path = relative_path(check.get("file_path"), project)
        line_range = check.get("file_line_range") or []
        start_line = line_range[0] if line_range else None
        end_line = line_range[1] if len(line_range) > 1 else start_line
        severity = _SEVERITY_MAP.get(
            str(check.get("severity") or "").upper(), Severity.MEDIUM
        )
        absolute = project / rel_path if rel_path else None
        guideline = check.get("guideline")

        return Finding(
            source=self.name,
            layer=Layer.CODE,
            severity=severity,
            title=f"Insecure configuration: {check.get('check_name', check.get('check_id'))}",
            explanation=(
                f"Checkov policy {check.get('check_id')} failed for {check_type} resource "
                f"'{check.get('resource', 'unknown')}'. "
                + (f"See {guideline}" if guideline else "")
            ).strip(),
            evidence=clean_evidence(
                read_snippet(absolute, start_line, end_line)
                or str(check.get("code_block", ""))[:2000]
            ),
            location=Location(
                file=rel_path,
                line=start_line,
                end_line=end_line,
                component=check.get("resource"),
            ),
            remediation=(
                str(guideline)
                if guideline
                else "Apply the hardening the failed policy describes to this resource."
            ),
            rule_id=str(check.get("check_id", "checkov")),
            references=[str(guideline)] if guideline else [],
            tags=["configuration", "iac", f"framework:{check_type}"],
            raw={k: v for k, v in check.items() if k != "code_block"},
        )
