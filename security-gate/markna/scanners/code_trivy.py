"""Trivy adapter — dependencies, secrets and infrastructure misconfiguration.

Trivy covers three capabilities in a single filesystem scan, which makes it the
highest-value single tool to install on a gate runner. Its vulnerability database
is downloaded on first use, so it is skipped in offline mode unless a cached
database is present.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

from ..exec import probe_version
from ..models import Finding, Layer, Location, Severity
from ..redact import clean_evidence
from .base import Scanner, ScannerContext, register
from .util import DEFAULT_EXCLUDES, relative_path

_SEVERITY_MAP = {
    "CRITICAL": Severity.CRITICAL,
    "HIGH": Severity.HIGH,
    "MEDIUM": Severity.MEDIUM,
    "LOW": Severity.LOW,
    "UNKNOWN": Severity.INFO,
}


@register
class TrivyScanner(Scanner):
    name = "trivy"
    layer = Layer.CODE
    capabilities = ("dependency-vulnerabilities", "secrets", "insecure-configuration")
    description = "Trivy filesystem scan: dependency CVEs, secrets and IaC misconfiguration."
    requires_executable = ("trivy",)
    install_hint = "Install from https://github.com/aquasecurity/trivy/releases"

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        return probe_version("trivy", tool_path=ctx.tool_path)

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        output = ctx.scanner_workdir(self.name) / "trivy.json"
        scanners = ctx.setting(self.name, "scanners", ["vuln", "secret", "misconfig"])

        command = [
            "trivy",
            "filesystem",
            "--format",
            "json",
            "--output",
            str(output),
            "--scanners",
            ",".join(scanners),
            "--quiet",
        ]
        for exclude in ctx.setting(self.name, "exclude", list(DEFAULT_EXCLUDES)):
            command += ["--skip-dirs", str(exclude)]
        if ctx.offline:
            command += ["--skip-db-update", "--skip-java-db-update", "--offline-scan"]
        command.append(str(project))

        result = self.exec(ctx, command, cwd=project)
        if not output.exists():
            raise RuntimeError(f"trivy failed: {result.failure_message()}")

        data = json.loads(output.read_text(encoding="utf-8", errors="replace") or "{}")
        findings: List[Finding] = []
        for group in data.get("Results", []) or []:
            target = relative_path(group.get("Target"), project)
            findings.extend(self._vulnerabilities(group, target))
            findings.extend(self._misconfigurations(group, target))
            findings.extend(self._secrets(group, target))
        return findings

    # ------------------------------------------------------------- internals

    def _vulnerabilities(self, group: Dict[str, Any], target: Optional[str]) -> List[Finding]:
        findings = []
        for vuln in group.get("Vulnerabilities", []) or []:
            package = vuln.get("PkgName", "unknown")
            installed = vuln.get("InstalledVersion", "unknown")
            fixed = vuln.get("FixedVersion")
            vuln_id = str(vuln.get("VulnerabilityID", "UNKNOWN"))
            findings.append(
                Finding(
                    source=self.name,
                    layer=Layer.CODE,
                    severity=_SEVERITY_MAP.get(
                        str(vuln.get("Severity", "")).upper(), Severity.MEDIUM
                    ),
                    title=f"Vulnerable dependency {package} {installed} ({vuln_id})",
                    explanation=(
                        str(vuln.get("Title") or vuln.get("Description") or "")[:2000].strip()
                        or f"{package} {installed} is affected by {vuln_id}."
                    ),
                    evidence=clean_evidence(
                        "\n".join(
                            [
                                f"package: {package} {installed}",
                                f"advisory: {vuln_id}",
                                f"fixed version: {fixed or 'none published'}",
                                f"manifest: {target or 'unknown'}",
                                f"status: {vuln.get('Status', 'unknown')}",
                            ]
                        )
                    ),
                    location=Location(file=target, component=f"{package}@{installed}"),
                    remediation=(
                        f"Upgrade {package} to {fixed}."
                        if fixed
                        else "No fixed version is published; assess exploitability and mitigate."
                    ),
                    rule_id=vuln_id,
                    references=[str(url) for url in (vuln.get("References") or [])][:3],
                    cwe=[str(cwe) for cwe in (vuln.get("CweIDs") or [])],
                    tags=["dependency", "sca"],
                    raw={k: v for k, v in vuln.items() if k != "Description"},
                )
            )
        return findings

    def _misconfigurations(self, group: Dict[str, Any], target: Optional[str]) -> List[Finding]:
        findings = []
        for misconfig in group.get("Misconfigurations", []) or []:
            cause = misconfig.get("CauseMetadata") or {}
            findings.append(
                Finding(
                    source=self.name,
                    layer=Layer.CODE,
                    severity=_SEVERITY_MAP.get(
                        str(misconfig.get("Severity", "")).upper(), Severity.MEDIUM
                    ),
                    title=f"Insecure configuration: {misconfig.get('Title', misconfig.get('ID'))}",
                    explanation=str(
                        misconfig.get("Description") or misconfig.get("Message") or ""
                    )[:2000].strip(),
                    evidence=clean_evidence(
                        "\n".join(
                            filter(
                                None,
                                [
                                    f"check: {misconfig.get('ID')} ({misconfig.get('Type')})",
                                    f"message: {misconfig.get('Message', '')}",
                                    f"resource: {cause.get('Resource', '')}",
                                ],
                            )
                        )
                    ),
                    location=Location(
                        file=target,
                        line=cause.get("StartLine"),
                        end_line=cause.get("EndLine"),
                        component=cause.get("Resource"),
                    ),
                    remediation=str(misconfig.get("Resolution", "")).strip()
                    or "Apply the referenced hardening guidance to the configuration file.",
                    rule_id=str(misconfig.get("ID", "trivy-misconfig")),
                    references=[str(url) for url in (misconfig.get("References") or [])][:3],
                    tags=["configuration", "iac"],
                    raw=misconfig,
                )
            )
        return findings

    def _secrets(self, group: Dict[str, Any], target: Optional[str]) -> List[Finding]:
        findings = []
        for secret in group.get("Secrets", []) or []:
            findings.append(
                Finding(
                    source=self.name,
                    layer=Layer.CODE,
                    severity=_SEVERITY_MAP.get(
                        str(secret.get("Severity", "")).upper(), Severity.HIGH
                    ),
                    title=f"Secret detected: {secret.get('Title', secret.get('RuleID'))}",
                    explanation=(
                        "Trivy's secret scanner matched a credential pattern in the source tree."
                    ),
                    evidence=clean_evidence(
                        "\n".join(
                            [
                                f"rule: {secret.get('RuleID')} ({secret.get('Category')})",
                                f"match: {secret.get('Match', '')}",
                            ]
                        )
                    ),
                    location=Location(
                        file=target,
                        line=secret.get("StartLine"),
                        end_line=secret.get("EndLine"),
                    ),
                    remediation=(
                        "Rotate the credential, remove it from the repository and its history, "
                        "and inject it at runtime from a secret manager."
                    ),
                    rule_id=str(secret.get("RuleID", "trivy-secret")),
                    cwe=["CWE-798"],
                    tags=["secrets"],
                    raw={k: v for k, v in secret.items() if k != "Match"},
                )
            )
        return findings
