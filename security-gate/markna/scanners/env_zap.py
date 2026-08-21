"""OWASP ZAP baseline scan (passive DAST).

The baseline scan spiders the target and reports what its passive rules observe.
It does not attack: no injection payloads, no active scan rules. That keeps it
appropriate for a shared UAT environment while still providing real DAST
evidence from a mature, widely trusted tool.

ZAP runs from its official container image. If Docker is unavailable the scanner
reports as unavailable, which surfaces as a coverage gap when the policy requires
DAST.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

from ..exec import probe_version
from ..models import Finding, Layer, Location, Severity
from ..redact import clean_evidence
from .base import Scanner, ScannerContext, register

_RISK_MAP = {
    "3": Severity.HIGH,
    "2": Severity.MEDIUM,
    "1": Severity.LOW,
    "0": Severity.INFO,
}

DEFAULT_IMAGE = "ghcr.io/zaproxy/zaproxy:stable"


@register
class ZapBaselineScanner(Scanner):
    name = "zap-baseline"
    layer = Layer.ENVIRONMENT
    capabilities = ("dast", "http-security-headers", "information-exposure")
    description = "OWASP ZAP baseline scan (passive rules only) against the deployed environment."
    requires_executable = ("docker", "zap-baseline.py")
    install_hint = (
        "Install Docker (the scanner pulls the official ZAP image), or put zap-baseline.py on PATH."
    )

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.target_url:
            return False, "no environment URL supplied"
        if not ctx.authorization:
            return False, "environment testing requires recorded authorisation"
        if ctx.offline:
            return False, "offline mode: the ZAP container image cannot be pulled"
        if not ctx.setting(self.name, "enabled", True):
            return False, "disabled in configuration"
        return True, ""

    def available(self, ctx: ScannerContext) -> tuple:
        if ctx.tool_path.which("zap-baseline.py"):
            return True, ""
        if not ctx.tool_path.which("docker"):
            return False, f"neither zap-baseline.py nor docker is available. {self.install_hint}"
        probe = self.exec(ctx, ["docker", "info"], timeout=60)
        if not probe.ok:
            return False, f"docker is installed but the daemon is not usable: {probe.failure_message()}"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        if ctx.tool_path.which("zap-baseline.py"):
            return "zap-baseline.py (native)"
        return probe_version("docker", tool_path=ctx.tool_path)

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        workdir = ctx.scanner_workdir(self.name)
        report_name = "zap-baseline.json"
        report = workdir / report_name
        minutes = int(ctx.setting(self.name, "spider_minutes", 2))
        target = ctx.target_url or ""

        if ctx.tool_path.which("zap-baseline.py"):
            command = [
                "zap-baseline.py",
                "-t", target,
                "-J", str(report),
                "-m", str(minutes),
                "-I",
            ]
        else:
            image = str(ctx.setting(self.name, "image", DEFAULT_IMAGE))
            command = [
                "docker", "run", "--rm",
                "-v", f"{workdir}:/zap/wrk/:rw",
                "-t", image,
                "zap-baseline.py",
                "-t", target,
                "-J", report_name,
                "-m", str(minutes),
                "-I",
            ]

        result = self.exec(ctx, command, timeout=max(ctx.timeout, minutes * 60 + 300))
        if not report.exists():
            raise RuntimeError(f"zap baseline produced no report: {result.failure_message()}")

        data = json.loads(report.read_text(encoding="utf-8", errors="replace") or "{}")
        findings: List[Finding] = []
        for site in data.get("site", []) or []:
            for alert in site.get("alerts", []) or []:
                findings.append(self._to_finding(alert, target))
        return findings

    def _to_finding(self, alert: Dict[str, Any], target: str) -> Finding:
        severity = _RISK_MAP.get(str(alert.get("riskcode", "0")), Severity.INFO)
        confidence = {"3": "high", "2": "medium", "1": "low", "0": "low"}.get(
            str(alert.get("confidence", "2")), "medium"
        )
        # ZAP's low-confidence findings are frequently context-dependent; do not
        # let them block a release on their own.
        if confidence == "low" and severity is Severity.HIGH:
            severity = Severity.MEDIUM

        instances = alert.get("instances", []) or []
        first = instances[0] if instances else {}
        cwe_id = str(alert.get("cweid", "")).strip()

        evidence_lines = [f"instances: {alert.get('count', len(instances))}"]
        for instance in instances[:5]:
            evidence_lines.append(
                f"{instance.get('method', 'GET')} {instance.get('uri', '')}"
                + (f" [param: {instance.get('param')}]" if instance.get("param") else "")
                + (f"\n  evidence: {instance.get('evidence')}" if instance.get("evidence") else "")
            )

        return Finding(
            source=self.name,
            layer=Layer.ENVIRONMENT,
            severity=severity,
            title=str(alert.get("name") or alert.get("alert") or "ZAP alert"),
            explanation=_strip_html(str(alert.get("desc", ""))),
            evidence=clean_evidence("\n".join(evidence_lines)),
            location=Location(url=str(first.get("uri") or target)),
            remediation=_strip_html(str(alert.get("solution", ""))) or "See the ZAP alert reference.",
            rule_id=f"zap/{alert.get('pluginid', 'unknown')}",
            references=[
                line.strip()
                for line in _strip_html(str(alert.get("reference", ""))).splitlines()
                if line.strip().startswith("http")
            ][:3],
            cwe=[f"CWE-{cwe_id}"] if cwe_id and cwe_id != "-1" else [],
            tags=["dast", "zap"],
            confidence=confidence,
            raw={k: v for k, v in alert.items() if k not in ("desc", "solution", "reference")},
        )


def _strip_html(text: str) -> str:
    """ZAP descriptions are HTML fragments; reports are plain text."""
    import re

    without_tags = re.sub(r"<[^>]+>", "", text or "")
    return re.sub(r"\n{3,}", "\n\n", without_tags).strip()
