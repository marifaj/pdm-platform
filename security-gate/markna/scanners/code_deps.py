"""Dependency vulnerability adapters.

Three complementary tools:

* **pip-audit** — Python requirement files, queried against PyPI/OSV.
* **osv-scanner** — multi-ecosystem lockfile scanning against the OSV database.
* **npm audit** — the Node ecosystem, using the project's own lockfile.

All three need network access to a vulnerability database. In an offline run they
report as unavailable, which surfaces as a coverage gap rather than a clean bill
of health.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..exec import probe_version
from ..models import Finding, Layer, Location, Severity
from ..redact import clean_evidence
from .base import Scanner, ScannerContext, register
from .util import DEFAULT_EXCLUDES, cvss_score, find_files, load_json, relative_path

_NPM_SEVERITY = {
    "critical": Severity.CRITICAL,
    "high": Severity.HIGH,
    "moderate": Severity.MEDIUM,
    "low": Severity.LOW,
    "info": Severity.INFO,
}


def _severity_from_score(score: Optional[float], default: Severity) -> Severity:
    if score is None:
        return default
    return Severity.parse(score, default)


@register
class PipAuditScanner(Scanner):
    name = "pip-audit"
    layer = Layer.CODE
    capabilities = ("dependency-vulnerabilities",)
    description = "pip-audit: known vulnerabilities in Python requirements (PyPI/OSV)."
    requires_executable = ("pip-audit",)
    install_hint = "Install with: pip install pip-audit"

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        if ctx.offline:
            return False, "offline mode: the OSV/PyPI advisory database is unreachable"
        if not self._requirement_files(ctx):
            return False, "no requirements*.txt files found"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        return probe_version("pip-audit", tool_path=ctx.tool_path)

    def _requirement_files(self, ctx: ScannerContext) -> List[Path]:
        assert ctx.project_path is not None
        configured = ctx.setting(self.name, "requirement_files")
        if configured:
            return [ctx.project_path / str(entry) for entry in configured]
        found = find_files(ctx.project_path, ["requirements*.txt", "requirements/*.txt"])
        return [path for path in found if path.stat().st_size > 0]

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        findings: List[Finding] = []
        failures: List[str] = []

        for requirements in self._requirement_files(ctx):
            command = [
                "pip-audit",
                "--format",
                "json",
                "--progress-spinner",
                "off",
                "--requirement",
                str(requirements),
            ]
            result = self.exec(ctx, command, cwd=project)
            # 0 = no vulnerabilities, 1 = vulnerabilities found.
            if result.returncode not in (0, 1) or not result.stdout.strip():
                failures.append(f"{requirements.name}: {result.failure_message()}")
                continue
            try:
                data = load_json(result.stdout)
            except ValueError as exc:
                failures.append(f"{requirements.name}: {exc}")
                continue
            findings.extend(self._parse(data, requirements, project))

        if failures and not findings:
            raise RuntimeError("; ".join(failures))
        for failure in failures:
            findings.append(
                Finding(
                    source=self.name,
                    layer=Layer.CODE,
                    severity=Severity.MEDIUM,
                    title="pip-audit could not resolve part of the dependency set",
                    explanation=(
                        "One or more requirement files could not be audited, so the dependency "
                        "result is incomplete."
                    ),
                    evidence=clean_evidence(failure),
                    location=Location(file=relative_path(failure.split(":")[0], project)),
                    remediation="Pin resolvable versions, or audit the file manually.",
                    rule_id="pip-audit/resolve-error",
                    tags=["scanner-warning"],
                )
            )
        return findings

    def _parse(self, data: Any, requirements: Path, project: Path) -> List[Finding]:
        findings: List[Finding] = []
        dependencies = data.get("dependencies", data) if isinstance(data, dict) else data
        rel_path = relative_path(str(requirements), project)
        for dependency in dependencies or []:
            name = dependency.get("name", "unknown")
            version = dependency.get("version", "unknown")
            for vuln in dependency.get("vulns", []) or []:
                vuln_id = str(vuln.get("id", "UNKNOWN"))
                aliases = [str(alias) for alias in vuln.get("aliases", []) or []]
                fixes = [str(fix) for fix in vuln.get("fix_versions", []) or []]
                score = cvss_score(vuln.get("severity") or [])
                # pip-audit's JSON carries no severity for most advisories. A
                # fixable known-vulnerable dependency is a release blocker; one
                # with no published fix needs a documented risk acceptance, so
                # it warns instead.
                severity = _severity_from_score(
                    score, Severity.HIGH if fixes else Severity.MEDIUM
                )
                findings.append(
                    Finding(
                        source=self.name,
                        layer=Layer.CODE,
                        severity=severity,
                        title=f"Vulnerable dependency {name} {version} ({vuln_id})",
                        explanation=(
                            str(vuln.get("description", "")).strip()
                            or f"{name} {version} is affected by {vuln_id}."
                        ),
                        evidence=clean_evidence(
                            "\n".join(
                                [
                                    f"package: {name}=={version}",
                                    f"advisory: {vuln_id}",
                                    f"aliases: {', '.join(aliases) or 'none'}",
                                    f"fixed in: {', '.join(fixes) or 'no fix published'}",
                                    f"declared in: {rel_path}",
                                ]
                            )
                        ),
                        location=Location(file=rel_path, component=f"{name}=={version}"),
                        remediation=(
                            f"Upgrade {name} to {' or '.join(fixes)}."
                            if fixes
                            else (
                                f"No fixed version is published for {vuln_id}. Assess "
                                "exploitability in this application, apply a mitigation, and "
                                "record an explicit risk acceptance before UAT."
                            )
                        ),
                        rule_id=vuln_id,
                        references=[
                            f"https://osv.dev/vulnerability/{vuln_id}",
                            *[f"https://nvd.nist.gov/vuln/detail/{a}" for a in aliases if a.startswith("CVE-")][:1],
                        ],
                        tags=["dependency", "sca"],
                        confidence="high" if score is not None else "medium",
                        raw=vuln,
                    )
                )
        return findings


@register
class OsvScannerScanner(Scanner):
    name = "osv-scanner"
    layer = Layer.CODE
    capabilities = ("dependency-vulnerabilities",)
    description = "OSV-Scanner: multi-ecosystem lockfile vulnerability scanning."
    requires_executable = ("osv-scanner",)
    install_hint = "Install from https://github.com/google/osv-scanner/releases"

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        if ctx.offline:
            return False, "offline mode: the OSV database is unreachable"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        return probe_version("osv-scanner", tool_path=ctx.tool_path)

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        output = ctx.scanner_workdir(self.name) / "osv.json"
        command = ["osv-scanner", "--format", "json", "--recursive", str(project)]
        result = self.exec(ctx, command, cwd=project, output_file=output)
        # 0 = clean, 1 = vulnerabilities found, 128 = no packages found.
        if result.returncode == 128:
            return []
        if result.returncode not in (0, 1):
            raise RuntimeError(f"osv-scanner failed: {result.failure_message()}")

        data = json.loads(output.read_text(encoding="utf-8", errors="replace") or "{}")
        findings: List[Finding] = []
        for group in data.get("results", []) or []:
            source_path = relative_path((group.get("source") or {}).get("path"), project)
            for package_entry in group.get("packages", []) or []:
                package = package_entry.get("package") or {}
                name = package.get("name", "unknown")
                version = package.get("version", "unknown")
                ecosystem = package.get("ecosystem", "")
                for vuln in package_entry.get("vulnerabilities", []) or []:
                    findings.append(
                        self._vuln_finding(vuln, name, version, ecosystem, source_path)
                    )
        return findings

    def _vuln_finding(
        self,
        vuln: Dict[str, Any],
        name: str,
        version: str,
        ecosystem: str,
        source_path: Optional[str],
    ) -> Finding:
        vuln_id = str(vuln.get("id", "UNKNOWN"))
        aliases = [str(alias) for alias in vuln.get("aliases", []) or []]
        score = cvss_score(vuln.get("severity") or [])
        severity = _severity_from_score(score, Severity.HIGH)
        summary = str(vuln.get("summary") or "").strip()
        return Finding(
            source=self.name,
            layer=Layer.CODE,
            severity=severity,
            title=f"Vulnerable dependency {name} {version} ({vuln_id})",
            explanation=(summary or str(vuln.get("details", ""))[:1500] or f"{name} is affected by {vuln_id}."),
            evidence=clean_evidence(
                "\n".join(
                    [
                        f"package: {name} {version} ({ecosystem})",
                        f"advisory: {vuln_id}",
                        f"aliases: {', '.join(aliases) or 'none'}",
                        f"cvss: {score if score is not None else 'not published'}",
                        f"manifest: {source_path or 'unknown'}",
                    ]
                )
            ),
            location=Location(file=source_path, component=f"{name}@{version}"),
            remediation=(
                f"Upgrade {name} past the affected range. See "
                f"https://osv.dev/vulnerability/{vuln_id} for fixed versions."
            ),
            rule_id=vuln_id,
            references=[f"https://osv.dev/vulnerability/{vuln_id}"],
            tags=["dependency", "sca", f"ecosystem:{ecosystem.lower()}" if ecosystem else "sca"],
            confidence="high" if score is not None else "medium",
            raw={k: v for k, v in vuln.items() if k != "affected"},
        )


@register
class NpmAuditScanner(Scanner):
    name = "npm-audit"
    layer = Layer.CODE
    capabilities = ("dependency-vulnerabilities",)
    description = "npm audit against the project's package-lock.json."
    requires_executable = ("npm",)
    install_hint = "Install Node.js (npm ships with it)."

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        if ctx.offline:
            return False, "offline mode: the npm advisory endpoint is unreachable"
        if not self._lock_dirs(ctx.project_path):
            return False, "no package-lock.json found"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        version = probe_version("npm", tool_path=ctx.tool_path)
        return f"npm {version}" if version else None

    def _lock_dirs(self, project: Path) -> List[Path]:
        locks = find_files(project, ["package-lock.json"], excludes=DEFAULT_EXCLUDES, limit=20)
        return [lock.parent for lock in locks]

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        findings: List[Finding] = []
        for directory in self._lock_dirs(project):
            command = ["npm", "audit", "--json", "--package-lock-only"]
            result = self.exec(ctx, command, cwd=directory)
            if not result.stdout.strip():
                raise RuntimeError(f"npm audit produced no output: {result.failure_message()}")
            try:
                data = load_json(result.stdout)
            except ValueError as exc:
                raise RuntimeError(f"npm audit output was not JSON: {exc}") from exc
            if data.get("error"):
                raise RuntimeError(f"npm audit error: {data['error']}")
            findings.extend(self._parse(data, directory, project))
        return findings

    def _parse(self, data: Dict[str, Any], directory: Path, project: Path) -> List[Finding]:
        findings: List[Finding] = []
        manifest = relative_path(str(directory / "package-lock.json"), project)
        for package_name, entry in (data.get("vulnerabilities") or {}).items():
            severity = _NPM_SEVERITY.get(str(entry.get("severity", "")).lower(), Severity.MEDIUM)
            advisories = [via for via in entry.get("via", []) if isinstance(via, dict)]
            titles = [str(via.get("title", "")) for via in advisories if via.get("title")]
            urls = [str(via.get("url", "")) for via in advisories if via.get("url")]
            cwes = sorted({cwe for via in advisories for cwe in via.get("cwe", []) or []})
            fix_available = entry.get("fixAvailable")
            findings.append(
                Finding(
                    source=self.name,
                    layer=Layer.CODE,
                    severity=severity,
                    title=f"Vulnerable npm dependency {package_name} ({entry.get('severity', 'unknown')})",
                    explanation=(
                        "; ".join(titles)
                        or f"npm audit reports {package_name} as vulnerable in the resolved tree."
                    ),
                    evidence=clean_evidence(
                        "\n".join(
                            [
                                f"package: {package_name}",
                                f"affected range: {entry.get('range', 'unknown')}",
                                f"direct dependency: {entry.get('isDirect', 'unknown')}",
                                f"fix available: {json.dumps(fix_available)[:200]}",
                                f"manifest: {manifest}",
                            ]
                        )
                    ),
                    location=Location(file=manifest, component=package_name),
                    remediation=(
                        "Run `npm audit fix` (or upgrade the parent dependency) and re-run the gate."
                        if fix_available
                        else "No automatic fix is available; upgrade or replace the dependent package."
                    ),
                    rule_id=f"npm-audit/{package_name}",
                    references=urls[:3],
                    cwe=[str(cwe) for cwe in cwes],
                    tags=["dependency", "sca", "ecosystem:npm"],
                    raw=entry,
                )
            )
        return findings
