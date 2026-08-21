"""Semgrep adapter — static analysis of first-party code.

Semgrep is the primary SAST engine. When the Semgrep registry is reachable, point
it at the registry packs (`p/default`, `p/owasp-top-ten`, …) for thousands of
rules. When it is not — air-gapped runners, egress proxies — MARKNA falls back to
the ruleset bundled in ``markna/rules/semgrep``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..exec import probe_version
from ..models import Finding, Layer, Location, Severity
from ..redact import clean_evidence
from .base import Scanner, ScannerContext, register
from .util import DEFAULT_EXCLUDES, load_json_file, read_snippet, relative_path

BUNDLED_RULES = Path(__file__).resolve().parent.parent / "rules" / "semgrep"

#: Semgrep severities are a three-level scale; MARKNA maps them conservatively
#: and lets rule metadata (impact/likelihood/confidence) refine the result.
_SEVERITY_MAP = {
    "ERROR": Severity.HIGH,
    "WARNING": Severity.MEDIUM,
    "INFO": Severity.LOW,
    "INVENTORY": Severity.INFO,
    "EXPERIMENT": Severity.INFO,
}


@register
class SemgrepScanner(Scanner):
    name = "semgrep"
    layer = Layer.CODE
    capabilities = ("sast",)
    description = "Semgrep static analysis (OWASP/CWE rule packs, bundled offline baseline)."
    requires_executable = ("semgrep",)
    install_hint = "Install with: pip install semgrep"

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        # SEMGREP_ENABLE_VERSION_CHECK=0 stops `semgrep --version` from blocking on
        # a network request that an egress proxy may never answer.
        return probe_version(
            "semgrep",
            tool_path=ctx.tool_path,
            timeout=30,
            extra_env={"SEMGREP_ENABLE_VERSION_CHECK": "0"},
        )

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        configs = self._configs(ctx)
        output = ctx.scanner_workdir(self.name) / "semgrep.json"

        command: List[str] = [
            "semgrep",
            "scan",
            "--json",
            "--quiet",
            "--metrics=off",
            # Without this Semgrep blocks on a version-check request, which costs
            # ~100s per run behind an egress proxy that blackholes it.
            "--disable-version-check",
        ]
        for config in configs:
            command += ["--config", str(config)]
        for exclude in ctx.setting(self.name, "exclude", list(DEFAULT_EXCLUDES)):
            command += ["--exclude", str(exclude)]
        command += ["--timeout", str(int(ctx.setting(self.name, "rule_timeout", 30)))]
        if ctx.setting(self.name, "include_ignored_files", False):
            command.append("--no-git-ignore")
        command.append(str(project))

        result = self.exec(ctx, command, cwd=project, output_file=output)
        # 0 = clean, 1 = findings present. Anything else is a real failure.
        if result.returncode not in (0, 1):
            raise RuntimeError(f"semgrep failed: {result.failure_message()}")

        data = load_json_file(output)
        findings: List[Finding] = []
        for error in data.get("errors", []) or []:
            findings.append(self._config_error_finding(error, configs))
        prefixes = _config_prefixes(configs)
        for item in data.get("results", []) or []:
            finding = self._to_finding(item, project, prefixes)
            if finding is not None:
                findings.append(finding)
        findings.append(self._scope_finding(data, configs))
        return findings

    # ------------------------------------------------------------- internals

    def _configs(self, ctx: ScannerContext) -> List[str]:
        configured = ctx.setting(self.name, "config")
        if configured:
            return [str(entry) for entry in configured]
        configs: List[str] = [str(BUNDLED_RULES)]
        extra_rules = ctx.setting(self.name, "extra_rules_path")
        if extra_rules:
            configs.append(str(extra_rules))
        if ctx.setting(self.name, "use_registry", False) and not ctx.offline:
            configs.extend(
                ctx.setting(self.name, "registry_packs", ["p/default", "p/owasp-top-ten"])
            )
        return configs

    def _to_finding(
        self, item: Dict[str, Any], project: Path, prefixes: List[str]
    ) -> Optional[Finding]:
        extra = item.get("extra") or {}
        metadata = extra.get("metadata") or {}
        rule_id = _strip_prefix(str(item.get("check_id", "semgrep-rule")), prefixes)
        rel_path = relative_path(item.get("path"), project)
        start_line = (item.get("start") or {}).get("line")
        end_line = (item.get("end") or {}).get("line")

        severity = _SEVERITY_MAP.get(str(extra.get("severity", "")).upper(), Severity.MEDIUM)
        if severity is Severity.HIGH and str(metadata.get("impact", "")).upper() == "HIGH":
            if str(metadata.get("confidence", "")).upper() == "HIGH":
                severity = Severity.CRITICAL

        absolute = project / rel_path if rel_path else None
        snippet = read_snippet(absolute, start_line, end_line, root=project)
        evidence = snippet or str(extra.get("lines", "")).strip()

        owasp = ", ".join(str(entry) for entry in _as_list(metadata.get("owasp")))
        explanation = str(extra.get("message", "")).strip() or rule_id
        if owasp:
            explanation = f"{explanation}\n\nOWASP: {owasp}"

        return Finding(
            source=self.name,
            layer=Layer.CODE,
            severity=severity,
            title=_title_from_rule(rule_id, explanation),
            explanation=explanation,
            evidence=clean_evidence(evidence),
            location=Location(file=rel_path, line=start_line, end_line=end_line),
            remediation=str(
                metadata.get("remediation")
                or metadata.get("fix")
                or extra.get("fix")
                or "Review the flagged code against the rule guidance and remove the unsafe pattern."
            ),
            rule_id=rule_id,
            references=[str(ref) for ref in _as_list(metadata.get("references"))][:5],
            cwe=[str(entry) for entry in _as_list(metadata.get("cwe"))],
            tags=["sast"] + [f"owasp:{entry}" for entry in _as_list(metadata.get("owasp"))][:3],
            confidence=str(metadata.get("confidence", "high")).lower() or "high",
            raw=item,
        )

    def _scope_finding(self, data: Dict[str, Any], configs: List[str]) -> Finding:
        """Record what Semgrep actually looked at.

        Semgrep applies a built-in ignore list (test directories, vendored code,
        minified files) on top of .gitignore. A reader of this report needs to
        know which files produced the 'no findings' result.
        """
        paths = data.get("paths") or {}
        scanned = paths.get("scanned") or []
        skipped = paths.get("skipped") or []
        return Finding(
            source=self.name,
            layer=Layer.CODE,
            severity=Severity.INFO,
            title=f"Semgrep analysed {len(scanned)} file(s)",
            explanation=(
                "Scan scope for the SAST result. Semgrep applies .gitignore plus a built-in "
                "ignore list that excludes test directories, vendored code and minified files, "
                "so files outside this count were not analysed."
            ),
            evidence=(
                f"configs: {', '.join(configs)}\n"
                f"files scanned: {len(scanned)}\n"
                f"files skipped: {len(skipped)}\n"
                + "\n".join(f"  - {path}" for path in scanned[:40])
                + ("\n  ..." if len(scanned) > 40 else "")
            ),
            location=Location(component="semgrep"),
            remediation="No action required unless a path you expected to be analysed is absent.",
            rule_id="semgrep/scan-scope",
            tags=["sast", "coverage"],
        )

    def _config_error_finding(self, error: Dict[str, Any], configs: List[str]) -> Finding:
        message = str(error.get("message") or error.get("type") or "unknown semgrep error")
        return Finding(
            source=self.name,
            layer=Layer.CODE,
            severity=Severity.MEDIUM,
            title="Semgrep reported a scan error (rule coverage may be incomplete)",
            explanation=(
                "Semgrep emitted an error while scanning. Rules that failed to load or files "
                "that failed to parse were not analysed, so the SAST result is partial."
            ),
            evidence=clean_evidence(f"configs: {', '.join(configs)}\n{message}"),
            location=Location(component="semgrep"),
            remediation=(
                "Check the rule configuration and the parse errors listed in the evidence. "
                "If the registry is unreachable, run with the bundled ruleset only."
            ),
            rule_id="semgrep/scan-error",
            tags=["scanner-warning"],
            raw=error,
        )


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _config_prefixes(configs: Iterable[str]) -> List[str]:
    """Semgrep prefixes file-based rule ids with the dotted directory path."""
    prefixes: List[str] = []
    for config in configs:
        path = Path(str(config))
        if not path.exists():
            continue
        parent = path if path.is_dir() else path.parent
        dotted = str(parent).strip("/").replace("/", ".")
        if dotted:
            prefixes.append(f"{dotted}.")
    return sorted(prefixes, key=len, reverse=True)


def _strip_prefix(check_id: str, prefixes: Iterable[str]) -> str:
    for prefix in prefixes:
        if check_id.startswith(prefix):
            return check_id[len(prefix) :]
    return check_id


def _title_from_rule(rule_id: str, message: str) -> str:
    """A short human title: first sentence of the message, else the rule id."""
    sentence = message.split("\n")[0].strip()
    if "." in sentence:
        sentence = sentence.split(". ")[0].strip().rstrip(".")
    if 12 <= len(sentence) <= 110:
        return sentence
    return rule_id.split(".")[-1].replace("-", " ").replace("_", " ").capitalize()
