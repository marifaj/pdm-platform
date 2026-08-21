"""Secret-detection adapters.

Preference order: Gitleaks (best coverage, scans git history) → detect-secrets
(pip-installable, entropy plus typed detectors) → a bundled regex fallback that
runs only when neither is installed, so a gate on a bare runner still produces
deterministic secrets evidence rather than silence.

None of these scanners ever writes a discovered secret into a report: values are
hashed or redacted before they reach a finding.
"""

from __future__ import annotations

import hashlib
import math
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from ..confinement import iter_safe_files
from ..exec import probe_version
from ..models import Finding, Layer, Location, Severity
from ..redact import clean_evidence, redact
from .base import Scanner, ScannerContext, register
from .util import DEFAULT_EXCLUDES, load_json, load_json_file, read_snippet, relative_path

_REMEDIATION = (
    "Treat the credential as compromised: rotate it at the provider, purge it from the "
    "repository (including git history), and load it at runtime from a secret manager or an "
    "injected environment variable instead."
)

#: Detector names that identify a concrete credential type. Entropy-only hits are
#: reported one severity lower because they have a real false-positive rate.
_TYPED_DETECTORS = {
    "aws access key",
    "aws sensitive information",
    "azure storage account access key",
    "basic auth credentials",
    "cloudant credentials",
    "discord bot token",
    "github token",
    "gitlab token",
    "ibm cloud iam key",
    "ibm cos hmac credentials",
    "jwt token",
    "mailchimp access key",
    "npm token",
    "openai token",
    "private key",
    "sendgrid api key",
    "slack token",
    "softlayer credentials",
    "square oauth secret",
    "stripe access key",
    "telegram bot token",
    "twilio api key",
}


@register
class GitleaksScanner(Scanner):
    name = "gitleaks"
    layer = Layer.CODE
    capabilities = ("secrets",)
    description = "Gitleaks secret detection across the working tree and git history."
    requires_executable = ("gitleaks",)
    install_hint = "Install from https://github.com/gitleaks/gitleaks/releases"

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        return probe_version("gitleaks", tool_path=ctx.tool_path)

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        report = ctx.scanner_workdir(self.name) / "gitleaks.json"
        scan_history = bool(ctx.setting(self.name, "scan_git_history", True))

        last_failure = "no gitleaks invocation was attempted"
        for command in self._command_variants(project, report, scan_history):
            if report.exists():
                report.unlink()
            result = self.exec(ctx, command, cwd=project)
            # 0 = no leaks, 1 = leaks found. Other codes mean the subcommand is
            # unsupported by this gitleaks version; try the next spelling.
            if result.returncode in (0, 1) and report.exists():
                return self._parse(report, project)
            last_failure = result.failure_message()
        raise RuntimeError(f"gitleaks failed: {last_failure}")

    def _command_variants(
        self, project: Path, report: Path, scan_history: bool
    ) -> List[List[str]]:
        common = [
            "--report-format",
            "json",
            "--report-path",
            str(report),
            "--redact",
            "--no-banner",
            "--exit-code",
            "1",
        ]
        variants: List[List[str]] = []
        if scan_history and (project / ".git").exists():
            # gitleaks >= 8.19 subcommand, then the older flag form.
            variants.append(["gitleaks", "git", str(project), *common])
            variants.append(["gitleaks", "detect", "--source", str(project), *common])
        variants.append(["gitleaks", "dir", str(project), *common])
        variants.append(["gitleaks", "detect", "--source", str(project), "--no-git", *common])
        return variants

    def _parse(self, report: Path, project: Path) -> List[Finding]:
        data = load_json_file(report)
        findings: List[Finding] = []
        for item in data or []:
            rule_id = str(item.get("RuleID") or item.get("Rule") or "gitleaks")
            rel_path = relative_path(item.get("File"), project)
            line = item.get("StartLine")
            commit = str(item.get("Commit") or "")
            in_history = bool(commit) and commit != "0" * 40
            findings.append(
                Finding(
                    source=self.name,
                    layer=Layer.CODE,
                    severity=Severity.CRITICAL if not in_history else Severity.HIGH,
                    title=f"Secret detected: {item.get('Description') or rule_id}",
                    explanation=(
                        "Gitleaks matched a credential pattern in the repository. "
                        + (
                            "The match is in git history, so it remains recoverable from any "
                            "clone even if the file has since changed."
                            if in_history
                            else "The match is in the current working tree."
                        )
                    ),
                    evidence=clean_evidence(
                        "\n".join(
                            filter(
                                None,
                                [
                                    f"rule: {rule_id}",
                                    f"match: {item.get('Match', '')}",
                                    f"commit: {commit}" if in_history else None,
                                    f"author: {item.get('Author', '')}" if in_history else None,
                                    f"entropy: {item.get('Entropy')}"
                                    if item.get("Entropy")
                                    else None,
                                ],
                            )
                        )
                    ),
                    location=Location(file=rel_path, line=line),
                    remediation=_REMEDIATION,
                    rule_id=rule_id,
                    cwe=["CWE-798"],
                    tags=["secrets"] + (["git-history"] if in_history else []),
                    raw={k: v for k, v in item.items() if k not in ("Secret", "Match")},
                )
            )
        return findings


@register
class DetectSecretsScanner(Scanner):
    name = "detect-secrets"
    layer = Layer.CODE
    capabilities = ("secrets",)
    description = "Yelp detect-secrets: typed credential detectors plus entropy analysis."
    requires_executable = ("detect-secrets",)
    install_hint = "Install with: pip install detect-secrets"

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        return probe_version("detect-secrets", tool_path=ctx.tool_path)

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        command = ["detect-secrets", "scan", "--all-files"]
        for exclude in ctx.setting(self.name, "exclude", list(DEFAULT_EXCLUDES)):
            command += ["--exclude-files", rf"(^|/){re.escape(str(exclude))}(/|$)"]
        command.append(str(project))

        result = self.exec(ctx, command, cwd=project)
        if not result.ok:
            raise RuntimeError(f"detect-secrets failed: {result.failure_message()}")

        data = load_json(result.stdout)
        findings: List[Finding] = []
        for file_path, entries in (data.get("results") or {}).items():
            rel_path = relative_path(file_path, project)
            for entry in entries:
                detector = str(entry.get("type", "unknown"))
                typed = detector.strip().lower() in _TYPED_DETECTORS
                line = entry.get("line_number")
                absolute = project / rel_path if rel_path else None
                findings.append(
                    Finding(
                        source=self.name,
                        layer=Layer.CODE,
                        severity=Severity.HIGH if typed else Severity.MEDIUM,
                        title=f"Possible hard-coded secret: {detector}",
                        explanation=(
                            f"detect-secrets matched the '{detector}' detector. "
                            + (
                                "This detector recognises a specific credential format, so the "
                                "match is very likely a real credential."
                                if typed
                                else "This is an entropy/keyword heuristic, so confirm the value "
                                "before rotating: it may be a test fixture or a placeholder."
                            )
                        ),
                        evidence=clean_evidence(
                            "\n".join(
                                filter(
                                    None,
                                    [
                                        f"detector: {detector}",
                                        f"sha1(secret): {entry.get('hashed_secret', '')}",
                                        read_snippet(absolute, line, line, root=project),
                                    ],
                                )
                            )
                        ),
                        location=Location(file=rel_path, line=line),
                        remediation=_REMEDIATION,
                        rule_id=f"detect-secrets/{detector.replace(' ', '-').lower()}",
                        cwe=["CWE-798"],
                        tags=["secrets"] + ([] if typed else ["heuristic"]),
                        confidence="high" if typed else "medium",
                        raw=entry,
                    )
                )
        return findings


#: Fallback patterns. Deliberately narrow — high-signal formats only.
FALLBACK_SECRET_PATTERNS: List[Tuple[str, str, "re.Pattern[str]", Severity]] = [
    ("aws-access-key", "AWS access key id", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), Severity.CRITICAL),
    ("github-token", "GitHub token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,}\b"), Severity.CRITICAL),
    ("slack-token", "Slack token", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b"), Severity.CRITICAL),
    ("google-api-key", "Google API key", re.compile(r"\bAIza[0-9A-Za-z_\-]{20,}\b"), Severity.HIGH),
    ("private-key", "Private key block", re.compile(r"-----BEGIN[ A-Z]*PRIVATE KEY-----"), Severity.CRITICAL),
    ("jwt", "JSON Web Token", re.compile(r"\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\b"), Severity.HIGH),
    (
        "url-credentials",
        "Credentials embedded in a URL",
        re.compile(r"\b[a-z][a-z0-9+.\-]*://[^\s:/@]+:[^\s@/]{3,}@"),
        Severity.HIGH,
    ),
    (
        "assigned-secret",
        "Credential assigned to a variable",
        re.compile(
            r"(?i)\b(password|passwd|secret|api[_-]?key|apikey|access[_-]?key|client[_-]?secret"
            r"|auth[_-]?token)s?\b\s*[:=]\s*['\"][^'\"\s]{8,}['\"]"
        ),
        Severity.MEDIUM,
    ),
]

_FALLBACK_TEXT_SUFFIXES = {
    ".py", ".js", ".ts", ".tsx", ".jsx", ".java", ".go", ".rb", ".php", ".cs", ".c", ".cpp",
    ".h", ".sh", ".bash", ".zsh", ".ps1", ".yaml", ".yml", ".json", ".toml", ".ini", ".cfg",
    ".conf", ".env", ".properties", ".xml", ".tf", ".tfvars", ".md", ".txt", ".sql", ".ino",
    ".service", ".dockerfile", "",
}

_PLACEHOLDER = re.compile(
    r"(?i)(example|changeme|placeholder|your[_-]?|xxx+|<[^>]+>|\$\{[^}]+\}|dummy|sample|redacted)"
)


@register
class BuiltinSecretsScanner(Scanner):
    """Regex fallback, active only when no dedicated secrets scanner is installed."""

    name = "markna-secrets-fallback"
    layer = Layer.CODE
    capabilities = ("secrets",)
    description = (
        "Bundled regex secret detection. Fallback only — install Gitleaks or detect-secrets "
        "for real coverage."
    )

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        for preferred in ("gitleaks", "detect-secrets"):
            if ctx.tool_path.which(preferred):
                return False, f"{preferred} is installed and supersedes this fallback"
        return True, ""

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        max_bytes = int(ctx.setting(self.name, "max_file_bytes", 2_000_000))
        excludes = set(ctx.setting(self.name, "exclude", list(DEFAULT_EXCLUDES)))

        findings: List[Finding] = [self._coverage_notice()]
        seen: set = set()
        # iter_safe_files never descends a directory symlink and never yields a
        # file whose target resolves outside the project: the repository under
        # assessment does not get to choose what this scanner reads.
        for path in iter_safe_files(project, skip_directories=excludes):
            if path.suffix.lower() not in _FALLBACK_TEXT_SUFFIXES:
                continue
            try:
                if path.stat().st_size > max_bytes:
                    continue
                content = path.read_text(encoding="utf-8", errors="strict")
            except (OSError, UnicodeDecodeError):
                continue
            findings.extend(self._scan_text(path, content, project, seen))
        return findings

    def _scan_text(
        self, path: Path, content: str, project: Path, seen: set
    ) -> Iterable[Finding]:
        rel_path = relative_path(str(path), project)
        for line_number, line in enumerate(content.splitlines(), start=1):
            if len(line) > 1000:
                continue
            for rule_id, label, pattern, severity in FALLBACK_SECRET_PATTERNS:
                match = pattern.search(line)
                if not match:
                    continue
                value = match.group(0)
                if _PLACEHOLDER.search(value):
                    severity = Severity.LOW
                key = (rel_path, rule_id, hashlib.sha1(value.encode()).hexdigest())
                if key in seen:
                    continue
                seen.add(key)
                yield Finding(
                    source=self.name,
                    layer=Layer.CODE,
                    severity=severity,
                    title=f"Possible hard-coded secret: {label}",
                    explanation=(
                        f"A bundled fallback pattern ('{rule_id}') matched. This scanner is a "
                        "backstop for runners with no dedicated secrets tool; confirm the match "
                        "manually."
                        + (
                            " The value looks like a placeholder, so severity was lowered."
                            if _PLACEHOLDER.search(value)
                            else ""
                        )
                    ),
                    evidence=clean_evidence(
                        f"{rel_path}:{line_number}\n{redact(line.strip())}\n"
                        f"sha1(match): {hashlib.sha1(value.encode()).hexdigest()}"
                    ),
                    location=Location(file=rel_path, line=line_number),
                    remediation=_REMEDIATION,
                    rule_id=f"markna-secrets/{rule_id}",
                    cwe=["CWE-798"],
                    tags=["secrets", "fallback"],
                    confidence="medium",
                )

    def _coverage_notice(self) -> Finding:
        return Finding(
            source=self.name,
            layer=Layer.CODE,
            severity=Severity.INFO,
            title="Secrets scanning ran on the bundled fallback engine",
            explanation=(
                "No dedicated secrets scanner was available, so MARKNA used its bundled regex "
                "patterns. These cover common credential formats but do not scan git history and "
                "have no entropy analysis, so absence of findings here is weak evidence."
            ),
            evidence="gitleaks: not installed; detect-secrets: not installed",
            location=Location(component="markna-secrets-fallback"),
            remediation=(
                "Install Gitleaks (recommended, scans history) or detect-secrets on the runner "
                "and re-run the gate."
            ),
            rule_id="markna-secrets/fallback-in-use",
            tags=["coverage", "secrets"],
        )


def shannon_entropy(value: str) -> float:
    """Entropy in bits per character; retained for policy tuning and tests."""
    if not value:
        return 0.0
    counts: Dict[str, int] = {}
    for char in value:
        counts[char] = counts.get(char, 0) + 1
    length = len(value)
    return -sum((count / length) * math.log2(count / length) for count in counts.values())
