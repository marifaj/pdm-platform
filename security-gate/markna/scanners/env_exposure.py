"""Information-exposure probing against the deployed environment.

A fixed list of well-known paths is requested with GET — nothing is fuzzed,
guessed at scale, or modified. Before probing, the scanner establishes a
baseline against a path that cannot exist, so applications that answer 200 for
everything (SPA fallbacks, catch-all routers) do not produce a page of false
positives.
"""

from __future__ import annotations

import hashlib
import re
from typing import Iterable, List, NamedTuple, Optional, Pattern

from ..http import HttpResponse, join_path
from ..models import Finding, Location, Severity
from ..redact import clean_evidence
from .base import ScannerContext, register
from .env_http import EnvironmentScanner


class Probe(NamedTuple):
    path: str
    title: str
    severity: Severity
    explanation: str
    remediation: str
    #: Content the response must match to count as a real exposure.
    validator: Optional[Pattern[str]] = None


_JSON_OR_YAML = re.compile(r"[:{=]")

PROBES: List[Probe] = [
    Probe(
        "/.git/HEAD",
        "Git repository is exposed over HTTP",
        Severity.CRITICAL,
        "The .git directory is served by the web server, so the entire source history — including "
        "any credential ever committed — can be downloaded and reconstructed.",
        "Block dot-directories at the web server, and deploy build output rather than a working clone.",
        re.compile(r"^(ref:|[0-9a-f]{40})", re.I),
    ),
    Probe(
        "/.git/config",
        "Git configuration is exposed over HTTP",
        Severity.CRITICAL,
        "The repository configuration is downloadable, revealing remote URLs and sometimes embedded "
        "credentials.",
        "Block access to .git at the web server or reverse proxy.",
        re.compile(r"\[core\]|\[remote", re.I),
    ),
    Probe(
        "/.env",
        "Environment file is exposed over HTTP",
        Severity.CRITICAL,
        "A .env file typically holds database passwords, API keys and signing secrets in plain text.",
        "Remove the file from the web root, block dotfiles, and load configuration from the process "
        "environment or a secret manager.",
        re.compile(r"^[A-Z0-9_]+\s*=", re.M),
    ),
    Probe(
        "/.svn/entries",
        "Subversion metadata is exposed over HTTP",
        Severity.HIGH,
        "Version-control metadata allows source reconstruction.",
        "Block .svn at the web server and deploy build artefacts only.",
    ),
    Probe(
        "/.DS_Store",
        "macOS directory index file is exposed",
        Severity.LOW,
        "A .DS_Store file reveals the names of files in the deployed directory, aiding further probing.",
        "Exclude .DS_Store from deployments and block it at the web server.",
    ),
    Probe(
        "/server-status",
        "Apache server-status page is exposed",
        Severity.HIGH,
        "The status page lists active requests with their URLs and client addresses, leaking session "
        "identifiers and internal endpoints in real time.",
        "Restrict mod_status to localhost, or disable it.",
        re.compile(r"Apache Server Status|Server uptime", re.I),
    ),
    Probe(
        "/server-info",
        "Apache server-info page is exposed",
        Severity.MEDIUM,
        "The page discloses the server's full module and configuration inventory.",
        "Disable mod_info or restrict it to localhost.",
        re.compile(r"Apache Server Information", re.I),
    ),
    Probe(
        "/actuator",
        "Spring Boot Actuator root is exposed",
        Severity.HIGH,
        "Actuator endpoints expose configuration, environment variables, heap dumps and, in some "
        "configurations, remote shutdown.",
        "Expose only /actuator/health, secure the management port, and bind it to an internal interface.",
        re.compile(r'"_links"|"self"', re.I),
    ),
    Probe(
        "/actuator/env",
        "Spring Boot Actuator environment endpoint is exposed",
        Severity.CRITICAL,
        "The env endpoint prints the full application environment, which routinely contains "
        "credentials and connection strings.",
        "Disable the env endpoint, or restrict management endpoints to an authenticated internal port.",
        re.compile(r'"propertySources"|"activeProfiles"', re.I),
    ),
    Probe(
        "/actuator/heapdump",
        "Spring Boot heap dump endpoint is exposed",
        Severity.CRITICAL,
        "A heap dump contains in-memory secrets, session tokens and user data, downloadable by anyone.",
        "Disable the heapdump endpoint entirely in any deployed environment.",
    ),
    Probe(
        "/debug/pprof/",
        "Go pprof debug endpoints are exposed",
        Severity.HIGH,
        "pprof exposes memory and goroutine profiles that leak internal state, and profiling can be "
        "used to degrade the service.",
        "Remove the pprof handler from the public mux, or bind it to localhost only.",
        re.compile(r"Types of profiles available|goroutine", re.I),
    ),
    Probe(
        "/metrics",
        "Prometheus metrics endpoint is publicly readable",
        Severity.MEDIUM,
        "Metrics reveal internal endpoints, request volumes, error rates and deployment topology, "
        "which supports targeted attacks and capacity abuse.",
        "Restrict /metrics to the monitoring network or require authentication.",
        re.compile(r"^# (HELP|TYPE) ", re.M),
    ),
    Probe(
        "/phpinfo.php",
        "phpinfo() page is exposed",
        Severity.HIGH,
        "phpinfo() discloses the full PHP configuration, loaded modules, paths and environment "
        "variables.",
        "Delete the file from the deployment.",
        re.compile(r"phpinfo\(\)|PHP Version", re.I),
    ),
    Probe(
        "/swagger-ui.html",
        "Interactive API documentation is publicly available",
        Severity.MEDIUM,
        "A published API surface tells an attacker exactly which endpoints and parameters exist. "
        "That is fine for a public API and a problem for an internal one.",
        "Require authentication for API documentation in deployed environments, or disable it.",
        re.compile(r"swagger|openapi", re.I),
    ),
    Probe(
        "/openapi.json",
        "OpenAPI specification is publicly available",
        Severity.MEDIUM,
        "The specification enumerates every endpoint, parameter and schema, including endpoints that "
        "are not linked from the UI.",
        "Serve the specification only to authenticated users in deployed environments.",
        re.compile(r'"openapi"|"swagger"', re.I),
    ),
    Probe(
        "/v3/api-docs",
        "Springdoc API specification is publicly available",
        Severity.MEDIUM,
        "The specification enumerates every endpoint and schema in the service.",
        "Restrict api-docs to authenticated users, or disable it outside development.",
        re.compile(r'"openapi"', re.I),
    ),
    Probe(
        "/graphql",
        "GraphQL endpoint is reachable",
        Severity.LOW,
        "A reachable GraphQL endpoint should be checked for introspection and for query-depth limits; "
        "MARKNA only confirmed that it answers.",
        "Disable introspection outside development and enforce depth/complexity limits and authentication.",
        re.compile(r"graphql|query|errors", re.I),
    ),
    Probe(
        "/admin",
        "Administrative interface is reachable",
        Severity.MEDIUM,
        "An administrative UI is reachable from this network. Administrative surfaces should not be "
        "exposed to the same population as the application itself.",
        "Restrict admin routes by network (VPN/IP allow-list) and require strong, separate authentication.",
    ),
    Probe(
        "/phpmyadmin/",
        "phpMyAdmin is reachable",
        Severity.HIGH,
        "A database administration console is exposed. These are targeted continuously by automated "
        "credential-stuffing.",
        "Remove phpMyAdmin from deployed environments, or bind it to localhost behind a bastion.",
        re.compile(r"phpMyAdmin", re.I),
    ),
    Probe(
        "/elmah.axd",
        "ELMAH error log is exposed",
        Severity.HIGH,
        "ELMAH publishes application error logs including stack traces, query strings and cookies.",
        "Restrict elmah.axd to localhost or remove it from the deployment.",
        re.compile(r"Error Log for", re.I),
    ),
    Probe(
        "/backup.zip",
        "A backup archive is downloadable",
        Severity.CRITICAL,
        "A backup archive in the web root exposes source, configuration and often database contents.",
        "Remove backups from the web root and store them outside any served directory.",
    ),
    Probe(
        "/dump.sql",
        "A database dump is downloadable",
        Severity.CRITICAL,
        "A SQL dump in the web root exposes the full contents of the database.",
        "Remove the dump and store backups outside the web root.",
        re.compile(r"(INSERT INTO|CREATE TABLE|DROP TABLE)", re.I),
    ),
    Probe(
        "/config.json",
        "An application configuration file is downloadable",
        Severity.HIGH,
        "Configuration files in the web root frequently contain endpoints, keys and credentials.",
        "Move configuration out of the served directory; keep secrets in a secret manager.",
        _JSON_OR_YAML,
    ),
    Probe(
        "/appsettings.json",
        "ASP.NET appsettings.json is downloadable",
        Severity.CRITICAL,
        "appsettings.json normally holds connection strings and API keys.",
        "Ensure the file is excluded from static serving, and move secrets to user-secrets or a vault.",
        re.compile(r"ConnectionStrings|Logging", re.I),
    ),
]


@register
class ExposureScanner(EnvironmentScanner):
    name = "env-exposure"
    capabilities = ("information-exposure",)
    description = (
        "Requests a fixed list of well-known sensitive paths (read-only GET) with soft-404 baselining."
    )

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        assert ctx.http is not None
        target = ctx.target_url or ""
        baseline = self._baseline(ctx)
        findings: List[Finding] = []

        probes = PROBES[: int(ctx.setting(self.name, "max_paths", len(PROBES)))]
        for probe in probes:
            url = join_path(target, probe.path)
            if ctx.scope and not ctx.scope.allows(url):
                continue
            response = ctx.http.get(url)
            if response.error or response.status != 200:
                continue
            if self._matches_baseline(response, baseline):
                continue
            body = response.text(limit=2000)
            if probe.validator and not probe.validator.search(body):
                continue
            findings.append(self._exposure_finding(probe, url, response))

        findings.extend(self._directory_listing(ctx, target))
        findings.extend(self._verbose_errors(ctx, target, baseline))
        findings.extend(self._security_txt(ctx, target))
        return findings

    # ---------------------------------------------------------------- helpers

    def _baseline(self, ctx: ScannerContext) -> Optional[HttpResponse]:
        """Fetch a path that cannot exist, to recognise catch-all 200 responses."""
        assert ctx.http is not None
        url = join_path(ctx.target_url or "", "/markna-baseline-probe-4f3a9c1e")
        response = ctx.http.get(url)
        return None if response.error else response

    def _matches_baseline(self, response: HttpResponse, baseline: Optional[HttpResponse]) -> bool:
        if baseline is None or baseline.status != 200:
            return False
        if response.status != baseline.status:
            return False
        if _digest(response) == _digest(baseline):
            return True
        # SPA shells differ only by a few bytes between routes.
        return abs(len(response.body) - len(baseline.body)) <= 64

    def _exposure_finding(self, probe: Probe, url: str, response: HttpResponse) -> Finding:
        return self.finding(
            severity=probe.severity,
            title=probe.title,
            explanation=probe.explanation,
            evidence=clean_evidence(
                f"GET {url} -> {response.status}\n"
                f"content-type: {response.header('content-type')}\n"
                f"content-length: {len(response.body)}\n\n"
                f"{response.text(limit=800)}",
                limit=2000,
            ),
            location=Location(url=url),
            remediation=probe.remediation,
            rule_id=f"env-exposure{probe.path.rstrip('/')}",
            cwe=["CWE-200"],
            tags=["exposure", "information-disclosure"],
        )

    def _directory_listing(self, ctx: ScannerContext, target: str) -> List[Finding]:
        assert ctx.http is not None
        response = ctx.http.get(target)
        if response.error:
            return []
        body = response.text(limit=4000)
        if not re.search(r"<title>\s*Index of /|Directory listing for /", body, re.I):
            return []
        return [
            self.finding(
                severity=Severity.MEDIUM,
                title="Directory listing is enabled",
                explanation=(
                    "The web server returns an index of files instead of a page, revealing every "
                    "file in the directory including any that were never meant to be linked."
                ),
                evidence=clean_evidence(body[:800]),
                location=Location(url=response.url),
                remediation="Disable automatic directory indexes (`autoindex off` / `Options -Indexes`).",
                rule_id="env-exposure/directory-listing",
                cwe=["CWE-548"],
                tags=["exposure"],
            )
        ]

    def _verbose_errors(
        self, ctx: ScannerContext, target: str, baseline: Optional[HttpResponse]
    ) -> List[Finding]:
        if baseline is None:
            return []
        body = baseline.text(limit=4000)
        patterns = [
            (r"Traceback \(most recent call last\)", "Python traceback"),
            (r"at [\w.$]+\([\w.]+\.java:\d+\)", "Java stack trace"),
            (r"System\.\w+Exception", ".NET exception"),
            (r"SQLSTATE\[|ORA-\d{5}|SQL syntax.*MySQL", "database error"),
            (r"Werkzeug Debugger|Whitespace: |console locked", "interactive debugger"),
            (r"<b>Warning</b>:|<b>Fatal error</b>:", "PHP error output"),
        ]
        for pattern, label in patterns:
            if re.search(pattern, body, re.I):
                return [
                    self.finding(
                        severity=Severity.HIGH if "debugger" in label else Severity.MEDIUM,
                        title=f"Application returns verbose error output ({label})",
                        explanation=(
                            "An unhandled error rendered internal detail — file paths, framework "
                            "versions, SQL fragments or a live debugger — to an anonymous client. "
                            "This hands an attacker the internal structure of the application, and "
                            "an exposed interactive debugger is remote code execution."
                        ),
                        evidence=clean_evidence(
                            f"GET {baseline.url} -> {baseline.status}\n\n{body[:800]}"
                        ),
                        location=Location(url=baseline.url),
                        remediation=(
                            "Disable debug mode in deployed environments and return a generic error "
                            "page; log the detail server-side only."
                        ),
                        rule_id="env-exposure/verbose-errors",
                        cwe=["CWE-209", "CWE-489"],
                        tags=["exposure", "information-disclosure"],
                    )
                ]
        return []

    def _security_txt(self, ctx: ScannerContext, target: str) -> List[Finding]:
        assert ctx.http is not None
        url = join_path(target, "/.well-known/security.txt")
        response = ctx.http.get(url)
        if not response.error and response.status == 200 and "contact" in response.text(600).lower():
            return []
        return [
            self.finding(
                severity=Severity.INFO,
                title="No security.txt contact is published",
                explanation=(
                    "There is no machine-readable way for someone who finds a vulnerability to "
                    "report it, which delays disclosure and pushes finders toward public channels."
                ),
                evidence=f"GET {url} -> {response.status or response.error}",
                location=Location(url=url),
                remediation=(
                    "Publish /.well-known/security.txt with a Contact address and an Expires date "
                    "(RFC 9116)."
                ),
                rule_id="env-exposure/no-security-txt",
                tags=["exposure", "disclosure-process"],
            )
        ]


def _digest(response: HttpResponse) -> str:
    return hashlib.sha256(response.body).hexdigest()
