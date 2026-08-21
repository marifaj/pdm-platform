"""HTTP response security checks: headers, cookies, CORS and enabled methods.

These are the controls a browser enforces on the application's behalf. They are
cheap to verify, cheap to fix, and their absence is the most common finding on a
UAT environment that was stood up quickly.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from ..http import HttpResponse
from ..models import Finding, Layer, Location, Severity
from ..redact import clean_evidence
from .base import Scanner, ScannerContext, register

#: Cookie names that usually carry an authenticated session.
_SESSION_COOKIE = re.compile(
    r"(?i)(sess|sid$|^sid|auth|token|jwt|login|remember|csrf|xsrf|\.asp|jsessionid|phpsessid|connect\.sid)"
)

_PROBE_ORIGIN = "https://markna-security-gate.invalid"


class EnvironmentScanner(Scanner):
    """Shared applicability rules for scanners that talk to the environment."""

    layer = Layer.ENVIRONMENT

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.target_url:
            return False, "no environment URL supplied"
        if not ctx.authorization:
            return False, "environment testing requires recorded authorisation"
        if ctx.http is None:
            return False, "no HTTP client was constructed for this run"
        return True, ""

    def fetch(self, ctx: ScannerContext, url: Optional[str] = None) -> HttpResponse:
        assert ctx.http is not None
        response = ctx.http.get(url or ctx.target_url or "")
        if response.error:
            raise RuntimeError(f"could not reach the environment: {response.error}")
        return response


@register
class HttpHeadersScanner(EnvironmentScanner):
    name = "env-http-headers"
    capabilities = ("http-security-headers", "cookie-security")
    description = "Browser-enforced security response headers and session cookie attributes."

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        response = self.fetch(ctx)
        url = response.url
        is_https = urlparse(url).scheme == "https"
        headers = response.header_map()
        content_type = headers.get("content-type", "")
        is_html = "text/html" in content_type.lower()
        evidence = clean_evidence(response.header_block())

        findings: List[Finding] = []
        findings.extend(self._hsts(url, headers, is_https, evidence))
        findings.extend(self._csp(url, headers, is_html, evidence))
        findings.extend(self._framing(url, headers, is_html, evidence))
        findings.extend(self._simple_headers(url, headers, is_html, evidence))
        findings.extend(self._disclosure(url, headers, evidence))
        findings.extend(self._cookies(url, response, is_https))
        return findings

    # ------------------------------------------------------------------ HSTS

    def _hsts(self, url: str, headers: Dict[str, str], is_https: bool, evidence: str) -> List[Finding]:
        if not is_https:
            return []
        value = headers.get("strict-transport-security")
        if not value:
            return [
                self.finding(
                    severity=Severity.MEDIUM,
                    title="Strict-Transport-Security header is missing",
                    explanation=(
                        "Without HSTS a browser will still try plaintext HTTP for this host, so an "
                        "attacker on the network can strip TLS from the first request of every "
                        "session and from any typed URL."
                    ),
                    evidence=evidence,
                    location=Location(url=url),
                    remediation=(
                        "Send `Strict-Transport-Security: max-age=31536000; includeSubDomains` on "
                        "HTTPS responses once you are confident every subdomain can serve HTTPS."
                    ),
                    rule_id="env-http/hsts-missing",
                    cwe=["CWE-319"],
                    tags=["headers", "transport"],
                )
            ]
        match = re.search(r"max-age\s*=\s*(\d+)", value, re.I)
        max_age = int(match.group(1)) if match else 0
        if max_age < 15_552_000:  # 180 days, the browser-preload minimum
            return [
                self.finding(
                    severity=Severity.LOW,
                    title=f"Strict-Transport-Security max-age is short ({max_age}s)",
                    explanation=(
                        "A short max-age narrows the window in which the browser refuses plaintext, "
                        "and is below the minimum required for HSTS preloading."
                    ),
                    evidence=f"Strict-Transport-Security: {value}",
                    location=Location(url=url),
                    remediation="Raise max-age to at least 15552000 (180 days); 31536000 is typical.",
                    rule_id="env-http/hsts-short-max-age",
                    tags=["headers", "transport"],
                )
            ]
        return []

    # ------------------------------------------------------------------- CSP

    def _csp(self, url: str, headers: Dict[str, str], is_html: bool, evidence: str) -> List[Finding]:
        value = headers.get("content-security-policy")
        if not value:
            if not is_html:
                return []
            return [
                self.finding(
                    severity=Severity.MEDIUM,
                    title="Content-Security-Policy header is missing",
                    explanation=(
                        "CSP is the control that limits the damage of an injected script: without "
                        "it, any XSS in the application runs with the full privileges of the page "
                        "and can exfiltrate to any host."
                    ),
                    evidence=evidence,
                    location=Location(url=url),
                    remediation=(
                        "Start with `default-src 'self'; frame-ancestors 'none'; object-src 'none'` "
                        "in report-only mode, fix what it reports, then enforce."
                    ),
                    rule_id="env-http/csp-missing",
                    cwe=["CWE-1021", "CWE-79"],
                    tags=["headers", "xss"],
                )
            ]

        weaknesses = []
        if "unsafe-inline" in value:
            weaknesses.append("'unsafe-inline' permits inline scripts, which defeats most of CSP's XSS protection")
        if "unsafe-eval" in value:
            weaknesses.append("'unsafe-eval' permits eval(), re-enabling a class of injection sinks")
        if re.search(r"(default|script)-src[^;]*\*(?!\.)", value):
            weaknesses.append("a wildcard source allows scripts from any host")
        if not weaknesses:
            return []
        return [
            self.finding(
                severity=Severity.MEDIUM,
                title="Content-Security-Policy contains weakening directives",
                explanation=(
                    "The policy is present but permissive:\n- " + "\n- ".join(weaknesses)
                ),
                evidence=f"Content-Security-Policy: {value[:1500]}",
                location=Location(url=url),
                remediation=(
                    "Replace inline scripts with nonces or hashes, drop 'unsafe-eval', and pin "
                    "explicit hosts instead of wildcards."
                ),
                rule_id="env-http/csp-weak",
                cwe=["CWE-79"],
                tags=["headers", "xss"],
            )
        ]

    # ---------------------------------------------------------------- framing

    def _framing(self, url: str, headers: Dict[str, str], is_html: bool, evidence: str) -> List[Finding]:
        if not is_html:
            return []
        csp = headers.get("content-security-policy", "")
        if "frame-ancestors" in csp or headers.get("x-frame-options"):
            return []
        return [
            self.finding(
                severity=Severity.MEDIUM,
                title="No clickjacking protection (X-Frame-Options / frame-ancestors)",
                explanation=(
                    "The page can be framed by any site, so a third party can overlay it and trick "
                    "an authenticated user into clicking controls they cannot see."
                ),
                evidence=evidence,
                location=Location(url=url),
                remediation=(
                    "Send `Content-Security-Policy: frame-ancestors 'none'` (or 'self'), and "
                    "`X-Frame-Options: DENY` for older browsers."
                ),
                rule_id="env-http/no-framing-protection",
                cwe=["CWE-1021"],
                tags=["headers", "clickjacking"],
            )
        ]

    # -------------------------------------------------------- simple headers

    def _simple_headers(
        self, url: str, headers: Dict[str, str], is_html: bool, evidence: str
    ) -> List[Finding]:
        checks: List[Tuple[str, str, Severity, str, str, str]] = [
            (
                "x-content-type-options",
                "X-Content-Type-Options: nosniff is missing",
                Severity.LOW,
                "Browsers may MIME-sniff a response and execute it as a different content type than "
                "declared, turning an uploaded file or a JSON endpoint into a script.",
                "Send `X-Content-Type-Options: nosniff` on every response.",
                "env-http/no-sniff-missing",
            ),
            (
                "referrer-policy",
                "Referrer-Policy header is missing",
                Severity.LOW,
                "Full URLs — including any identifiers or tokens in the path or query — are sent to "
                "third-party sites in the Referer header.",
                "Send `Referrer-Policy: strict-origin-when-cross-origin` or stricter.",
                "env-http/referrer-policy-missing",
            ),
            (
                "permissions-policy",
                "Permissions-Policy header is missing",
                Severity.INFO,
                "Powerful browser features (camera, microphone, geolocation) are not explicitly "
                "denied, so any injected or framed content may request them.",
                "Send `Permissions-Policy: camera=(), microphone=(), geolocation=()` and allow only what the app uses.",
                "env-http/permissions-policy-missing",
            ),
        ]
        findings = []
        for header, title, severity, explanation, remediation, rule_id in checks:
            if headers.get(header):
                continue
            if header != "x-content-type-options" and not is_html:
                continue
            findings.append(
                self.finding(
                    severity=severity,
                    title=title,
                    explanation=explanation,
                    evidence=evidence,
                    location=Location(url=url),
                    remediation=remediation,
                    rule_id=rule_id,
                    tags=["headers"],
                )
            )
        return findings

    # ------------------------------------------------------------ disclosure

    def _disclosure(self, url: str, headers: Dict[str, str], evidence: str) -> List[Finding]:
        disclosed = []
        for header in ("server", "x-powered-by", "x-aspnet-version", "x-aspnetmvc-version", "x-generator"):
            value = headers.get(header)
            if value and re.search(r"\d+\.\d+", value):
                disclosed.append(f"{header}: {value}")
        if not disclosed:
            return []
        return [
            self.finding(
                severity=Severity.LOW,
                title="Response headers disclose software versions",
                explanation=(
                    "Version banners let an attacker match the deployment against public exploit "
                    "databases without touching the application. This is not a vulnerability by "
                    "itself, but it removes the reconnaissance step."
                ),
                evidence="\n".join(disclosed),
                location=Location(url=url),
                remediation=(
                    "Suppress or genericise version banners at the web server or reverse proxy "
                    "(`server_tokens off` in nginx, `ServerTokens Prod` in Apache)."
                ),
                rule_id="env-http/version-disclosure",
                cwe=["CWE-200"],
                tags=["headers", "information-disclosure"],
            )
        ]

    # --------------------------------------------------------------- cookies

    def _cookies(self, url: str, response: HttpResponse, is_https: bool) -> List[Finding]:
        findings: List[Finding] = []
        for raw in response.header_values("set-cookie"):
            name = raw.split("=", 1)[0].strip()
            attributes = {
                part.strip().split("=", 1)[0].lower() for part in raw.split(";")[1:]
            }
            same_site = re.search(r"(?i)samesite\s*=\s*(\w+)", raw)
            session_like = bool(_SESSION_COOKIE.search(name))
            redacted = clean_evidence(raw)
            issues: List[Tuple[str, Severity, str, str, str]] = []

            if is_https and "secure" not in attributes:
                issues.append(
                    (
                        "secure-missing",
                        Severity.HIGH if session_like else Severity.MEDIUM,
                        f"Cookie '{name}' is set without the Secure attribute",
                        "The browser will send this cookie over plaintext HTTP, so a network "
                        "attacker can capture it by forcing a single unencrypted request.",
                        "Add the Secure attribute to every cookie set over HTTPS.",
                    )
                )
            if session_like and "httponly" not in attributes:
                issues.append(
                    (
                        "httponly-missing",
                        Severity.HIGH,
                        f"Session cookie '{name}' is readable by JavaScript (no HttpOnly)",
                        "Any XSS in the application can read the session cookie and hand the "
                        "session to an attacker, turning a scripting bug into account takeover.",
                        "Add the HttpOnly attribute to session cookies.",
                    )
                )
            if not same_site:
                issues.append(
                    (
                        "samesite-missing",
                        Severity.MEDIUM if session_like else Severity.LOW,
                        f"Cookie '{name}' has no SameSite attribute",
                        "Without SameSite the cookie is attached to cross-site requests, which is "
                        "the precondition for cross-site request forgery.",
                        "Set SameSite=Lax (or Strict), and pair SameSite=None with Secure.",
                    )
                )
            elif same_site.group(1).lower() == "none" and "secure" not in attributes:
                issues.append(
                    (
                        "samesite-none-insecure",
                        Severity.HIGH,
                        f"Cookie '{name}' uses SameSite=None without Secure",
                        "SameSite=None permits cross-site sending; without Secure the cookie also "
                        "travels in plaintext. Browsers increasingly reject this combination outright.",
                        "Add Secure, or change SameSite to Lax.",
                    )
                )

            for rule, severity, title, explanation, remediation in issues:
                findings.append(
                    self.finding(
                        severity=severity,
                        title=title,
                        explanation=explanation,
                        evidence=redacted,
                        location=Location(url=url, component=f"cookie:{name}"),
                        remediation=remediation,
                        rule_id=f"env-http/cookie/{rule}",
                        cwe=["CWE-614"] if "secure" in rule else ["CWE-1004"],
                        tags=["cookies"],
                    )
                )
        return findings


@register
class CorsScanner(EnvironmentScanner):
    name = "env-cors"
    capabilities = ("cors",)
    description = "Cross-origin resource sharing policy: origin reflection and credentialed wildcards."

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        assert ctx.http is not None
        url = ctx.target_url or ""
        response = ctx.http.get(url, headers={"Origin": _PROBE_ORIGIN})
        if response.error:
            raise RuntimeError(f"could not reach the environment: {response.error}")

        allow_origin = response.header("access-control-allow-origin")
        allow_credentials = str(response.header("access-control-allow-credentials") or "").lower() == "true"
        if not allow_origin:
            return []

        evidence = clean_evidence(
            f"request Origin: {_PROBE_ORIGIN}\n"
            f"Access-Control-Allow-Origin: {allow_origin}\n"
            f"Access-Control-Allow-Credentials: {allow_credentials}"
        )
        reflected = allow_origin.strip() == _PROBE_ORIGIN

        if reflected and allow_credentials:
            return [
                self.finding(
                    severity=Severity.HIGH,
                    title="CORS reflects any origin and allows credentials",
                    explanation=(
                        "The server echoed an arbitrary origin back in Access-Control-Allow-Origin "
                        "while allowing credentials. Any website a logged-in user visits can read "
                        "authenticated responses from this application."
                    ),
                    evidence=evidence,
                    location=Location(url=url),
                    remediation=(
                        "Validate the Origin header against an explicit allow-list and echo only "
                        "matching origins. Never combine credentials with a reflected origin."
                    ),
                    rule_id="env-cors/reflected-origin-with-credentials",
                    cwe=["CWE-942"],
                    tags=["cors"],
                )
            ]
        if reflected:
            return [
                self.finding(
                    severity=Severity.MEDIUM,
                    title="CORS reflects arbitrary origins",
                    explanation=(
                        "The server echoes whatever Origin it is sent. Any site can read responses "
                        "from this application; today that is limited to unauthenticated data, but "
                        "it becomes account-level exposure the moment credentials are allowed."
                    ),
                    evidence=evidence,
                    location=Location(url=url),
                    remediation="Echo only origins from an explicit allow-list.",
                    rule_id="env-cors/reflected-origin",
                    cwe=["CWE-942"],
                    tags=["cors"],
                )
            ]
        if allow_origin.strip() == "*" and allow_credentials:
            return [
                self.finding(
                    severity=Severity.HIGH,
                    title="CORS allows any origin together with credentials",
                    explanation=(
                        "Access-Control-Allow-Origin '*' combined with credentials is rejected by "
                        "browsers, but signals an intent that will be implemented unsafely as soon "
                        "as someone 'fixes' the rejection by reflecting the origin instead."
                    ),
                    evidence=evidence,
                    location=Location(url=url),
                    remediation="Use an explicit origin allow-list; do not send credentials cross-origin unless required.",
                    rule_id="env-cors/wildcard-with-credentials",
                    cwe=["CWE-942"],
                    tags=["cors"],
                )
            ]
        return []


@register
class HttpMethodsScanner(EnvironmentScanner):
    name = "env-http-methods"
    capabilities = ("http-methods",)
    description = "Enumerates advertised HTTP methods via OPTIONS (no state-changing request is sent)."

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        assert ctx.http is not None
        url = ctx.target_url or ""
        response = ctx.http.options(url)
        if response.error:
            return []  # OPTIONS refused is a perfectly good outcome

        allow = response.header("allow") or response.header("access-control-allow-methods") or ""
        methods = {method.strip().upper() for method in allow.split(",") if method.strip()}
        if not methods:
            return []

        risky = methods & {"TRACE", "TRACK", "PUT", "DELETE", "PATCH", "CONNECT"}
        if not risky:
            return []

        severity = Severity.MEDIUM if methods & {"TRACE", "TRACK", "PUT", "DELETE"} else Severity.LOW
        return [
            self.finding(
                severity=severity,
                title=f"Server advertises risky HTTP methods: {', '.join(sorted(risky))}",
                explanation=(
                    "The endpoint advertises methods beyond what a read-only client needs. TRACE/TRACK "
                    "enable cross-site tracing; PUT/DELETE/PATCH indicate write operations that must "
                    "be authenticated and authorised. MARKNA only enumerated the advertised list — it "
                    "did not invoke any of these methods."
                ),
                evidence=clean_evidence(f"Allow: {allow}\n{response.header_block()}"),
                location=Location(url=url),
                remediation=(
                    "Disable TRACE/TRACK at the web server. Confirm that every write method is "
                    "authenticated, authorised and CSRF-protected, and remove those not in use."
                ),
                rule_id="env-http/risky-methods",
                cwe=["CWE-16"],
                tags=["http-methods"],
            )
        ]
