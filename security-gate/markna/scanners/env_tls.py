"""Transport security checks against the deployed environment.

Everything here is a read-only connection: a TLS handshake, a certificate read,
and one plaintext HTTP request to see whether the origin redirects. No payloads,
no fuzzing.
"""

from __future__ import annotations

import re
import socket
import ssl
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from ..models import Finding, Layer, Location, Severity
from ..redact import clean_evidence
from .base import Scanner, ScannerContext, register

#: Protocol versions that must not be negotiable on a system going to UAT.
_LEGACY_PROTOCOLS: List[Tuple[str, int]] = [
    ("TLSv1", getattr(ssl.TLSVersion, "TLSv1", None)),
    ("TLSv1.1", getattr(ssl.TLSVersion, "TLSv1_1", None)),
]

_HANDSHAKE_HINTS = {
    "CERTIFICATE_VERIFY_FAILED": "the certificate chain could not be verified",
    "certificate has expired": "the certificate has expired",
    "self signed certificate": "the certificate is self-signed",
    "self-signed certificate": "the certificate is self-signed",
    "Hostname mismatch": "the certificate does not cover this hostname",
    "unable to get local issuer certificate": "an intermediate CA certificate is missing from the chain",
    "WRONG_VERSION_NUMBER": "the port did not answer with TLS (is the service plaintext?)",
    "UNSUPPORTED_PROTOCOL": "no mutually supported TLS version",
}


@register
class TlsScanner(Scanner):
    name = "env-tls"
    layer = Layer.ENVIRONMENT
    capabilities = ("transport-security",)
    description = "TLS availability, certificate validity, protocol versions and HTTPS redirection."

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.target_url:
            return False, "no environment URL supplied"
        if not ctx.authorization:
            return False, "environment testing requires recorded authorisation"
        return True, ""

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        url = ctx.target_url
        assert url is not None
        parsed = urlparse(url)
        host = parsed.hostname or ""
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        findings: List[Finding] = []

        if parsed.scheme != "https":
            findings.append(self._plaintext_target(url))
            return findings

        verified, error = self._handshake(host, port, verify=True)
        if verified is None:
            unverified, unverified_error = self._handshake(host, port, verify=False)
            if unverified is None:
                findings.append(self._connection_failed(url, error or unverified_error))
                return findings
            findings.append(self._verification_failed(url, error or "", unverified))
            findings.extend(
                self._certificate_findings(ctx, url, unverified, verification_failed=True)
            )
            findings.extend(self._protocol_findings(ctx, host, port, unverified))
            return findings

        findings.extend(self._certificate_findings(ctx, url, verified, verification_failed=False))
        findings.extend(self._protocol_findings(ctx, host, port, verified))
        findings.extend(self._http_redirect(ctx, host, parsed.port))
        return findings

    # ------------------------------------------------------------- handshake

    def _handshake(self, host: str, port: int, *, verify: bool) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        context = ssl.create_default_context()
        if not verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        try:
            with socket.create_connection((host, port), timeout=15) as raw:
                with context.wrap_socket(raw, server_hostname=host) as tls:
                    der = tls.getpeercert(binary_form=True)
                    return (
                        {
                            "certificate": tls.getpeercert() or {},
                            "der": der,
                            "protocol": tls.version(),
                            "cipher": tls.cipher(),
                        },
                        None,
                    )
        except ssl.SSLError as exc:
            return None, f"{type(exc).__name__}: {exc}"
        except (socket.timeout, OSError) as exc:
            return None, f"{type(exc).__name__}: {exc}"

    def _protocol_findings(
        self, ctx: ScannerContext, host: str, port: int, handshake: Dict[str, Any]
    ) -> List[Finding]:
        findings: List[Finding] = []
        negotiated = handshake.get("protocol") or "unknown"
        cipher = handshake.get("cipher") or ("unknown", "unknown", 0)

        if negotiated in ("TLSv1", "TLSv1.1", "SSLv3", "SSLv2"):
            findings.append(
                self.finding(
                    severity=Severity.HIGH,
                    title=f"Endpoint negotiates deprecated {negotiated}",
                    explanation=(
                        f"The default handshake settled on {negotiated}, which is deprecated and no "
                        "longer considered secure by any major browser or compliance regime."
                    ),
                    evidence=f"negotiated: {negotiated}\ncipher: {cipher}",
                    location=Location(url=ctx.target_url),
                    remediation="Configure the server to require TLS 1.2 as a minimum, and prefer TLS 1.3.",
                    rule_id="env-tls/deprecated-protocol",
                    cwe=["CWE-327"],
                    tags=["tls", "transport"],
                )
            )

        for label, version in _LEGACY_PROTOCOLS:
            if version is None:
                continue
            supported = self._supports_protocol(host, port, version)
            if supported is True:
                findings.append(
                    self.finding(
                        severity=Severity.HIGH,
                        title=f"Endpoint still accepts {label}",
                        explanation=(
                            f"{label} is disabled in current browsers and prohibited by PCI DSS. "
                            "Leaving it enabled allows a downgrade to a protocol with known "
                            "weaknesses."
                        ),
                        evidence=f"a {label} handshake to {host}:{port} succeeded",
                        location=Location(url=ctx.target_url),
                        remediation=f"Disable {label} in the TLS configuration; require TLS 1.2 or higher.",
                        rule_id=f"env-tls/legacy-protocol-enabled/{label.lower().replace('.', '-')}",
                        cwe=["CWE-327"],
                        tags=["tls", "transport"],
                    )
                )

        strength = cipher[2] if isinstance(cipher, (list, tuple)) and len(cipher) > 2 else 0
        if isinstance(strength, int) and 0 < strength < 128:
            findings.append(
                self.finding(
                    severity=Severity.HIGH,
                    title=f"Weak cipher negotiated ({cipher[0]}, {strength}-bit)",
                    explanation="The negotiated cipher suite provides less than 128 bits of security.",
                    evidence=f"cipher: {cipher}",
                    location=Location(url=ctx.target_url),
                    remediation="Restrict the cipher list to modern AEAD suites (AES-GCM, ChaCha20-Poly1305).",
                    rule_id="env-tls/weak-cipher",
                    cwe=["CWE-326"],
                    tags=["tls", "transport"],
                )
            )
        return findings

    def _supports_protocol(self, host: str, port: int, version: Any) -> Optional[bool]:
        """True/False if the probe was conclusive, None if the client cannot test."""
        try:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            context.minimum_version = version
            context.maximum_version = version
        except (ValueError, AttributeError, ssl.SSLError):
            return None
        try:
            with socket.create_connection((host, port), timeout=10) as raw:
                with context.wrap_socket(raw, server_hostname=host):
                    return True
        except ssl.SSLError as exc:
            text = str(exc)
            if "NO_PROTOCOLS_AVAILABLE" in text or "no protocols available" in text.lower():
                return None  # this OpenSSL build cannot offer the protocol at all
            return False
        except (socket.timeout, OSError):
            return False

    # ----------------------------------------------------------- certificate

    def _certificate_findings(
        self, ctx: ScannerContext, url: str, handshake: Dict[str, Any], *, verification_failed: bool
    ) -> List[Finding]:
        certificate: Dict[str, Any] = handshake.get("certificate") or {}
        findings: List[Finding] = []
        subject = _name_to_str(certificate.get("subject"))
        issuer = _name_to_str(certificate.get("issuer"))
        not_after = certificate.get("notAfter")

        if not_after:
            expiry = _parse_cert_time(not_after)
            if expiry is not None:
                days = (expiry - datetime.now(timezone.utc)).days
                if days < 0:
                    findings.append(
                        self.finding(
                            severity=Severity.CRITICAL,
                            title=f"TLS certificate expired {abs(days)} day(s) ago",
                            explanation=(
                                "Clients reject the certificate outright or, worse, are trained to "
                                "click through the warning. Either way the channel's authenticity "
                                "guarantee is gone."
                            ),
                            evidence=f"notAfter: {not_after}\nsubject: {subject}\nissuer: {issuer}",
                            location=Location(url=url),
                            remediation="Renew the certificate and automate renewal (ACME or the platform's certificate manager).",
                            rule_id="env-tls/certificate-expired",
                            cwe=["CWE-324"],
                            tags=["tls", "certificate"],
                        )
                    )
                elif days <= 30:
                    findings.append(
                        self.finding(
                            severity=Severity.HIGH if days <= 14 else Severity.MEDIUM,
                            title=f"TLS certificate expires in {days} day(s)",
                            explanation=(
                                "The certificate expires imminently. If UAT runs past that date the "
                                "environment breaks, and an expiry during a demo is indistinguishable "
                                "from an attack to the audience."
                            ),
                            evidence=f"notAfter: {not_after}\nsubject: {subject}",
                            location=Location(url=url),
                            remediation="Renew now and verify that automated renewal is working.",
                            rule_id="env-tls/certificate-expiring",
                            tags=["tls", "certificate"],
                        )
                    )

        if subject and issuer and subject == issuer:
            findings.append(
                self.finding(
                    severity=Severity.HIGH if not verification_failed else Severity.MEDIUM,
                    title="TLS certificate is self-signed",
                    explanation=(
                        "A self-signed certificate cannot be verified by clients, so users and "
                        "integrations must either ignore the warning or disable verification — both "
                        "of which permanently remove protection against interception."
                    ),
                    evidence=f"subject: {subject}\nissuer: {issuer}",
                    location=Location(url=url),
                    remediation=(
                        "Issue the certificate from a CA the clients already trust (public CA, or "
                        "an internal CA distributed to the client trust stores)."
                    ),
                    rule_id="env-tls/self-signed-certificate",
                    cwe=["CWE-295"],
                    tags=["tls", "certificate"],
                )
            )

        findings.extend(self._openssl_details(ctx, url, handshake.get("der")))
        return findings

    def _openssl_details(
        self, ctx: ScannerContext, url: str, der: Optional[bytes]
    ) -> List[Finding]:
        """Key size and signature algorithm, read via the openssl CLI when present."""
        if not der:
            return []
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".der", delete=True) as handle:
            handle.write(der)
            handle.flush()
            result = self.exec(
                ctx,
                ["openssl", "x509", "-inform", "DER", "-in", handle.name, "-noout", "-text"],
                timeout=30,
            )
        if not result.ok:
            return []

        findings: List[Finding] = []
        text = result.stdout
        key_match = re.search(r"Public-Key:\s*\((\d+)\s*bit\)", text)
        algorithm_match = re.search(r"Signature Algorithm:\s*(\S+)", text)
        key_type = "RSA" if "rsaEncryption" in text else ("EC" if "id-ecPublicKey" in text else "unknown")

        if key_match:
            bits = int(key_match.group(1))
            too_small = (key_type == "RSA" and bits < 2048) or (key_type == "EC" and bits < 224)
            if too_small:
                findings.append(
                    self.finding(
                        severity=Severity.HIGH,
                        title=f"TLS certificate uses a weak {key_type} key ({bits}-bit)",
                        explanation="The key is below the current minimum strength and is rejected by modern clients.",
                        evidence=f"public key: {key_type} {bits}-bit",
                        location=Location(url=url),
                        remediation="Reissue with at least RSA-2048 (RSA-3072 preferred) or a P-256 EC key.",
                        rule_id="env-tls/weak-key",
                        cwe=["CWE-326"],
                        tags=["tls", "certificate"],
                    )
                )

        if algorithm_match and re.search(r"(md5|sha1)", algorithm_match.group(1), re.I):
            findings.append(
                self.finding(
                    severity=Severity.HIGH,
                    title=f"TLS certificate signed with {algorithm_match.group(1)}",
                    explanation="MD5 and SHA-1 signatures are forgeable and are rejected by current clients.",
                    evidence=f"signature algorithm: {algorithm_match.group(1)}",
                    location=Location(url=url),
                    remediation="Reissue the certificate with a SHA-256 or stronger signature.",
                    rule_id="env-tls/weak-signature-algorithm",
                    cwe=["CWE-327"],
                    tags=["tls", "certificate"],
                )
            )
        return findings

    # ------------------------------------------------------------- redirects

    def _http_redirect(self, ctx: ScannerContext, host: str, port: Optional[int]) -> List[Finding]:
        """Does the plaintext origin exist, and does it redirect to HTTPS?"""
        if ctx.http is None or port not in (None, 443):
            return []
        plaintext_url = f"http://{host}/"
        if not ctx.scope or not ctx.scope.allows(plaintext_url):
            return []
        response = ctx.http.get(plaintext_url, follow_redirects=False)
        if response.error:
            return []  # plaintext port closed: that is the desired state

        location = response.header("Location") or ""
        if response.status in (301, 302, 303, 307, 308) and location.lower().startswith("https://"):
            return []
        if response.status in (301, 302, 303, 307, 308):
            severity, detail = Severity.MEDIUM, f"redirects to a non-HTTPS location: {location}"
        else:
            severity, detail = Severity.HIGH, f"serves content over plaintext HTTP (status {response.status})"

        return [
            self.finding(
                severity=severity,
                title="Plaintext HTTP is served without redirecting to HTTPS",
                explanation=(
                    f"The origin {detail}. Any client that reaches the site over HTTP first — which "
                    "is what typing a hostname does — exposes that request, its cookies and its "
                    "credentials before TLS is ever established."
                ),
                evidence=clean_evidence(response.header_block()),
                location=Location(url=plaintext_url),
                remediation=(
                    "Return a 301 to the HTTPS origin for every plaintext request, then add "
                    "Strict-Transport-Security so subsequent visits never use HTTP."
                ),
                rule_id="env-tls/no-https-redirect",
                cwe=["CWE-319"],
                tags=["tls", "transport"],
            )
        ]

    # -------------------------------------------------------------- failures

    def _plaintext_target(self, url: str) -> Finding:
        return self.finding(
            severity=Severity.HIGH,
            title="Environment is served over plaintext HTTP",
            explanation=(
                "The assessed URL uses http://, so all traffic — credentials, session cookies, "
                "telemetry and any personal data — travels unencrypted and unauthenticated."
            ),
            evidence=f"target: {url}",
            location=Location(url=url),
            remediation=(
                "Terminate TLS 1.2+ in front of the environment before it is used for UAT, and "
                "redirect plaintext to HTTPS."
            ),
            rule_id="env-tls/no-tls",
            cwe=["CWE-319"],
            tags=["tls", "transport"],
        )

    def _connection_failed(self, url: str, error: Optional[str]) -> Finding:
        return self.finding(
            severity=Severity.MEDIUM,
            title="Could not complete a TLS handshake with the environment",
            explanation=(
                "The environment layer could not establish TLS, so no transport-security evidence "
                "was gathered. This may be a firewall, a stopped service or a broken listener."
            ),
            evidence=clean_evidence(str(error or "unknown error")),
            location=Location(url=url),
            remediation="Confirm the URL, that the environment is running, and that the runner can reach it.",
            rule_id="env-tls/handshake-failed",
            tags=["tls", "connectivity"],
        )

    def _verification_failed(self, url: str, error: str, handshake: Dict[str, Any]) -> Finding:
        hint = next(
            (reason for marker, reason in _HANDSHAKE_HINTS.items() if marker.lower() in error.lower()),
            "the certificate could not be validated",
        )
        return self.finding(
            severity=Severity.HIGH,
            title="TLS certificate fails validation",
            explanation=(
                f"A verifying client rejects this endpoint because {hint}. Users and integrations "
                "must disable verification to use it, which removes the protection TLS was there to "
                "provide."
            ),
            evidence=clean_evidence(
                f"verification error: {error}\n"
                f"negotiated (unverified): {handshake.get('protocol')}\n"
                f"subject: {_name_to_str((handshake.get('certificate') or {}).get('subject'))}\n"
                f"issuer: {_name_to_str((handshake.get('certificate') or {}).get('issuer'))}"
            ),
            location=Location(url=url),
            remediation=(
                "Install a certificate that is valid for this hostname, unexpired, and chained to a "
                "CA the clients trust. Include any intermediate certificates in the chain."
            ),
            rule_id="env-tls/certificate-validation-failed",
            cwe=["CWE-295"],
            tags=["tls", "certificate"],
        )


def _name_to_str(name: Any) -> str:
    """Flatten the nested tuple structure ssl returns for subject/issuer."""
    if not name:
        return ""
    parts = []
    for rdn in name:
        for entry in rdn:
            if isinstance(entry, (list, tuple)) and len(entry) == 2:
                parts.append(f"{entry[0]}={entry[1]}")
    return ", ".join(parts)


def _parse_cert_time(value: str) -> Optional[datetime]:
    for fmt in ("%b %d %H:%M:%S %Y %Z", "%b %d %H:%M:%S %Y"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None
