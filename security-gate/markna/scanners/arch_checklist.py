"""Architecture document ingestion and threat-model checklist.

Reads the architecture description as text and applies two deterministic passes:

1. **Topic coverage** — does the document actually address trust boundaries,
   authentication, authorization, sensitive-data flows, external integrations,
   secrets handling, network exposure, threat modelling, logging and recovery?
   A topic the document never mentions is a topic nobody reviewed.
2. **Stated-risk detection** — phrases that describe an accepted weakness
   ("no authentication", "http://", "self-signed", "runs as root") are lifted out
   with their line number, so a reviewer sees what the design already admits.

Both passes are keyword-driven and reproducible. Interpretation of the prose is
the job of the AI advisory layer, not of this scanner.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from ..models import Finding, Layer, Location, Severity
from ..redact import clean_evidence, redact
from .base import Scanner, ScannerContext, register

TEXT_SUFFIXES = {".md", ".markdown", ".txt", ".rst", ".adoc", ".asciidoc", ".org", ".text", ""}


class Topic:
    """One checklist topic and the vocabulary that shows it was addressed."""

    def __init__(
        self,
        key: str,
        title: str,
        severity: Severity,
        patterns: Sequence[str],
        question: str,
    ) -> None:
        self.key = key
        self.title = title
        self.severity = severity
        self.regex = re.compile("|".join(patterns), re.IGNORECASE)
        self.question = question


#: The checklist. Core design topics are MEDIUM; supporting topics are LOW.
TOPICS: List[Topic] = [
    Topic(
        "trust-boundaries",
        "Trust boundaries",
        Severity.MEDIUM,
        [r"trust\s+boundar", r"trust\s+zone", r"security\s+boundar", r"\bdmz\b", r"attack\s+surface", r"threat\s+boundar"],
        "Where does data cross from a less-trusted context into a more-trusted one, and what is enforced there?",
    ),
    Topic(
        "authentication",
        "Authentication",
        Severity.MEDIUM,
        [r"authenticat", r"\bauthn\b", r"\bsso\b", r"\boauth", r"\bmtls\b", r"mutual tls", r"api[- ]key", r"\bjwt\b", r"login"],
        "How does each component prove who is calling it?",
    ),
    Topic(
        "authorization",
        "Authorization",
        Severity.MEDIUM,
        [r"authoriz", r"authoris", r"\brbac\b", r"\babac\b", r"access control", r"permission", r"\brole[s]?\b", r"least privilege"],
        "Once authenticated, what decides which data and operations a caller may reach?",
    ),
    Topic(
        "sensitive-data",
        "Sensitive data handling",
        Severity.MEDIUM,
        [r"sensitive data", r"\bpii\b", r"personal data", r"data classification", r"confidential", r"\bgdpr\b", r"\bphi\b", r"anonymi[sz]"],
        "What sensitive or personal data does the system hold, and how is it classified?",
    ),
    Topic(
        "data-flow",
        "Data flows",
        Severity.MEDIUM,
        [r"data\s*flow", r"dataflow", r"sequence diagram", r"\bdfd\b", r"pipeline", r"ingest", r"flows? (from|to|between)"],
        "How does data move between components, and which flows leave the system?",
    ),
    Topic(
        "encryption",
        "Encryption in transit and at rest",
        Severity.MEDIUM,
        [r"\btls\b", r"\bssl\b", r"https", r"encrypt", r"\baes\b", r"at rest", r"in transit", r"cipher"],
        "Which channels and stores are encrypted, with what, and which are deliberately not?",
    ),
    Topic(
        "secrets",
        "Secrets and key management",
        Severity.MEDIUM,
        [r"secret", r"credential", r"key management", r"\bvault\b", r"keystore", r"rotat", r"\bkms\b", r"password"],
        "Where do credentials live, how are they injected at runtime, and how are they rotated?",
    ),
    Topic(
        "integrations",
        "External dependencies and integrations",
        Severity.MEDIUM,
        [r"third[- ]party", r"external (service|system|api|provider)", r"integration", r"vendor", r"\bsaas\b", r"upstream", r"downstream (service|system)"],
        "Which external services are relied on, what data reaches them, and how are they authenticated?",
    ),
    Topic(
        "network-exposure",
        "Deployment and network exposure",
        Severity.MEDIUM,
        [r"firewall", r"network", r"\bport[s]?\b", r"ingress", r"\bvpn\b", r"reverse proxy", r"load balancer", r"expos", r"public"],
        "What is reachable, from which network, and what sits in front of it?",
    ),
    Topic(
        "threat-model",
        "Threat model",
        Severity.MEDIUM,
        [r"threat model", r"\bstride\b", r"attack tree", r"threat[s]?\b", r"abuse case", r"adversar"],
        "Which threats were enumerated, and which mitigations were chosen against them?",
    ),
    Topic(
        "logging-monitoring",
        "Logging and monitoring",
        Severity.LOW,
        [r"logging", r"\blogs?\b", r"monitor", r"\baudit\b", r"\bsiem\b", r"alert", r"observab", r"telemetry"],
        "What security-relevant events are recorded, and who looks at them?",
    ),
    Topic(
        "backup-recovery",
        "Backup and recovery",
        Severity.LOW,
        [r"backup", r"restore", r"disaster recovery", r"\brto\b", r"\brpo\b", r"replicat", r"retention"],
        "How is the system recovered after data loss, and how is that tested?",
    ),
    Topic(
        "input-validation",
        "Input validation",
        Severity.LOW,
        [r"validat", r"saniti[sz]", r"schema", r"allow[- ]?list", r"whitelist", r"rate limit", r"\bquota\b"],
        "How is untrusted input constrained before it reaches business logic or a datastore?",
    ),
]


#: Phrases that describe a weakness the design has already accepted.
RiskPattern = Tuple[str, str, Severity, "re.Pattern[str]", str, str]

RISK_PATTERNS: List[RiskPattern] = [
    (
        "no-authentication",
        "Architecture describes an unauthenticated interface",
        Severity.HIGH,
        re.compile(r"(?i)\b(no|without|lacks?|missing|disabled?)\s+(authentication|auth|authn|login)\b|\b(unauthenticated|anonymous)\s+(access|endpoint|api|user)"),
        "The document states that an interface accepts callers without authenticating them.",
        "Add authentication, or record an explicit, dated risk acceptance naming who owns it.",
    ),
    (
        "no-encryption",
        "Architecture describes unencrypted transport or storage",
        Severity.HIGH,
        re.compile(r"(?i)\b(unencrypted|not encrypted|no encryption|plain[- ]?text|clear[- ]?text|without tls|no tls|disable[d]? (tls|ssl))\b"),
        "The document describes data moving or resting without encryption.",
        "Encrypt the channel (TLS 1.2+) or the store, or document the compensating control.",
    ),
    (
        "http-endpoint",
        "Architecture documents a plaintext HTTP endpoint",
        Severity.MEDIUM,
        re.compile(r"http://(?!localhost|127\.0\.0\.1|0\.0\.0\.0|\[::1\])[^\s\)\]\"'>]+"),
        "A non-local http:// URL appears in the architecture description.",
        "Serve the endpoint over HTTPS and redirect plaintext requests.",
    ),
    (
        "bind-all-interfaces",
        "Architecture documents a service bound to all interfaces",
        Severity.MEDIUM,
        re.compile(r"(?<![\w.])0\.0\.0\.0(?![\w.])"),
        "Binding 0.0.0.0 exposes the service on every interface the host has, including public ones.",
        "Bind the loopback or a specific internal interface, and let the proxy or network policy control reachability.",
    ),
    (
        "hardcoded-credentials",
        "Architecture mentions hard-coded or default credentials",
        Severity.HIGH,
        re.compile(r"(?i)\b(hard[- ]?coded|default (password|credential|account)|admin[:/]admin|shared (password|credential|account))\b"),
        "The design relies on credentials that are embedded, shared or left at their defaults.",
        "Move credentials to a secret manager, make them per-identity, and force a change from any default.",
    ),
    (
        "self-signed-certificate",
        "Architecture relies on self-signed or unverified certificates",
        Severity.MEDIUM,
        re.compile(r"(?i)\b(self[- ]signed|skip (certificate|cert) (check|verification)|verify\s*=\s*false|insecure[- ]skip[- ]verify)\b"),
        "Self-signed or unverified certificates remove protection against an active network attacker.",
        "Issue certificates from a CA the clients trust, and keep verification enabled.",
    ),
    (
        "runs-as-root",
        "Architecture describes a process running as root",
        Severity.MEDIUM,
        re.compile(r"(?i)\b(run[s]?|running|execute[sd]?)\s+(as\s+)?(root|administrator)\b|\bprivileged\s+container\b"),
        "A compromise of the process becomes a compromise of the host.",
        "Run as a dedicated unprivileged user, and drop container capabilities.",
    ),
    (
        "public-exposure",
        "Architecture describes a publicly reachable component",
        Severity.MEDIUM,
        re.compile(r"(?i)\b(public(ly)?\s+(accessible|exposed|reachable|available)|open to the internet|internet[- ]facing)\b"),
        "A component is reachable from the public internet, which sets the baseline attacker population.",
        "Confirm the exposure is intended, and that authentication, rate limiting and monitoring are in place.",
    ),
    (
        "insecure-protocol",
        "Architecture references an insecure protocol",
        Severity.MEDIUM,
        re.compile(r"(?i)\b(telnet|ftp://|\bftp\b(?!\s*s)|http\s*1883|mqtt\s*(over\s*)?tcp|port\s*1883|\bsnmpv[12]\b|\bsmbv1\b)"),
        "The protocol offers no confidentiality or integrity, or has known unfixable weaknesses.",
        "Use the secured equivalent (SFTP, MQTT over TLS on 8883, SNMPv3).",
    ),
    (
        "unresolved-security-todo",
        "Architecture leaves a security decision unresolved",
        Severity.LOW,
        re.compile(
            r"(?i)(?:\b(?:tbd|todo|fixme|to be (?:decided|determined|confirmed))\b.{0,80}"
            r"(?:secur|auth|encrypt|secret|access|certificat)"
            r"|(?:secur|auth|encrypt|secret|access|certificat).{0,80}\b(?:tbd|todo|fixme)\b)"
        ),
        "A security-relevant decision is still open in the document being used to authorise the release.",
        "Close the decision, or record it as an accepted risk with an owner and a review date.",
    ),
]


@register
class ArchitectureDocumentScanner(Scanner):
    name = "arch-doc-checklist"
    layer = Layer.ARCHITECTURE
    capabilities = ("architecture-review", "threat-model")
    description = (
        "Ingests architecture documents and applies a deterministic topic-coverage checklist "
        "plus stated-risk detection."
    )

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.architecture_docs:
            return False, "no architecture document supplied (--arch)"
        return True, ""

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        documents: Dict[str, str] = {}
        findings: List[Finding] = []

        for path in ctx.architecture_docs:
            if path.suffix.lower() not in TEXT_SUFFIXES:
                findings.append(self._unsupported_format(path))
                continue
            try:
                documents[str(path)] = path.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                findings.append(self._unreadable(path, exc))

        if not documents:
            findings.append(
                self.finding(
                    severity=Severity.HIGH,
                    title="No architecture document could be ingested",
                    explanation=(
                        "Every supplied document was unreadable or in an unsupported format, so the "
                        "architecture layer reviewed nothing."
                    ),
                    evidence="\n".join(str(path) for path in ctx.architecture_docs),
                    location=Location(component="architecture-documents"),
                    remediation="Supply the architecture description as Markdown, reStructuredText or plain text.",
                    rule_id="arch-doc/no-ingestible-document",
                    tags=["architecture", "ingestion"],
                )
            )
            return findings

        findings.append(self._inventory(documents))
        findings.extend(self._topic_coverage(documents))
        for source, text in documents.items():
            findings.extend(self._stated_risks(source, text))
            findings.extend(self._secrets_in_document(source, text))
        return findings

    # ------------------------------------------------------------- ingestion

    def _inventory(self, documents: Dict[str, str]) -> Finding:
        lines = [
            f"{source}: {len(text.splitlines())} lines, {len(text.split())} words"
            for source, text in documents.items()
        ]
        return self.finding(
            severity=Severity.INFO,
            title=f"Ingested {len(documents)} architecture document(s)",
            explanation=(
                "These documents are the evidence base for the architecture layer. Findings below "
                "refer to them by path and line."
            ),
            evidence="\n".join(lines),
            location=Location(component="architecture-documents"),
            remediation="No action required.",
            rule_id="arch-doc/ingested",
            tags=["architecture", "ingestion"],
        )

    def _unsupported_format(self, path: Path) -> Finding:
        return self.finding(
            severity=Severity.MEDIUM,
            title=f"Architecture document '{path.name}' was not ingested (unsupported format)",
            explanation=(
                "MARKNA reads text formats so that findings can cite exact lines. Binary formats "
                "(PDF, DOCX, images) are not parsed, so this document contributed no evidence."
            ),
            evidence=f"{path} (suffix '{path.suffix or 'none'}')",
            location=Location(file=str(path)),
            remediation=(
                "Export the document to Markdown or plain text, or record its content in the "
                "structured architecture manifest."
            ),
            rule_id="arch-doc/unsupported-format",
            tags=["architecture", "ingestion"],
        )

    def _unreadable(self, path: Path, exc: OSError) -> Finding:
        return self.finding(
            severity=Severity.MEDIUM,
            title=f"Architecture document '{path.name}' could not be read",
            explanation="The file exists in the run configuration but could not be opened.",
            evidence=str(exc),
            location=Location(file=str(path)),
            remediation="Check the path and permissions, then re-run the gate.",
            rule_id="arch-doc/unreadable",
            tags=["architecture", "ingestion"],
        )

    # -------------------------------------------------------- topic coverage

    def _topic_coverage(self, documents: Dict[str, str]) -> List[Finding]:
        combined = "\n".join(documents.values())
        findings = []
        for topic in TOPICS:
            if topic.regex.search(combined):
                continue
            findings.append(
                self.finding(
                    severity=topic.severity,
                    title=f"Architecture does not address: {topic.title}",
                    explanation=(
                        f"None of the ingested documents mention {topic.title.lower()}. "
                        f"The review question this leaves unanswered is: {topic.question} "
                        "A topic absent from the design description has not been designed for, "
                        "reviewed or tested."
                    ),
                    evidence=(
                        f"searched {len(documents)} document(s) for: {topic.regex.pattern[:300]}\n"
                        "no match"
                    ),
                    location=Location(component=f"checklist/{topic.key}"),
                    remediation=(
                        f"Add a section covering {topic.title.lower()} to the architecture "
                        "description, or record it in the structured manifest."
                    ),
                    rule_id=f"arch-doc/topic-missing/{topic.key}",
                    tags=["architecture", "checklist", topic.key],
                )
            )
        return findings

    # --------------------------------------------------------- stated risks

    def _stated_risks(self, source: str, text: str) -> List[Finding]:
        findings = []
        seen: set = set()
        for line_number, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            for key, title, severity, pattern, explanation, remediation in RISK_PATTERNS:
                match = pattern.search(stripped)
                if not match:
                    continue
                if (key, stripped[:120]) in seen:
                    continue
                seen.add((key, stripped[:120]))
                findings.append(
                    self.finding(
                        severity=severity,
                        title=title,
                        explanation=(
                            f"{explanation}\n\nThis is a statement in the architecture description "
                            "itself, not an inference: the design as documented accepts this "
                            "weakness."
                        ),
                        evidence=clean_evidence(f"{Path(source).name}:{line_number}\n{stripped}"),
                        location=Location(file=source, line=line_number),
                        remediation=remediation,
                        rule_id=f"arch-doc/stated-risk/{key}",
                        tags=["architecture", "stated-risk", key],
                        confidence="medium",
                    )
                )
        return findings

    def _secrets_in_document(self, source: str, text: str) -> List[Finding]:
        """Architecture documents are a surprisingly common place to find real keys."""
        from .code_secrets import FALLBACK_SECRET_PATTERNS  # one definition, used in two layers

        findings = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            for rule_id, label, pattern, severity in FALLBACK_SECRET_PATTERNS:
                if rule_id == "assigned-secret":
                    continue  # too noisy in prose
                if not pattern.search(line):
                    continue
                findings.append(
                    self.finding(
                        severity=severity,
                        title=f"Credential material in architecture document: {label}",
                        explanation=(
                            "A value matching a credential format appears in the architecture "
                            "document. Design documents are widely circulated, so treat any real "
                            "credential found here as disclosed."
                        ),
                        evidence=clean_evidence(f"{Path(source).name}:{line_number}\n{redact(line.strip())}"),
                        location=Location(file=source, line=line_number),
                        remediation=(
                            "Remove the value from the document, rotate the credential, and replace "
                            "it with a reference to where the secret is stored."
                        ),
                        rule_id=f"arch-doc/secret/{rule_id}",
                        cwe=["CWE-798"],
                        tags=["architecture", "secrets"],
                        confidence="medium",
                    )
                )
        return findings
