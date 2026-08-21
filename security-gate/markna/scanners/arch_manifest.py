"""Deterministic architecture review over a structured manifest.

Free-text architecture documents cannot be checked mechanically. A structured
manifest can: MARKNA reads ``architecture.yaml`` — components, trust boundaries,
data flows, data stores, integrations, secrets handling, exposure and the threat
model — and applies fixed rules to it. Every finding here is reproducible and
traceable to a manifest field; no language model is involved.

Run ``markna init`` to generate a commented template.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Set

from ..models import Finding, Layer, Location, Severity
from .base import Scanner, ScannerContext, register

#: Sections a complete architecture description is expected to contain.
REQUIRED_SECTIONS: Dict[str, str] = {
    "system": "System identity, owner and data classification",
    "trust_boundaries": "Where trust changes hands",
    "components": "Services, datastores and clients in scope",
    "data_flows": "How data moves between components",
    "data_stores": "Where data comes to rest",
    "integrations": "External dependencies and third-party services",
    "secrets": "How credentials are stored, injected and rotated",
    "exposure": "What is reachable, from which network, behind what control",
    "threat_model": "Threat enumeration and mitigation status",
}

#: Values that mean "no control is in place".
_NONE_VALUES = {"", "none", "no", "false", "n/a", "na", "unknown", "tbd", "todo"}

_INSECURE_TRANSPORT = {"none", "http", "plaintext", "clear", "cleartext", "tcp", "mqtt"}
_WEAK_SECRET_MANAGEMENT = {
    "hardcoded": Severity.CRITICAL,
    "in-code": Severity.CRITICAL,
    "source-control": Severity.CRITICAL,
    "repository": Severity.CRITICAL,
    "env-file": Severity.HIGH,
    "dotenv": Severity.HIGH,
    "config-file": Severity.HIGH,
    "plain-file": Severity.HIGH,
    "unknown": Severity.HIGH,
    "none": Severity.HIGH,
    "env-var": Severity.MEDIUM,
    "environment-variable": Severity.MEDIUM,
}
_SENSITIVE_CLASSIFICATIONS = {"confidential", "restricted", "secret", "pii", "phi", "regulated"}
_STRIDE = ("spoofing", "tampering", "repudiation", "information_disclosure", "denial_of_service", "elevation_of_privilege")


def _is_none(value: Any) -> bool:
    return str(value or "").strip().lower() in _NONE_VALUES


def _sensitive(classification: Any) -> bool:
    return str(classification or "").strip().lower() in _SENSITIVE_CLASSIFICATIONS


def _entries(manifest: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    value = manifest.get(key)
    if isinstance(value, dict):
        # Allow either a list of objects or a mapping of id -> object.
        return [{"id": k, **(v if isinstance(v, dict) else {"value": v})} for k, v in value.items()]
    if isinstance(value, list):
        return [entry for entry in value if isinstance(entry, dict)]
    return []


def _label(entry: Dict[str, Any], fallback: str = "unnamed") -> str:
    return str(entry.get("name") or entry.get("id") or fallback)


def _yaml_evidence(entry: Dict[str, Any], keys: Iterable[str]) -> str:
    lines = []
    for key in keys:
        if key in entry:
            lines.append(f"{key}: {entry[key]}")
    return "\n".join(lines) or str(entry)[:500]


@register
class ArchitectureManifestScanner(Scanner):
    name = "arch-manifest"
    layer = Layer.ARCHITECTURE
    capabilities = ("architecture-review", "threat-model")
    description = (
        "Deterministic rule engine over a structured architecture manifest: trust boundaries, "
        "authn/authz, sensitive-data flows, integrations, secrets, exposure and threat model."
    )

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.architecture_manifest:
            return False, (
                "no architecture manifest supplied (--arch-manifest); free-text documents alone "
                "cannot be checked deterministically"
            )
        return True, ""

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        manifest = ctx.architecture_manifest or {}
        source = str(ctx.architecture_manifest_path or "architecture manifest")
        findings: List[Finding] = []
        checks: List[Callable[[Dict[str, Any], str], List[Finding]]] = [
            self._check_sections,
            self._check_trust_boundaries,
            self._check_components,
            self._check_data_flows,
            self._check_data_stores,
            self._check_integrations,
            self._check_secrets,
            self._check_exposure,
            self._check_threat_model,
            self._check_logging,
            self._check_model_consistency,
        ]
        for check in checks:
            findings.extend(check(manifest, source))
        return findings

    # ------------------------------------------------------- section coverage

    def _check_sections(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        findings = []
        for section, purpose in REQUIRED_SECTIONS.items():
            value = manifest.get(section)
            if value in (None, {}, [], ""):
                findings.append(
                    self.finding(
                        severity=Severity.MEDIUM,
                        title=f"Architecture manifest does not describe '{section}'",
                        explanation=(
                            f"The '{section}' section ({purpose}) is missing or empty, so the gate "
                            "cannot review that aspect of the design. An undocumented area of the "
                            "architecture is an unreviewed area."
                        ),
                        evidence=f"{source}: section '{section}' absent or empty",
                        location=Location(file=source, component=section),
                        remediation=(
                            f"Populate the '{section}' section of the architecture manifest. "
                            "See the template produced by `markna init`."
                        ),
                        rule_id=f"arch/section-missing/{section}",
                        tags=["architecture", "completeness"],
                    )
                )
        return findings

    # ------------------------------------------------------- trust boundaries

    def _check_trust_boundaries(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        boundaries = _entries(manifest, "trust_boundaries")
        if not boundaries:
            return [
                self.finding(
                    severity=Severity.HIGH,
                    title="No trust boundaries are defined",
                    explanation=(
                        "Trust boundaries are where an attacker's reachable surface meets your "
                        "controls. Without them, no statement about authentication, encryption or "
                        "validation can be scoped, and the threat model has no structure."
                    ),
                    evidence=f"{source}: trust_boundaries is empty",
                    location=Location(file=source, component="trust_boundaries"),
                    remediation=(
                        "Enumerate each boundary (device→gateway, browser→API, service→database, "
                        "internal→third party) and record the controls enforced at each one."
                    ),
                    rule_id="arch/no-trust-boundaries",
                    tags=["architecture", "trust-boundary"],
                )
            ]
        findings = []
        for boundary in boundaries:
            controls = boundary.get("controls") or []
            if not controls:
                findings.append(
                    self.finding(
                        severity=Severity.MEDIUM,
                        title=f"Trust boundary '{_label(boundary)}' declares no controls",
                        explanation=(
                            "A boundary with no enforcement is a boundary in name only. Record what "
                            "actually stops an untrusted caller here: authentication, mutual TLS, "
                            "network policy, input validation, rate limiting."
                        ),
                        evidence=_yaml_evidence(boundary, ("id", "name", "description", "controls")),
                        location=Location(file=source, component=f"trust_boundaries/{_label(boundary)}"),
                        remediation="Add a 'controls' list describing the enforcement at this boundary.",
                        rule_id="arch/boundary-without-controls",
                        tags=["architecture", "trust-boundary"],
                    )
                )
        return findings

    # -------------------------------------------------------------- components

    def _check_components(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        findings = []
        for component in _entries(manifest, "components"):
            name = _label(component)
            internet_facing = bool(component.get("internet_facing"))
            sensitive = bool(component.get("handles_sensitive_data")) or _sensitive(
                component.get("classification")
            )
            authn = component.get("authentication")
            authz = component.get("authorization")

            if internet_facing and _is_none(authn):
                findings.append(
                    self.finding(
                        severity=Severity.CRITICAL,
                        title=f"Internet-facing component '{name}' has no authentication",
                        explanation=(
                            "The manifest marks this component as reachable from the internet while "
                            "declaring no authentication. Anyone who can find the endpoint can use "
                            "it, and every downstream control has to assume an anonymous caller."
                        ),
                        evidence=_yaml_evidence(
                            component,
                            ("id", "name", "type", "internet_facing", "authentication", "authorization"),
                        ),
                        location=Location(file=source, component=f"components/{name}"),
                        remediation=(
                            "Put authentication in front of the component (gateway, reverse proxy, "
                            "or in-app), or move it off the public network. If anonymous access is "
                            "intentional, document exactly which operations are safe unauthenticated."
                        ),
                        rule_id="arch/internet-facing-unauthenticated",
                        cwe=["CWE-306"],
                        tags=["architecture", "authentication", "exposure"],
                    )
                )

            if sensitive and _is_none(authz):
                findings.append(
                    self.finding(
                        severity=Severity.HIGH,
                        title=f"Component '{name}' handles sensitive data with no authorization model",
                        explanation=(
                            "Authentication establishes who is calling; authorization decides what "
                            "they may reach. A component holding sensitive data with no "
                            "authorization model gives every authenticated caller full access."
                        ),
                        evidence=_yaml_evidence(
                            component,
                            ("id", "name", "handles_sensitive_data", "classification", "authentication", "authorization"),
                        ),
                        location=Location(file=source, component=f"components/{name}"),
                        remediation=(
                            "Define the authorization model (RBAC, ownership checks, tenant scoping) "
                            "and state where it is enforced."
                        ),
                        rule_id="arch/sensitive-component-without-authz",
                        cwe=["CWE-862"],
                        tags=["architecture", "authorization"],
                    )
                )

            if not internet_facing and _is_none(authn) and str(component.get("type", "")).lower() in ("service", "api"):
                findings.append(
                    self.finding(
                        severity=Severity.MEDIUM,
                        title=f"Internal service '{name}' has no authentication",
                        explanation=(
                            "The service relies on network position alone. That is acceptable only "
                            "if the network boundary is genuinely enforced and documented; "
                            "otherwise any workload on the same network is fully trusted."
                        ),
                        evidence=_yaml_evidence(
                            component, ("id", "name", "type", "trust_zone", "authentication")
                        ),
                        location=Location(file=source, component=f"components/{name}"),
                        remediation=(
                            "Add service-to-service authentication (mTLS, signed tokens), or "
                            "document the enforced network control that substitutes for it."
                        ),
                        rule_id="arch/internal-service-unauthenticated",
                        cwe=["CWE-306"],
                        tags=["architecture", "authentication"],
                    )
                )
        return findings

    # -------------------------------------------------------------- data flows

    def _check_data_flows(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        findings = []
        for flow in _entries(manifest, "data_flows"):
            name = _label(flow, "flow")
            transport = str(flow.get("encryption_in_transit") or flow.get("transport") or "").lower()
            crosses = flow.get("crosses_boundary")
            classification = flow.get("classification")
            insecure = _is_none(transport) or transport in _INSECURE_TRANSPORT

            if insecure and crosses:
                findings.append(
                    self.finding(
                        severity=Severity.HIGH,
                        title=f"Data flow '{name}' crosses a trust boundary without encryption",
                        explanation=(
                            f"The flow crosses boundary '{crosses}' with transport "
                            f"'{transport or 'unspecified'}'. Anything on the path can read and "
                            "modify the traffic, including any credentials it carries."
                        ),
                        evidence=_yaml_evidence(
                            flow,
                            ("id", "name", "source", "destination", "data", "classification",
                             "crosses_boundary", "encryption_in_transit", "authentication"),
                        ),
                        location=Location(file=source, component=f"data_flows/{name}"),
                        remediation=(
                            "Terminate the flow over TLS 1.2+ (mutual TLS between services), or "
                            "document the compensating control that makes the path trusted."
                        ),
                        rule_id="arch/unencrypted-boundary-crossing",
                        cwe=["CWE-319"],
                        tags=["architecture", "data-flow", "encryption"],
                    )
                )
            elif insecure and _sensitive(classification):
                findings.append(
                    self.finding(
                        severity=Severity.HIGH,
                        title=f"Sensitive data flow '{name}' is unencrypted in transit",
                        explanation=(
                            f"The flow is classified '{classification}' but declares transport "
                            f"'{transport or 'unspecified'}'."
                        ),
                        evidence=_yaml_evidence(
                            flow, ("id", "name", "source", "destination", "classification", "encryption_in_transit")
                        ),
                        location=Location(file=source, component=f"data_flows/{name}"),
                        remediation="Encrypt the flow with TLS 1.2+ end to end.",
                        rule_id="arch/unencrypted-sensitive-flow",
                        cwe=["CWE-319"],
                        tags=["architecture", "data-flow", "encryption"],
                    )
                )

            if _sensitive(classification) and _is_none(flow.get("authentication")):
                findings.append(
                    self.finding(
                        severity=Severity.HIGH,
                        title=f"Sensitive data flow '{name}' is unauthenticated",
                        explanation=(
                            "Sensitive data is exchanged without either side authenticating the "
                            "other, so neither the producer nor the consumer can be trusted."
                        ),
                        evidence=_yaml_evidence(
                            flow, ("id", "name", "source", "destination", "classification", "authentication")
                        ),
                        location=Location(file=source, component=f"data_flows/{name}"),
                        remediation="Authenticate both ends of the flow (mTLS, signed tokens, API keys with rotation).",
                        rule_id="arch/unauthenticated-sensitive-flow",
                        cwe=["CWE-306"],
                        tags=["architecture", "data-flow", "authentication"],
                    )
                )
        return findings

    # ------------------------------------------------------------- data stores

    def _check_data_stores(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        findings = []
        for store in _entries(manifest, "data_stores"):
            name = _label(store, "store")
            contains_pii = bool(store.get("contains_pii"))
            sensitive = contains_pii or _sensitive(store.get("classification"))
            at_rest = store.get("encryption_at_rest")

            if sensitive and _is_none(at_rest):
                findings.append(
                    self.finding(
                        severity=Severity.HIGH,
                        title=f"Data store '{name}' holds sensitive data without encryption at rest",
                        explanation=(
                            "Sensitive or personal data is stored unencrypted. Anyone with host, "
                            "backup or volume access reads it directly, and a lost disk or "
                            "misconfigured snapshot becomes a reportable breach."
                        ),
                        evidence=_yaml_evidence(
                            store,
                            ("id", "name", "classification", "contains_pii", "encryption_at_rest", "backup"),
                        ),
                        location=Location(file=source, component=f"data_stores/{name}"),
                        remediation=(
                            "Enable encryption at rest (managed database encryption, LUKS/BitLocker, "
                            "or application-level field encryption for the sensitive columns)."
                        ),
                        rule_id="arch/sensitive-store-unencrypted",
                        cwe=["CWE-311"],
                        tags=["architecture", "data-at-rest", "encryption"],
                    )
                )

            if sensitive and _is_none(store.get("retention")):
                findings.append(
                    self.finding(
                        severity=Severity.LOW,
                        title=f"Data store '{name}' has no stated retention period",
                        explanation=(
                            "Sensitive data kept indefinitely increases breach impact and may "
                            "conflict with data-protection obligations."
                        ),
                        evidence=_yaml_evidence(store, ("id", "name", "classification", "contains_pii", "retention")),
                        location=Location(file=source, component=f"data_stores/{name}"),
                        remediation="Define and implement a retention and deletion policy for this store.",
                        rule_id="arch/no-retention-policy",
                        tags=["architecture", "data-governance"],
                    )
                )

            if sensitive and store.get("backup") in (False, None) :
                findings.append(
                    self.finding(
                        severity=Severity.LOW,
                        title=f"Data store '{name}' declares no backup",
                        explanation=(
                            "Availability and recoverability are security properties. A store with "
                            "no backup cannot be restored after ransomware or accidental deletion."
                        ),
                        evidence=_yaml_evidence(store, ("id", "name", "backup", "classification")),
                        location=Location(file=source, component=f"data_stores/{name}"),
                        remediation="Define backup frequency, storage location and a tested restore procedure.",
                        rule_id="arch/no-backup",
                        tags=["architecture", "availability"],
                    )
                )
        return findings

    # ------------------------------------------------------------ integrations

    def _check_integrations(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        findings = []
        for integration in _entries(manifest, "integrations"):
            name = _label(integration, "integration")
            transport = str(integration.get("transport") or "").lower()
            shares_data = integration.get("data_shared") or []

            if _is_none(integration.get("authentication")):
                findings.append(
                    self.finding(
                        severity=Severity.HIGH,
                        title=f"External integration '{name}' has no authentication",
                        explanation=(
                            "An external dependency consumed or exposed without authentication can "
                            "be impersonated, and its responses cannot be trusted as input."
                        ),
                        evidence=_yaml_evidence(
                            integration, ("id", "name", "provider", "authentication", "transport", "data_shared")
                        ),
                        location=Location(file=source, component=f"integrations/{name}"),
                        remediation="Authenticate the integration and pin/verify the peer identity.",
                        rule_id="arch/integration-unauthenticated",
                        cwe=["CWE-306"],
                        tags=["architecture", "third-party"],
                    )
                )

            if _is_none(transport) or transport in _INSECURE_TRANSPORT:
                findings.append(
                    self.finding(
                        severity=Severity.HIGH if shares_data else Severity.MEDIUM,
                        title=f"External integration '{name}' uses an unencrypted transport",
                        explanation=(
                            f"Transport is '{transport or 'unspecified'}'. Data exchanged with a "
                            "third party leaves your network; on the public internet an unencrypted "
                            "channel is readable and modifiable end to end."
                        ),
                        evidence=_yaml_evidence(
                            integration, ("id", "name", "provider", "transport", "data_shared")
                        ),
                        location=Location(file=source, component=f"integrations/{name}"),
                        remediation="Use TLS 1.2+ for all third-party traffic and validate certificates.",
                        rule_id="arch/integration-insecure-transport",
                        cwe=["CWE-319"],
                        tags=["architecture", "third-party", "encryption"],
                    )
                )

            if shares_data and _is_none(integration.get("data_processing_agreement")):
                findings.append(
                    self.finding(
                        severity=Severity.INFO,
                        title=f"Integration '{name}' shares data with no recorded processing agreement",
                        explanation=(
                            "Data leaves the system boundary to a third party. Record the legal "
                            "basis and the agreement covering it, or confirm the data is non-personal."
                        ),
                        evidence=_yaml_evidence(integration, ("id", "name", "provider", "data_shared")),
                        location=Location(file=source, component=f"integrations/{name}"),
                        remediation="Record the data processing agreement reference, or the reason none is needed.",
                        rule_id="arch/integration-no-dpa",
                        tags=["architecture", "third-party", "governance"],
                    )
                )
        return findings

    # ----------------------------------------------------------------- secrets

    def _check_secrets(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        secrets = manifest.get("secrets")
        if not isinstance(secrets, dict) or not secrets:
            return []
        findings = []
        management = str(secrets.get("management") or "unknown").strip().lower()
        severity = _WEAK_SECRET_MANAGEMENT.get(management)
        if severity is not None:
            findings.append(
                self.finding(
                    severity=severity,
                    title=f"Secrets are managed as '{management}'",
                    explanation=(
                        "Credentials handled this way are readable by anyone with access to the "
                        "repository, the image or the host filesystem, cannot be rotated without a "
                        "redeploy, and leave no audit trail of access."
                    ),
                    evidence=_yaml_evidence(secrets, ("management", "rotation", "storage_locations")),
                    location=Location(file=source, component="secrets"),
                    remediation=(
                        "Move credentials into a secret manager (Vault, AWS/GCP/Azure secret "
                        "services, Kubernetes secrets with encryption at rest) and inject them at "
                        "runtime. Keep them out of images and source control."
                    ),
                    rule_id=f"arch/secret-management/{management}",
                    cwe=["CWE-798", "CWE-522"],
                    tags=["architecture", "secrets"],
                )
            )
        if _is_none(secrets.get("rotation")):
            findings.append(
                self.finding(
                    severity=Severity.MEDIUM,
                    title="No secret rotation policy is defined",
                    explanation=(
                        "Without rotation, a credential leaked at any point stays valid "
                        "indefinitely, and there is no routine that would ever invalidate it."
                    ),
                    evidence=_yaml_evidence(secrets, ("management", "rotation")),
                    location=Location(file=source, component="secrets"),
                    remediation="Define a rotation interval and an emergency rotation procedure, and test both.",
                    rule_id="arch/no-secret-rotation",
                    cwe=["CWE-798"],
                    tags=["architecture", "secrets"],
                )
            )
        return findings

    # ---------------------------------------------------------------- exposure

    def _check_exposure(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        findings = []
        for exposure in _entries(manifest, "exposure"):
            name = _label(exposure, "endpoint")
            network = str(exposure.get("network") or "").strip().lower()
            public = network in ("internet", "public", "wan", "0.0.0.0/0")

            if public and _is_none(exposure.get("authentication")):
                findings.append(
                    self.finding(
                        severity=Severity.CRITICAL,
                        title=f"Publicly exposed endpoint '{name}' requires no authentication",
                        explanation=(
                            "The deployment plan places this endpoint on the public internet with "
                            "no authentication in front of it. Assume it will be found by automated "
                            "scanning within hours of going live."
                        ),
                        evidence=_yaml_evidence(
                            exposure, ("id", "component", "url", "network", "authentication", "waf")
                        ),
                        location=Location(
                            file=source, url=exposure.get("url"), component=f"exposure/{name}"
                        ),
                        remediation=(
                            "Require authentication, restrict the source network (VPN, IP allow-list), "
                            "or keep the endpoint off the public internet until UAT is complete."
                        ),
                        rule_id="arch/public-endpoint-unauthenticated",
                        cwe=["CWE-306", "CWE-668"],
                        tags=["architecture", "exposure", "authentication"],
                    )
                )

            if public and not exposure.get("tls", True):
                findings.append(
                    self.finding(
                        severity=Severity.HIGH,
                        title=f"Publicly exposed endpoint '{name}' does not use TLS",
                        explanation="Public traffic without TLS is readable and modifiable in transit.",
                        evidence=_yaml_evidence(exposure, ("id", "component", "url", "network", "tls")),
                        location=Location(file=source, url=exposure.get("url"), component=f"exposure/{name}"),
                        remediation="Terminate TLS 1.2+ and redirect all plaintext HTTP to HTTPS.",
                        rule_id="arch/public-endpoint-no-tls",
                        cwe=["CWE-319"],
                        tags=["architecture", "exposure", "encryption"],
                    )
                )

            if str(exposure.get("environment", "")).lower() in ("uat", "demo", "staging") and public:
                findings.append(
                    self.finding(
                        severity=Severity.MEDIUM,
                        title=f"Pre-production endpoint '{name}' is exposed to the internet",
                        explanation=(
                            "UAT and demo environments typically carry weaker monitoring, test "
                            "credentials and copies of production data, yet are reachable by the "
                            "same attackers as production."
                        ),
                        evidence=_yaml_evidence(exposure, ("id", "component", "url", "network", "environment")),
                        location=Location(file=source, url=exposure.get("url"), component=f"exposure/{name}"),
                        remediation=(
                            "Put pre-production behind a VPN or IP allow-list, and never load it with "
                            "unmasked production data."
                        ),
                        rule_id="arch/preprod-publicly-exposed",
                        tags=["architecture", "exposure"],
                    )
                )
        return findings

    # ------------------------------------------------------------ threat model

    def _check_threat_model(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        threat_model = manifest.get("threat_model")
        if not isinstance(threat_model, dict) or not threat_model:
            return []
        findings: List[Finding] = []

        if not threat_model.get("performed"):
            findings.append(
                self.finding(
                    severity=Severity.HIGH,
                    title="No threat model has been performed",
                    explanation=(
                        "The manifest records that no threat modelling took place. Without it, the "
                        "controls in the design were not chosen against enumerated threats, and the "
                        "gate has nothing to verify them against."
                    ),
                    evidence=_yaml_evidence(threat_model, ("performed", "methodology", "date", "reviewed_by")),
                    location=Location(file=source, component="threat_model"),
                    remediation=(
                        "Run a STRIDE (or equivalent) session over the data-flow diagram before UAT "
                        "and record each threat with its mitigation and status."
                    ),
                    rule_id="arch/no-threat-model",
                    tags=["architecture", "threat-model"],
                )
            )
            return findings

        threats = _entries(threat_model, "threats")
        if not threats:
            findings.append(
                self.finding(
                    severity=Severity.MEDIUM,
                    title="Threat model is declared as performed but lists no threats",
                    explanation=(
                        "A threat model with no enumerated threats cannot be reviewed or tested "
                        "against."
                    ),
                    evidence=_yaml_evidence(threat_model, ("performed", "methodology", "date")),
                    location=Location(file=source, component="threat_model"),
                    remediation="Record the enumerated threats, their mitigations and their status.",
                    rule_id="arch/empty-threat-model",
                    tags=["architecture", "threat-model"],
                )
            )
            return findings

        covered: Set[str] = set()
        for threat in threats:
            category = str(threat.get("stride") or threat.get("category") or "").strip().lower().replace(" ", "_")
            if category:
                covered.add(category)
            status = str(threat.get("status") or "open").strip().lower()
            mitigation = threat.get("mitigation")
            if status in ("open", "unmitigated", "todo") or _is_none(mitigation):
                findings.append(
                    self.finding(
                        severity=Severity.MEDIUM if status != "accepted" else Severity.LOW,
                        title=f"Threat '{_label(threat, 'threat')}' is not mitigated",
                        explanation=(
                            f"The threat model records this threat with status '{status}' and "
                            f"mitigation '{mitigation or 'none'}'. Open threats must be closed or "
                            "explicitly risk-accepted by a named owner before UAT."
                        ),
                        evidence=_yaml_evidence(
                            threat, ("id", "stride", "description", "mitigation", "status", "owner")
                        ),
                        location=Location(file=source, component=f"threat_model/{_label(threat, 'threat')}"),
                        remediation=(
                            "Implement the mitigation, or record a dated risk acceptance with a "
                            "named owner and a review date."
                        ),
                        rule_id="arch/unmitigated-threat",
                        tags=["architecture", "threat-model"],
                    )
                )

        missing = [category for category in _STRIDE if category not in covered]
        if missing:
            findings.append(
                self.finding(
                    severity=Severity.LOW,
                    title=f"Threat model does not cover {len(missing)} STRIDE categor{'y' if len(missing) == 1 else 'ies'}",
                    explanation=(
                        "The following STRIDE categories have no enumerated threat: "
                        + ", ".join(category.replace("_", " ") for category in missing)
                        + ". A category with no threats is usually an unexamined category rather "
                        "than an inapplicable one."
                    ),
                    evidence=f"covered: {', '.join(sorted(covered)) or 'none'}\nmissing: {', '.join(missing)}",
                    location=Location(file=source, component="threat_model"),
                    remediation=(
                        "Walk each missing STRIDE category against the data-flow diagram and either "
                        "record a threat or note why the category does not apply."
                    ),
                    rule_id="arch/incomplete-stride-coverage",
                    tags=["architecture", "threat-model", "checklist"],
                )
            )
        return findings

    # ----------------------------------------------------------------- logging

    def _check_logging(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        logging_config = manifest.get("logging")
        if not isinstance(logging_config, dict):
            return []
        findings = []
        if not logging_config.get("security_events"):
            findings.append(
                self.finding(
                    severity=Severity.MEDIUM,
                    title="Security-relevant events are not logged",
                    explanation=(
                        "Authentication attempts, authorization failures and administrative actions "
                        "are not recorded, so an intrusion would leave no trace to detect or "
                        "investigate."
                    ),
                    evidence=_yaml_evidence(logging_config, ("security_events", "centralised", "retention")),
                    location=Location(file=source, component="logging"),
                    remediation=(
                        "Log authentication outcomes, authorization denials and administrative "
                        "changes with a correlation id, and ship them off the host."
                    ),
                    rule_id="arch/no-security-logging",
                    cwe=["CWE-778"],
                    tags=["architecture", "logging"],
                )
            )
        if logging_config.get("pii_redaction") is False:
            findings.append(
                self.finding(
                    severity=Severity.MEDIUM,
                    title="Logs are not redacted for personal or sensitive data",
                    explanation=(
                        "Unredacted logs turn a low-sensitivity system (log storage, log shipping, "
                        "support tooling) into a store of sensitive data."
                    ),
                    evidence=_yaml_evidence(logging_config, ("pii_redaction", "centralised")),
                    location=Location(file=source, component="logging"),
                    remediation="Redact credentials, tokens and personal data at the logging boundary.",
                    rule_id="arch/no-log-redaction",
                    cwe=["CWE-532"],
                    tags=["architecture", "logging"],
                )
            )
        return findings

    # -------------------------------------------------------- model integrity

    def _check_model_consistency(self, manifest: Dict[str, Any], source: str) -> List[Finding]:
        component_ids = {
            str(entry.get("id") or entry.get("name")) for entry in _entries(manifest, "components")
        }
        component_ids |= {
            str(entry.get("id") or entry.get("name")) for entry in _entries(manifest, "data_stores")
        }
        component_ids |= {
            str(entry.get("id") or entry.get("name")) for entry in _entries(manifest, "integrations")
        }
        boundary_ids = {
            str(entry.get("id") or entry.get("name")) for entry in _entries(manifest, "trust_boundaries")
        }

        findings = []
        for flow in _entries(manifest, "data_flows"):
            name = _label(flow, "flow")
            for role in ("source", "destination"):
                referenced = flow.get(role)
                if referenced and str(referenced) not in component_ids:
                    findings.append(
                        self.finding(
                            severity=Severity.LOW,
                            title=f"Data flow '{name}' references undefined {role} '{referenced}'",
                            explanation=(
                                "The architecture model is internally inconsistent: this endpoint of "
                                "the flow is not defined as a component, data store or integration, "
                                "so its controls were never reviewed."
                            ),
                            evidence=_yaml_evidence(flow, ("id", "name", "source", "destination")),
                            location=Location(file=source, component=f"data_flows/{name}"),
                            remediation=f"Define '{referenced}' in the manifest, or correct the reference.",
                            rule_id="arch/undefined-flow-endpoint",
                            tags=["architecture", "consistency"],
                        )
                    )
            crosses = flow.get("crosses_boundary")
            if crosses and str(crosses) not in boundary_ids:
                findings.append(
                    self.finding(
                        severity=Severity.LOW,
                        title=f"Data flow '{name}' references undefined trust boundary '{crosses}'",
                        explanation=(
                            "The flow claims to cross a boundary that the manifest does not define, "
                            "so the controls at that boundary are unknown."
                        ),
                        evidence=_yaml_evidence(flow, ("id", "name", "crosses_boundary")),
                        location=Location(file=source, component=f"data_flows/{name}"),
                        remediation=f"Define trust boundary '{crosses}', or correct the reference.",
                        rule_id="arch/undefined-trust-boundary-reference",
                        tags=["architecture", "consistency"],
                    )
                )
        return findings
