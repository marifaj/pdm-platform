"""Architecture layer: the manifest rule engine and the document checklist."""

from pathlib import Path

import pytest
import yaml

from markna.models import Severity
from markna.scanners.arch_checklist import ArchitectureDocumentScanner
from markna.scanners.arch_manifest import ArchitectureManifestScanner
from markna.scanners.base import ScannerContext
from markna.templates import ARCHITECTURE_TEMPLATE

EXAMPLE_DIR = Path(__file__).resolve().parent.parent / "examples" / "vulnerable-demo"


def context(tmp_path, manifest=None, docs=()) -> ScannerContext:
    return ScannerContext(
        workdir=tmp_path,
        architecture_manifest=manifest,
        architecture_manifest_path=Path("architecture.yaml") if manifest else None,
        architecture_docs=list(docs),
    )


def rule_ids(findings) -> set:
    return {finding.rule_id for finding in findings}


class TestManifestScanner:
    @pytest.fixture
    def weak(self):
        return yaml.safe_load((EXAMPLE_DIR / "architecture.yaml").read_text())

    def test_not_applicable_without_a_manifest(self, tmp_path):
        applicable, reason = ArchitectureManifestScanner().applicable(context(tmp_path))
        assert not applicable
        assert "manifest" in reason

    def test_weak_manifest_fires_the_expected_rules(self, tmp_path, weak):
        findings = list(ArchitectureManifestScanner().scan(context(tmp_path, manifest=weak)))
        ids = rule_ids(findings)
        expected = {
            "arch/internet-facing-unauthenticated",
            "arch/sensitive-component-without-authz",
            "arch/unencrypted-boundary-crossing",
            "arch/unauthenticated-sensitive-flow",
            "arch/sensitive-store-unencrypted",
            "arch/no-backup",
            "arch/integration-unauthenticated",
            "arch/integration-insecure-transport",
            "arch/secret-management/env-file",
            "arch/no-secret-rotation",
            "arch/public-endpoint-unauthenticated",
            "arch/public-endpoint-no-tls",
            "arch/preprod-publicly-exposed",
            "arch/unmitigated-threat",
            "arch/incomplete-stride-coverage",
            "arch/no-security-logging",
            "arch/no-log-redaction",
            "arch/boundary-without-controls",
        }
        assert expected <= ids, f"missing: {sorted(expected - ids)}"

    def test_public_unauthenticated_endpoint_is_critical(self, tmp_path, weak):
        findings = list(ArchitectureManifestScanner().scan(context(tmp_path, manifest=weak)))
        critical = [
            f for f in findings if f.rule_id == "arch/public-endpoint-unauthenticated"
        ]
        assert critical and critical[0].severity is Severity.CRITICAL

    def test_findings_cite_the_manifest_field(self, tmp_path, weak):
        findings = list(ArchitectureManifestScanner().scan(context(tmp_path, manifest=weak)))
        secrets = next(f for f in findings if f.rule_id == "arch/secret-management/env-file")
        assert secrets.location.component == "secrets"
        assert "management: env-file" in secrets.evidence

    def test_shipped_template_produces_no_high_severity_findings(self, tmp_path):
        manifest = yaml.safe_load(ARCHITECTURE_TEMPLATE)
        findings = list(ArchitectureManifestScanner().scan(context(tmp_path, manifest=manifest)))
        serious = [f for f in findings if f.severity.rank >= Severity.HIGH.rank]
        assert not serious, [f.title for f in serious]

    def test_missing_sections_are_reported(self, tmp_path):
        findings = list(
            ArchitectureManifestScanner().scan(context(tmp_path, manifest={"system": {"name": "x"}}))
        )
        missing = {f.rule_id for f in findings if f.rule_id.startswith("arch/section-missing/")}
        assert "arch/section-missing/trust_boundaries" in missing
        assert "arch/section-missing/threat_model" in missing

    def test_dangling_references_are_reported(self, tmp_path):
        manifest = {
            "components": [{"id": "api", "type": "service", "authentication": "oauth2"}],
            "data_flows": [
                {
                    "id": "df-1",
                    "source": "api",
                    "destination": "ghost",
                    "crosses_boundary": "tb-missing",
                    "encryption_in_transit": "tls1.3",
                }
            ],
        }
        ids = rule_ids(ArchitectureManifestScanner().scan(context(tmp_path, manifest=manifest)))
        assert "arch/undefined-flow-endpoint" in ids
        assert "arch/undefined-trust-boundary-reference" in ids

    def test_no_trust_boundaries_is_a_finding_in_its_own_right(self, tmp_path):
        manifest = {"trust_boundaries": [], "system": {"name": "x"}}
        ids = rule_ids(ArchitectureManifestScanner().scan(context(tmp_path, manifest=manifest)))
        # An empty section reports as missing rather than as an empty boundary list.
        assert "arch/section-missing/trust_boundaries" in ids


class TestDocumentChecklist:
    def test_not_applicable_without_documents(self, tmp_path):
        applicable, reason = ArchitectureDocumentScanner().applicable(context(tmp_path))
        assert not applicable
        assert "--arch" in reason

    def test_missing_topics_are_reported(self, tmp_path):
        document = tmp_path / "arch.md"
        document.write_text("# Overview\n\nA service that stores data in a database.\n")
        findings = list(
            ArchitectureDocumentScanner().scan(context(tmp_path, docs=[document]))
        )
        missing = {f.rule_id for f in findings if "topic-missing" in f.rule_id}
        assert "arch-doc/topic-missing/threat-model" in missing
        assert "arch-doc/topic-missing/authentication" in missing

    def test_covered_topics_are_not_reported(self, tmp_path):
        document = tmp_path / "arch.md"
        document.write_text(
            "# Design\n"
            "Trust boundaries are documented below. Authentication uses OAuth2 and "
            "authorization is RBAC. Sensitive data (PII) is classified. The data flow "
            "diagram is attached. TLS 1.3 encrypts everything in transit and at rest. "
            "Secrets live in Vault with 90-day rotation. Third-party integrations are "
            "listed. The firewall exposes only port 443. A STRIDE threat model was run. "
            "Logging is centralised and audited. Backups run nightly with a tested "
            "restore. All input is validated against a schema.\n"
        )
        findings = list(
            ArchitectureDocumentScanner().scan(context(tmp_path, docs=[document]))
        )
        assert not [f for f in findings if "topic-missing" in f.rule_id]

    def test_stated_risks_are_extracted_with_line_numbers(self, tmp_path):
        document = tmp_path / "arch.md"
        document.write_text(
            "# Design\n"
            "There is no authentication on the collector endpoint.\n"
            "It listens on 0.0.0.0 and the service runs as root.\n"
            "Data is sent to http://collector.example.com/ingest unencrypted.\n"
        )
        findings = list(
            ArchitectureDocumentScanner().scan(context(tmp_path, docs=[document]))
        )
        ids = rule_ids(findings)
        assert "arch-doc/stated-risk/no-authentication" in ids
        assert "arch-doc/stated-risk/bind-all-interfaces" in ids
        assert "arch-doc/stated-risk/runs-as-root" in ids
        assert "arch-doc/stated-risk/http-endpoint" in ids
        located = next(f for f in findings if f.rule_id == "arch-doc/stated-risk/no-authentication")
        assert located.location.line == 2

    def test_localhost_http_urls_are_not_flagged(self, tmp_path):
        document = tmp_path / "arch.md"
        document.write_text("Developers use http://localhost:8000 for local testing.\n")
        findings = list(
            ArchitectureDocumentScanner().scan(context(tmp_path, docs=[document]))
        )
        assert "arch-doc/stated-risk/http-endpoint" not in rule_ids(findings)

    def test_credentials_in_the_document_are_reported_and_redacted(self, tmp_path):
        document = tmp_path / "arch.md"
        document.write_text("The collector uses AKIAIOSFODNN7EXAMPLE for S3 uploads.\n")
        findings = list(
            ArchitectureDocumentScanner().scan(context(tmp_path, docs=[document]))
        )
        secret = next(f for f in findings if f.rule_id.startswith("arch-doc/secret/"))
        assert "AKIAIOSFODNN7EXAMPLE" not in secret.evidence

    def test_binary_documents_are_reported_as_not_ingested(self, tmp_path):
        document = tmp_path / "arch.pdf"
        document.write_bytes(b"%PDF-1.7 binary")
        findings = list(
            ArchitectureDocumentScanner().scan(context(tmp_path, docs=[document]))
        )
        assert "arch-doc/unsupported-format" in rule_ids(findings)
        assert "arch-doc/no-ingestible-document" in rule_ids(findings)

    def test_ingestion_inventory_is_recorded(self, tmp_path):
        document = tmp_path / "arch.md"
        document.write_text("# Design\n")
        findings = list(
            ArchitectureDocumentScanner().scan(context(tmp_path, docs=[document]))
        )
        inventory = next(f for f in findings if f.rule_id == "arch-doc/ingested")
        assert inventory.severity is Severity.INFO
        assert "arch.md" in inventory.evidence
