"""Report rendering: JSON, Markdown, SARIF and HTML."""

import json

import pytest

from markna.models import (
    Assessment,
    CoverageEntry,
    Finding,
    Layer,
    Location,
    RunStatus,
    ScannerRun,
    Severity,
    Target,
    assign_ids,
    utc_now,
)
from markna.policy import Policy
from markna.report import render_html, render_json, render_markdown, render_sarif, write_reports


@pytest.fixture
def assessment() -> Assessment:
    findings = [
        Finding(
            source="semgrep",
            layer=Layer.CODE,
            severity=Severity.CRITICAL,
            title="Command injection in report generation",
            explanation="shell=True with an interpolated value.",
            evidence="29| subprocess.check_output(cmd, shell=True)",
            location=Location(file="app.py", line=29, end_line=29),
            remediation="Pass an argument list.",
            rule_id="markna-py-subprocess-shell-true",
            cwe=["CWE-78"],
            references=["https://example.invalid/cwe-78"],
        ),
        Finding(
            source="ai-advisory",
            layer=Layer.ARCHITECTURE,
            severity=Severity.HIGH,
            title="Device identity is not verified anywhere in the pipeline",
            explanation="No component authenticates the publishing device.",
            evidence="architecture.yaml: components[].authentication == none",
            location=Location(component="components/collector"),
            remediation="Introduce per-device credentials.",
            rule_id="ai-advisory/reasoning",
            ai_generated=True,
            confidence="medium",
        ),
        Finding(
            source="env-http-headers",
            layer=Layer.ENVIRONMENT,
            severity=Severity.MEDIUM,
            title="Content-Security-Policy header is missing",
            explanation="No CSP on an HTML response.",
            evidence="HTTP/1.1 200 OK",
            location=Location(url="https://uat.example.com/"),
            remediation="Add a CSP.",
            rule_id="env-http/csp-missing",
        ),
    ]
    result = Assessment(
        assessment_id="MKA-20260821T120000Z-ABC123",
        target=Target(
            name="demo",
            project_path="/srv/demo",
            architecture_docs=["docs/architecture.md"],
            architecture_manifest="architecture.yaml",
            environment_url="https://uat.example.com",
            git_commit="deadbeef",
            git_branch="main",
        ),
        policy_name="markna-default-v1",
        started_at=utc_now(),
        finished_at=utc_now(),
        layers_requested=[Layer.ARCHITECTURE, Layer.CODE, Layer.ENVIRONMENT],
        findings=assign_ids(findings),
        runs=[
            ScannerRun(
                name="semgrep",
                layer=Layer.CODE,
                status=RunStatus.OK,
                capabilities=["sast"],
                findings_count=1,
                tool_version="1.174.0",
            ),
            ScannerRun(
                name="gitleaks",
                layer=Layer.CODE,
                status=RunStatus.UNAVAILABLE,
                capabilities=["secrets"],
                message="required executable not found: gitleaks",
            ),
        ],
        coverage=[
            CoverageEntry(Layer.CODE, "sast", True, ["semgrep"]),
            CoverageEntry(Layer.CODE, "secrets", False, []),
        ],
        tool_version="1.0.0",
    )
    return Policy().apply(result)


class TestJson:
    def test_round_trips(self, assessment):
        data = json.loads(render_json(assessment))
        assert data["verdict"] in ("PASS", "WARN", "BLOCK")
        assert data["schema_version"] == "1.0"
        assert len(data["findings"]) >= 3

    def test_every_finding_has_the_required_fields(self, assessment):
        required = {
            "id",
            "source",
            "layer",
            "severity",
            "title",
            "explanation",
            "evidence",
            "location",
            "remediation",
            "blocking",
            "timestamp",
        }
        for finding in json.loads(render_json(assessment))["findings"]:
            assert required <= set(finding)

    def test_raw_tool_output_is_excluded_by_default(self, assessment):
        assessment.findings[0].raw = {"secret": "value"}
        assert "raw" not in json.loads(render_json(assessment))["findings"][0]
        with_raw = json.loads(render_json(assessment, include_raw=True))
        assert any("raw" in finding for finding in with_raw["findings"])

    def test_scanner_runs_are_recorded(self, assessment):
        runs = json.loads(render_json(assessment))["scanner_runs"]
        assert {run["name"] for run in runs} == {"semgrep", "gitleaks"}
        unavailable = next(run for run in runs if run["name"] == "gitleaks")
        assert unavailable["status"] == "unavailable"


class TestMarkdown:
    def test_leads_with_the_verdict(self, assessment):
        text = render_markdown(assessment)
        assert text.startswith("# MARKNA Security Gate Report")
        assert f"## Verdict: {assessment.verdict.value}" in text

    def test_labels_ai_findings(self, assessment):
        text = render_markdown(assessment)
        assert "AI ADVISORY — not security evidence on its own" in text

    def test_shows_the_coverage_gap(self, assessment):
        text = render_markdown(assessment)
        assert "Evidence coverage" in text
        assert "| code | secrets | **NO** |" in text

    def test_includes_evidence_and_remediation(self, assessment):
        text = render_markdown(assessment)
        assert "subprocess.check_output(cmd, shell=True)" in text
        assert "**Remediation** — Pass an argument list." in text

    def test_explains_how_to_read_the_report(self, assessment):
        assert "How to read this report" in render_markdown(assessment)


class TestSarif:
    def test_is_valid_sarif_shape(self, assessment):
        data = json.loads(render_sarif(assessment))
        assert data["version"] == "2.1.0"
        run = data["runs"][0]
        assert run["tool"]["driver"]["name"] == "MARKNA Security Gate"
        assert run["results"]

    def test_severity_maps_to_sarif_levels(self, assessment):
        results = json.loads(render_sarif(assessment))["runs"][0]["results"]
        by_rule = {result["ruleId"]: result for result in results}
        assert by_rule["markna-py-subprocess-shell-true"]["level"] == "error"
        assert by_rule["env-http/csp-missing"]["level"] == "warning"

    def test_file_findings_carry_a_physical_location(self, assessment):
        results = json.loads(render_sarif(assessment))["runs"][0]["results"]
        code = next(r for r in results if r["ruleId"] == "markna-py-subprocess-shell-true")
        region = code["locations"][0]["physicalLocation"]["region"]
        assert region["startLine"] == 29

    def test_non_file_findings_use_a_logical_location(self, assessment):
        results = json.loads(render_sarif(assessment))["runs"][0]["results"]
        advisory = next(r for r in results if r["ruleId"] == "ai-advisory/reasoning")
        assert advisory["locations"][0]["logicalLocations"]

    def test_ai_findings_are_marked_in_the_message(self, assessment):
        results = json.loads(render_sarif(assessment))["runs"][0]["results"]
        advisory = next(r for r in results if r["ruleId"] == "ai-advisory/reasoning")
        assert advisory["message"]["text"].startswith("[AI ADVISORY")
        assert advisory["properties"]["aiGenerated"] is True

    def test_fingerprints_are_stable_identifiers(self, assessment):
        results = json.loads(render_sarif(assessment))["runs"][0]["results"]
        assert all(result["partialFingerprints"]["marknaFingerprint"] for result in results)


class TestHtml:
    def test_is_a_self_contained_document(self, assessment):
        html = render_html(assessment)
        assert html.startswith("<!doctype html>")
        assert "<style>" in html
        assert "http-equiv" not in html
        # No external resources: the report must render from a file share or an email.
        assert "src=\"http" not in html and "href=\"http" not in html.split("<footer>")[0]

    def test_escapes_finding_content(self, assessment):
        assessment.findings[0].evidence = "<script>alert(1)</script>"
        assert "<script>alert(1)</script>" not in render_html(assessment)
        assert "&lt;script&gt;" in render_html(assessment)

    def test_marks_ai_findings(self, assessment):
        assert "AI ADVISORY" in render_html(assessment)


class TestWriteReports:
    def test_writes_every_requested_format(self, assessment, tmp_path):
        written = write_reports(assessment, tmp_path, ["json", "markdown", "sarif", "html"])
        assert set(written) == {"json", "markdown", "sarif", "html"}
        for path in written.values():
            assert path.is_file() and path.stat().st_size > 0

    def test_rejects_an_unknown_format(self, assessment, tmp_path):
        with pytest.raises(ValueError, match="unknown report format"):
            write_reports(assessment, tmp_path, ["pdf"])

    def test_creates_the_output_directory(self, assessment, tmp_path):
        target = tmp_path / "nested" / "reports"
        write_reports(assessment, target, ["json"])
        assert (target / "markna-report.json").is_file()
