"""The policy engine decides the verdict — including what a missing scanner means."""

from datetime import date, timedelta

import pytest

from markna.models import (
    Assessment,
    Finding,
    Layer,
    Location,
    RunStatus,
    ScannerRun,
    Severity,
    Target,
    Verdict,
    utc_now,
)
from markna.policy import Policy, PolicyError, cap_findings


def finding(severity=Severity.HIGH, **overrides) -> Finding:
    defaults = dict(
        source="test-scanner",
        layer=Layer.CODE,
        severity=severity,
        title="Example",
        explanation="Explanation",
        evidence="evidence",
        location=Location(file="app.py", line=1),
        remediation="Fix",
        rule_id="test/rule",
    )
    defaults.update(overrides)
    return Finding(**defaults)


def assessment(findings=(), runs=(), layers=(Layer.CODE,)) -> Assessment:
    return Assessment(
        assessment_id="MKA-TEST",
        target=Target(name="unit"),
        policy_name="test",
        started_at=utc_now(),
        finished_at=utc_now(),
        layers_requested=list(layers),
        findings=list(findings),
        runs=list(runs),
    )


def full_code_coverage() -> list:
    """Runs that satisfy every capability the default policy requires for code."""
    return [
        ScannerRun(
            name=f"scanner-{capability}",
            layer=Layer.CODE,
            status=RunStatus.OK,
            capabilities=[capability],
        )
        for capability in ("sast", "dependency-vulnerabilities", "secrets", "sbom")
    ]


class TestVerdict:
    def test_clean_run_with_full_coverage_passes(self):
        result = Policy().apply(assessment(runs=full_code_coverage()))
        assert result.verdict is Verdict.PASS

    def test_high_severity_finding_blocks(self):
        result = Policy().apply(
            assessment(findings=[finding(Severity.HIGH)], runs=full_code_coverage())
        )
        assert result.verdict is Verdict.BLOCK
        assert result.blocking_findings

    def test_medium_severity_finding_warns(self):
        result = Policy().apply(
            assessment(findings=[finding(Severity.MEDIUM)], runs=full_code_coverage())
        )
        assert result.verdict is Verdict.WARN
        assert not result.blocking_findings

    def test_info_only_still_passes(self):
        result = Policy().apply(
            assessment(findings=[finding(Severity.INFO)], runs=full_code_coverage())
        )
        assert result.verdict is Verdict.PASS

    def test_block_severities_are_configurable(self):
        policy = Policy.from_dict({"block_on": ["critical"], "warn_on": ["high", "medium"]})
        result = policy.apply(
            assessment(findings=[finding(Severity.HIGH)], runs=full_code_coverage())
        )
        assert result.verdict is Verdict.WARN


class TestAiFindingsAreAdvisory:
    def test_ai_findings_do_not_block_by_default(self):
        result = Policy().apply(
            assessment(
                findings=[finding(Severity.CRITICAL, ai_generated=True)],
                runs=full_code_coverage(),
            )
        )
        assert result.verdict is Verdict.WARN
        assert not result.blocking_findings

    def test_ai_findings_can_block_when_explicitly_enabled(self):
        policy = Policy.from_dict({"ai_findings_can_block": True})
        result = policy.apply(
            assessment(
                findings=[finding(Severity.CRITICAL, ai_generated=True)],
                runs=full_code_coverage(),
            )
        )
        assert result.verdict is Verdict.BLOCK

    def test_verdict_reasons_call_out_advisory_findings(self):
        result = Policy().apply(
            assessment(
                findings=[finding(Severity.HIGH, ai_generated=True)],
                runs=full_code_coverage(),
            )
        )
        assert any("advisory" in reason.lower() for reason in result.verdict_reasons)


class TestCoverage:
    def test_missing_scanner_produces_a_blocking_coverage_finding(self):
        result = Policy().apply(assessment(runs=[]))
        gaps = [f for f in result.findings if "coverage-gap" in f.tags]
        assert len(gaps) == 4  # sast, dependency-vulnerabilities, secrets, sbom
        assert result.verdict is Verdict.BLOCK
        assert all(gap.blocking for gap in gaps)

    def test_an_unavailable_scanner_does_not_satisfy_coverage(self):
        runs = full_code_coverage()
        runs[0].status = RunStatus.UNAVAILABLE
        runs[0].message = "not installed"
        result = Policy().apply(assessment(runs=runs))
        gaps = [f for f in result.findings if "coverage-gap" in f.tags]
        assert [gap.rule_id for gap in gaps] == ["coverage/code/sast"]
        assert "not installed" in gaps[0].evidence

    def test_ai_scanner_cannot_satisfy_deterministic_coverage(self):
        runs = full_code_coverage()
        runs[0].deterministic = False
        result = Policy().apply(assessment(runs=runs))
        assert any(f.rule_id == "coverage/code/sast" for f in result.findings)

    def test_coverage_is_only_required_for_requested_layers(self):
        result = Policy().apply(assessment(runs=full_code_coverage(), layers=[Layer.CODE]))
        assert {entry.layer for entry in result.coverage} == {Layer.CODE}

    def test_scanner_error_is_reported_as_a_finding(self):
        runs = full_code_coverage()
        runs.append(
            ScannerRun(
                name="broken",
                layer=Layer.CODE,
                status=RunStatus.ERROR,
                capabilities=["sast"],
                message="tool exploded",
            )
        )
        result = Policy().apply(assessment(runs=runs))
        errors = [f for f in result.findings if "scanner-error" in f.tags]
        assert len(errors) == 1
        assert "tool exploded" in errors[0].evidence


class TestSuppressionsAndOverrides:
    def test_suppression_removes_a_finding_from_the_verdict(self):
        policy = Policy.from_dict(
            {
                "suppressions": [
                    {"match": {"rule_id": "test/rule"}, "reason": "reviewed and accepted"}
                ]
            }
        )
        result = policy.apply(
            assessment(findings=[finding(Severity.CRITICAL)], runs=full_code_coverage())
        )
        assert result.verdict is Verdict.PASS
        assert result.findings[0].suppressed
        assert result.findings[0].suppression_reason == "reviewed and accepted"

    def test_expired_suppression_is_ignored(self):
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        policy = Policy.from_dict(
            {
                "suppressions": [
                    {"match": {"rule_id": "test/rule"}, "reason": "stale", "expires": yesterday}
                ]
            }
        )
        result = policy.apply(
            assessment(findings=[finding(Severity.CRITICAL)], runs=full_code_coverage())
        )
        assert result.verdict is Verdict.BLOCK

    def test_severity_override_changes_the_verdict(self):
        policy = Policy.from_dict(
            {
                "severity_overrides": [
                    {"match": {"rule_id": "test/rule"}, "severity": "low", "reason": "test fixture"}
                ]
            }
        )
        result = policy.apply(
            assessment(findings=[finding(Severity.CRITICAL)], runs=full_code_coverage())
        )
        assert result.verdict is Verdict.WARN
        assert result.findings[0].severity is Severity.LOW
        assert "test fixture" in result.findings[0].explanation

    def test_an_empty_match_never_matches_everything(self):
        policy = Policy.from_dict({"suppressions": [{"match": {}, "reason": "oops"}]})
        result = policy.apply(
            assessment(findings=[finding(Severity.CRITICAL)], runs=full_code_coverage())
        )
        assert result.verdict is Verdict.BLOCK

    def test_path_glob_matching(self):
        policy = Policy.from_dict(
            {"suppressions": [{"match": {"path_glob": "*.py"}, "reason": "python only"}]}
        )
        result = policy.apply(
            assessment(findings=[finding(Severity.CRITICAL)], runs=full_code_coverage())
        )
        assert result.findings[0].suppressed


class TestPolicyLoading:
    def test_unknown_top_level_key_is_rejected(self):
        with pytest.raises(PolicyError, match="unknown policy key"):
            Policy.from_dict({"blok_on": ["high"]})

    def test_unknown_match_key_is_rejected(self):
        with pytest.raises(PolicyError, match="unknown match key"):
            Policy.from_dict({"suppressions": [{"match": {"filename": "x"}, "reason": "r"}]})

    def test_unknown_layer_in_required_capabilities_is_rejected(self):
        with pytest.raises(PolicyError, match="unknown layer"):
            Policy.from_dict({"required_capabilities": {"infra": ["sast"]}})

    def test_missing_file_is_reported_clearly(self):
        with pytest.raises(PolicyError, match="policy file not found"):
            Policy.load("/nonexistent/policy.yaml")

    def test_shipped_template_is_valid(self):
        import yaml

        from markna.templates import POLICY_TEMPLATE

        policy = Policy.from_dict(yaml.safe_load(POLICY_TEMPLATE))
        assert policy.name == "markna-default-v1"
        assert policy.ai_findings_can_block is False


def test_cap_findings_truncates_and_records_the_truncation():
    findings = [finding(Severity.LOW, title=f"f{i}", rule_id=f"r{i}") for i in range(10)]
    capped = cap_findings(findings, 5, "noisy-scanner")
    assert len(capped) == 6  # 5 kept plus the truncation notice
    assert capped[-1].rule_id == "runner/truncated"
    assert "dropped=5" in capped[-1].evidence


def test_cap_findings_is_a_no_op_below_the_limit():
    findings = [finding(Severity.LOW)]
    assert cap_findings(findings, 5, "quiet-scanner") == findings
