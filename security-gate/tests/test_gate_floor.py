"""B1 — the mandatory security-policy floor.

Regression tests for the finding that a maintainer-written policy could turn the
gate into an unconditional PASS. Each test below is an attack on the verdict,
written from the attacker's side: the assertion is that the gate refuses.
"""

from __future__ import annotations

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
    Verdict,
    utc_now,
)
from markna.policy import (
    FLOOR_TAG,
    MANDATORY_BLOCK_SEVERITIES,
    MANDATORY_CAPABILITIES,
    Policy,
)


def assessment(
    findings=(), runs=(), layers=(Layer.CODE,), available=None
) -> Assessment:
    return Assessment(
        assessment_id="MKA-TEST",
        target=Target(name="unit"),
        policy_name="test",
        started_at=utc_now(),
        finished_at=utc_now(),
        layers_requested=list(layers),
        layers_available=list(available if available is not None else layers),
        findings=list(findings),
        runs=list(runs),
    )


def satisfying_runs(layer: Layer = Layer.CODE, capabilities=None) -> list:
    """Scanner runs that satisfy every capability the default policy wants."""
    capabilities = capabilities or ["sast", "dependency-vulnerabilities", "secrets", "sbom"]
    return [
        ScannerRun(name=f"scanner-{name}", layer=layer, status=RunStatus.OK, capabilities=[name])
        for name in capabilities
    ]


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


class TestCapabilityFloorCannotBeRemoved:
    def test_an_empty_required_capabilities_map_is_restored(self):
        policy = Policy.from_dict({"required_capabilities": {}})
        for layer, mandatory in MANDATORY_CAPABILITIES.items():
            assert set(mandatory) <= set(policy.required_capabilities[layer])

    def test_dropping_one_capability_restores_only_that_one(self):
        policy = Policy.from_dict({"required_capabilities": {"code": ["sbom"]}})
        assert set(policy.required_capabilities["code"]) >= {"sast", "secrets", "sbom"}

    def test_the_policy_may_still_require_more_than_the_floor(self):
        policy = Policy.from_dict(
            {"required_capabilities": {"code": ["sast", "secrets", "license-risk"]}}
        )
        assert "license-risk" in policy.required_capabilities["code"]

    def test_the_attempt_is_recorded(self):
        policy = Policy.from_dict({"required_capabilities": {"code": []}})
        assert any("code" in note for note in policy.floor_corrections)

    def test_the_attempt_becomes_a_blocking_finding(self):
        policy = Policy.from_dict({"required_capabilities": {"code": []}})
        result = policy.apply(assessment(runs=satisfying_runs()))
        restored = [f for f in result.findings if f.rule_id == "policy/floor-restored"]
        assert restored and restored[0].blocking
        assert result.verdict is Verdict.BLOCK


class TestBlockSeverityFloor:
    def test_an_empty_block_on_still_blocks_critical(self):
        policy = Policy.from_dict({"block_on": []})
        assert set(MANDATORY_BLOCK_SEVERITIES) <= set(policy.block_on)

    def test_a_critical_finding_blocks_under_an_empty_block_on(self):
        policy = Policy.from_dict({"block_on": [], "warn_on": []})
        result = policy.apply(
            assessment(findings=[finding(Severity.CRITICAL)], runs=satisfying_runs())
        )
        assert result.verdict is Verdict.BLOCK


class TestPassRequiresMandatoryCoverage:
    def test_pass_is_impossible_with_no_mandatory_capability_evaluated(self):
        """The exact shape Codex demonstrated: nothing required, so nothing proved."""
        policy = Policy()
        policy.required_capabilities = {}  # bypass the loader, as a bug might
        result = policy.apply(assessment(runs=satisfying_runs()))
        assert result.verdict is Verdict.BLOCK
        assert any("establishes nothing" in reason for reason in result.verdict_reasons)

    def test_a_clean_run_with_full_mandatory_coverage_passes(self):
        result = Policy().apply(assessment(runs=satisfying_runs()))
        assert result.verdict is Verdict.PASS
        assert any("mandatory" in reason for reason in result.verdict_reasons)

    def test_a_missing_mandatory_capability_blocks_whatever_its_severity(self):
        policy = Policy.from_dict({"coverage_gap_severity": "info", "block_on": ["critical"]})
        runs = satisfying_runs(capabilities=["dependency-vulnerabilities", "sbom", "secrets"])
        result = policy.apply(assessment(runs=runs))
        gaps = [f for f in result.findings if f.rule_id == "coverage/code/sast"]
        assert gaps and gaps[0].blocking, "a mandatory gap must block even at info severity"
        assert result.verdict is Verdict.BLOCK

    def test_the_verdict_reports_the_mandatory_ratio(self):
        result = Policy().apply(assessment(runs=satisfying_runs()))
        assert any("2/2" in reason or "mandatory" in reason for reason in result.verdict_reasons)


class TestFloorFindingsAreOutOfReach:
    def _lax_policy(self) -> Policy:
        return Policy.from_dict(
            {
                "block_on": [],
                "warn_on": [],
                "required_capabilities": {"code": []},
                "suppressions": [
                    {"match": {"source": "markna:*"}, "reason": "make it green"},
                    {"match": {"layer": "code"}, "reason": "make it green"},
                    {"match": {"severity": "high"}, "reason": "make it green"},
                    {"match": {"severity": "critical"}, "reason": "make it green"},
                ],
                "severity_overrides": [
                    {"match": {"source": "markna:*"}, "severity": "info", "reason": "quiet"}
                ],
            }
        )

    def test_suppressions_cannot_reach_a_floor_finding(self):
        result = self._lax_policy().apply(assessment(runs=[]))
        floor = [f for f in result.findings if FLOOR_TAG in f.tags]
        assert floor, "expected mandatory coverage gaps"
        assert not any(f.suppressed for f in floor)

    def test_severity_overrides_cannot_reach_a_floor_finding(self):
        result = self._lax_policy().apply(assessment(runs=[]))
        floor = [f for f in result.findings if FLOOR_TAG in f.tags]
        assert all("severity-overridden" not in f.tags for f in floor)

    def test_the_most_permissive_possible_policy_still_blocks(self):
        result = self._lax_policy().apply(
            assessment(findings=[finding(Severity.CRITICAL)], runs=[])
        )
        assert result.verdict is Verdict.BLOCK

    def test_a_non_mandatory_gap_remains_policy_controlled(self):
        """The floor is a floor, not a straitjacket: extra requirements stay tunable."""
        policy = Policy.from_dict(
            {
                "required_capabilities": {"code": ["sast", "secrets", "license-risk"]},
                "suppressions": [
                    {"match": {"rule_id": "coverage/code/license-risk"}, "reason": "not in scope"}
                ],
            }
        )
        result = policy.apply(assessment(runs=satisfying_runs()))
        licence_gap = [f for f in result.findings if f.rule_id == "coverage/code/license-risk"]
        assert licence_gap and licence_gap[0].suppressed


class TestUnassessedLayersAreSurfaced:
    def test_a_configured_but_unrequested_layer_produces_a_finding(self):
        result = Policy().apply(
            assessment(
                runs=satisfying_runs(),
                layers=[Layer.CODE],
                available=[Layer.CODE, Layer.ARCHITECTURE, Layer.ENVIRONMENT],
            )
        )
        skipped = {
            f.rule_id for f in result.findings if "layer-not-assessed" in f.tags
        }
        assert skipped == {
            "coverage/layer-not-assessed/architecture",
            "coverage/layer-not-assessed/environment",
        }

    def test_the_finding_names_what_was_and_was_not_assessed(self):
        result = Policy().apply(
            assessment(runs=satisfying_runs(), layers=[Layer.CODE],
                       available=[Layer.CODE, Layer.ENVIRONMENT])
        )
        finding_ = next(f for f in result.findings if "layer-not-assessed" in f.tags)
        assert "assessed: code" in finding_.evidence
        assert "environment" in finding_.evidence

    def test_no_finding_when_every_available_layer_was_assessed(self):
        result = Policy().apply(
            assessment(runs=satisfying_runs(), layers=[Layer.CODE], available=[Layer.CODE])
        )
        assert not [f for f in result.findings if "layer-not-assessed" in f.tags]

    def test_an_unassessed_layer_prevents_a_silent_pass(self):
        result = Policy().apply(
            assessment(runs=satisfying_runs(), layers=[Layer.CODE],
                       available=[Layer.CODE, Layer.ENVIRONMENT])
        )
        assert result.verdict is Verdict.WARN
        assert result.verdict is not Verdict.PASS
