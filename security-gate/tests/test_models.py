"""Finding identity, severity parsing and assessment summarisation."""

from markna.models import (
    Assessment,
    Finding,
    Layer,
    Location,
    Severity,
    Target,
    Verdict,
    assign_ids,
    sort_findings,
    utc_now,
)


def make_finding(**overrides) -> Finding:
    defaults = dict(
        source="test",
        layer=Layer.CODE,
        severity=Severity.HIGH,
        title="Example finding",
        explanation="Something is wrong.",
        evidence="line 1",
        location=Location(file="app.py", line=10),
        remediation="Fix it.",
        rule_id="test/example",
    )
    defaults.update(overrides)
    return Finding(**defaults)


class TestSeverity:
    def test_named_severities(self):
        assert Severity.parse("CRITICAL") is Severity.CRITICAL
        assert Severity.parse("moderate") is Severity.MEDIUM
        assert Severity.parse("WARNING") is Severity.MEDIUM
        assert Severity.parse("informational") is Severity.INFO

    def test_cvss_scores_map_to_bands(self):
        assert Severity.parse(9.8) is Severity.CRITICAL
        assert Severity.parse("7.5") is Severity.HIGH
        assert Severity.parse(4.0) is Severity.MEDIUM
        assert Severity.parse(0.5) is Severity.LOW
        assert Severity.parse(0) is Severity.INFO

    def test_unknown_falls_back_to_the_given_default(self):
        assert Severity.parse("nonsense") is Severity.INFO
        assert Severity.parse(None, Severity.MEDIUM) is Severity.MEDIUM

    def test_rank_orders_worst_first(self):
        ranks = [severity.rank for severity in
                 (Severity.INFO, Severity.LOW, Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL)]
        assert ranks == sorted(ranks)


class TestFindingIdentity:
    def test_fingerprint_is_stable_across_equivalent_findings(self):
        assert make_finding().compute_fingerprint() == make_finding().compute_fingerprint()

    def test_fingerprint_changes_with_location(self):
        other = make_finding(location=Location(file="app.py", line=11))
        assert make_finding().compute_fingerprint() != other.compute_fingerprint()

    def test_fingerprint_ignores_whitespace_noise_in_evidence(self):
        spaced = make_finding(evidence="line    1\n")
        assert make_finding().compute_fingerprint() == spaced.compute_fingerprint()

    def test_id_encodes_the_layer(self):
        assert make_finding(layer=Layer.ENVIRONMENT).finalise().id.startswith("MK-ENV-")
        assert make_finding(layer=Layer.ARCHITECTURE).finalise().id.startswith("MK-ARC-")

    def test_identical_findings_still_get_unique_ids(self):
        findings = assign_ids([make_finding(), make_finding(), make_finding()])
        assert len({finding.id for finding in findings}) == 3

    def test_reassignment_keeps_earlier_ids_stable(self):
        first = make_finding()
        second = make_finding(title="Another", rule_id="test/other")
        assign_ids([first, second])
        original = first.id
        assign_ids([first, second, make_finding(title="Third", rule_id="test/third")])
        assert first.id == original


class TestAssessment:
    def _assessment(self, findings) -> Assessment:
        return Assessment(
            assessment_id="MKA-TEST",
            target=Target(name="unit-test"),
            policy_name="test",
            started_at=utc_now(),
            finished_at=utc_now(),
            layers_requested=[Layer.CODE],
            findings=assign_ids(findings),
        )

    def test_summary_counts_exclude_suppressed_findings(self):
        suppressed = make_finding(title="Suppressed", rule_id="test/suppressed")
        suppressed.suppressed = True
        assessment = self._assessment([make_finding(), suppressed])
        summary = assessment.to_dict()["summary"]
        assert summary["total_findings"] == 1
        assert summary["suppressed_findings"] == 1

    def test_summary_separates_ai_from_deterministic(self):
        advisory = make_finding(title="Advisory", rule_id="ai/x", ai_generated=True)
        assessment = self._assessment([make_finding(), advisory])
        summary = assessment.to_dict()["summary"]
        assert summary["deterministic_findings"] == 1
        assert summary["advisory_ai_findings"] == 1

    def test_verdict_defaults_to_pass_before_scoring(self):
        assert self._assessment([]).verdict is Verdict.PASS


def test_sort_puts_worst_and_blocking_first():
    low = make_finding(severity=Severity.LOW, title="low", rule_id="a")
    critical = make_finding(severity=Severity.CRITICAL, title="crit", rule_id="b")
    blocking_high = make_finding(title="high", rule_id="c")
    blocking_high.blocking = True
    ordered = sort_findings(assign_ids([low, blocking_high, critical]))
    assert [finding.title for finding in ordered] == ["crit", "high", "low"]


def test_location_describe_prefers_file_then_url_then_component():
    assert Location(file="a.py", line=3, end_line=5).describe() == "a.py:3-5"
    assert Location(url="https://example.com").describe() == "https://example.com"
    assert Location(component="secrets").describe() == "secrets"
    assert Location().describe() == "n/a"
