"""M9 — real scanner execution against the vulnerable fixture.

Everything else in this suite asserts that MARKNA parses a scanner's output
correctly. These tests assert that the scanners actually run: the real binary,
the real ruleset, the real repository, in the same code path the worker uses.
A unit test with a canned JSON blob proves the parser works; it cannot prove the
command line is valid, the exit codes are handled, or the rules match anything.

Skipped when the binaries are absent, so a machine without the scanner toolchain
can still run the suite — but a skip is a gap, not a pass. `-p no:cacheprovider
--runxfail` is not needed; run with `-rs` to see what was skipped.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from markna.exec import ToolPath
from markna.models import Layer, Severity
from markna.scanners.base import ScannerContext, get_scanner

FIXTURE = Path(__file__).resolve().parent.parent / "examples" / "vulnerable-demo"

requires_semgrep = pytest.mark.skipif(
    shutil.which("semgrep") is None, reason="semgrep is not installed on this host"
)
requires_bandit = pytest.mark.skipif(
    shutil.which("bandit") is None, reason="bandit is not installed on this host"
)


@pytest.fixture
def context(tmp_path: Path) -> ScannerContext:
    """A scanner context pointed at a private copy of the vulnerable fixture."""
    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project)
    workdir = tmp_path / "work"
    workdir.mkdir()
    return ScannerContext(
        workdir=workdir,
        project_path=project,
        tool_path=ToolPath.from_env(),
        timeout=300,
        offline=True,
    )


def run(name: str, context: ScannerContext):
    scanner = get_scanner(name)
    assert scanner is not None, f"{name} is not registered"
    applicable, reason = scanner.applicable(context)
    assert applicable, reason
    available, reason = scanner.available(context)
    assert available, reason
    return scanner, list(scanner.scan(context))


@requires_semgrep
class TestSemgrepRunsForReal:
    def test_it_produces_findings_on_the_vulnerable_fixture(self, context):
        _, findings = run("semgrep", context)
        assert findings, "semgrep found nothing in a deliberately vulnerable project"

    def test_the_command_that_ran_is_recorded(self, context):
        run("semgrep", context)
        assert any("semgrep" in command for command in context.commands)

    def test_the_findings_are_normalised(self, context):
        _, findings = run("semgrep", context)
        real = [f for f in findings if f.severity is not Severity.INFO]
        assert real, "expected at least one non-informational finding"
        for finding in real:
            assert finding.layer is Layer.CODE
            assert finding.source == "semgrep"
            assert finding.title and finding.explanation and finding.remediation
            assert finding.rule_id
            assert finding.location.file, "a SAST finding must name a file"

    def test_every_reported_file_is_inside_the_project(self, context):
        from markna.confinement import path_within

        _, findings = run("semgrep", context)
        for finding in findings:
            if finding.location.file:
                assert path_within(finding.location.file, context.project_path)

    def test_the_version_probe_answers(self, context):
        scanner = get_scanner("semgrep")
        assert scanner.tool_version(context), "the version probe must not hang or return nothing"

    def test_it_finds_the_injection_the_fixture_contains(self, context):
        _, findings = run("semgrep", context)
        text = " ".join(
            f"{f.rule_id} {f.title}".lower() for f in findings
        )
        assert any(word in text for word in ("sql", "injection", "subprocess", "shell", "eval")), (
            "the bundled ruleset matched nothing recognisable in the fixture"
        )

    def test_a_clean_project_produces_no_real_findings(self, context, tmp_path):
        clean = tmp_path / "clean"
        clean.mkdir()
        (clean / "safe.py").write_text("def add(a, b):\n    return a + b\n")
        context.project_path = clean
        _, findings = run("semgrep", context)
        assert not [f for f in findings if f.severity is not Severity.INFO]


@requires_bandit
class TestBanditRunsForReal:
    def test_it_produces_findings_on_the_vulnerable_fixture(self, context):
        _, findings = run("bandit", context)
        assert findings, "bandit found nothing in a deliberately vulnerable project"

    def test_the_findings_are_normalised(self, context):
        _, findings = run("bandit", context)
        for finding in findings:
            assert finding.layer is Layer.CODE
            assert finding.source == "bandit"
            assert finding.title and finding.explanation and finding.remediation
            assert finding.location.file and finding.location.line

    def test_it_reports_a_cwe(self, context):
        _, findings = run("bandit", context)
        assert any(finding.cwe for finding in findings), "bandit findings should carry CWE ids"

    def test_every_reported_file_is_inside_the_project(self, context):
        from markna.confinement import path_within

        _, findings = run("bandit", context)
        for finding in findings:
            assert path_within(finding.location.file, context.project_path)

    def test_the_version_probe_answers(self, context):
        assert get_scanner("bandit").tool_version(context)

    def test_a_project_with_no_python_is_not_applicable(self, context, tmp_path):
        empty = tmp_path / "go-project"
        empty.mkdir()
        (empty / "main.go").write_text("package main\n")
        context.project_path = empty
        applicable, reason = get_scanner("bandit").applicable(context)
        assert not applicable and "Python" in reason

    def test_a_clean_project_produces_nothing(self, context, tmp_path):
        clean = tmp_path / "clean"
        clean.mkdir()
        (clean / "safe.py").write_text("def add(a, b):\n    return a + b\n")
        context.project_path = clean
        _, findings = run("bandit", context)
        assert findings == []


@requires_semgrep
@requires_bandit
class TestTheTwoAgree:
    def test_both_scanners_see_the_same_repository(self, context):
        _, semgrep = run("semgrep", context)
        _, bandit = run("bandit", context)
        semgrep_files = {Path(f.location.file).name for f in semgrep if f.location.file}
        bandit_files = {Path(f.location.file).name for f in bandit if f.location.file}
        assert semgrep_files & bandit_files, (
            "two SAST tools on the same vulnerable file should overlap somewhere"
        )

    def test_neither_escapes_the_project_via_a_symlink(self, context, tmp_path):
        """The fixture is made hostile: a symlink to a file outside the workspace."""
        import os

        secret = tmp_path / "outside-secret.txt"
        secret.write_text("AWS_SECRET_ACCESS_KEY = 'do-not-report-this'\n")
        os.symlink(secret, context.project_path / "linked_secret.py")

        from markna.confinement import confine_findings

        for name in ("semgrep", "bandit"):
            _, findings = run(name, context)
            confined = confine_findings(findings, context.project_path)
            assert not any(
                "do-not-report-this" in (f.evidence or "") for f in confined
            ), f"{name} results leaked content from outside the project"
