"""CLI behaviour, including the exit-code contract with CI."""

import json
from pathlib import Path

import pytest

from markna.cli import EXIT_BLOCK, EXIT_ERROR, EXIT_PASS, EXIT_WARN, main
from markna.models import Verdict
from markna.cli import _exit_code

EXAMPLE_DIR = Path(__file__).resolve().parent.parent / "examples" / "vulnerable-demo"


class TestExitCodes:
    def test_pass_is_zero(self):
        assert _exit_code(Verdict.PASS, "block") == EXIT_PASS

    def test_block_is_two(self):
        assert _exit_code(Verdict.BLOCK, "block") == EXIT_BLOCK

    def test_warn_is_zero_by_default(self):
        assert _exit_code(Verdict.WARN, "block") == EXIT_PASS

    def test_warn_is_one_when_requested(self):
        assert _exit_code(Verdict.WARN, "warn") == EXIT_WARN

    def test_fail_on_never_always_passes(self):
        assert _exit_code(Verdict.BLOCK, "never") == EXIT_PASS


class TestScannersCommand:
    def test_lists_the_registry(self, capsys):
        assert main(["scanners"]) == EXIT_PASS
        output = capsys.readouterr().out
        assert "semgrep" in output
        assert "arch-manifest" in output
        assert "env-tls" in output

    def test_json_output_is_machine_readable(self, capsys):
        assert main(["scanners", "--json"]) == EXIT_PASS
        rows = json.loads(capsys.readouterr().out)
        assert all({"name", "layer", "capabilities", "available"} <= set(row) for row in rows)

    def test_ai_scanner_is_marked_non_deterministic(self, capsys):
        main(["scanners", "--json"])
        rows = json.loads(capsys.readouterr().out)
        ai = next(row for row in rows if row["name"] == "ai-advisory")
        assert ai["deterministic"] is False


class TestCapabilitiesCommand:
    def test_lists_the_vocabulary(self, capsys):
        assert main(["capabilities"]) == EXIT_PASS
        output = capsys.readouterr().out
        assert "sast" in output and "transport-security" in output


class TestInitCommand:
    def test_writes_the_templates(self, tmp_path, capsys):
        assert main(["init", "--dir", str(tmp_path)]) == EXIT_PASS
        for name in ("markna.yaml", "markna-policy.yaml", "architecture.yaml"):
            assert (tmp_path / name).is_file()

    def test_does_not_overwrite_without_force(self, tmp_path, capsys):
        (tmp_path / "architecture.yaml").write_text("mine\n")
        main(["init", "--dir", str(tmp_path)])
        assert (tmp_path / "architecture.yaml").read_text() == "mine\n"
        assert "skipped" in capsys.readouterr().out

    def test_force_overwrites(self, tmp_path):
        (tmp_path / "architecture.yaml").write_text("mine\n")
        main(["init", "--dir", str(tmp_path), "--force"])
        assert (tmp_path / "architecture.yaml").read_text() != "mine\n"


class TestAssessValidation:
    def test_requires_a_target(self, capsys):
        assert main(["assess"]) == EXIT_ERROR
        assert "nothing to assess" in capsys.readouterr().err

    def test_environment_layer_requires_authorization(self, capsys):
        code = main(["assess", "--url", "https://uat.example.invalid", "--layers", "environment"])
        assert code == EXIT_ERROR
        assert "authorized-by" in capsys.readouterr().err

    def test_unknown_layer_is_rejected(self, capsys):
        code = main(["assess", "--project", ".", "--layers", "infra"])
        assert code == EXIT_ERROR
        assert "unknown layer" in capsys.readouterr().err

    def test_unknown_scanner_is_rejected(self, capsys):
        code = main(["assess", "--project", ".", "--layers", "code", "--only", "nosuchscanner"])
        assert code == EXIT_ERROR
        assert "unknown scanner" in capsys.readouterr().err

    def test_missing_architecture_document_is_reported(self, capsys):
        code = main(["assess", "--arch", "/nonexistent/arch.md", "--layers", "architecture"])
        assert code == EXIT_ERROR
        assert "not found" in capsys.readouterr().err

    def test_private_environment_target_is_refused_without_the_flag(self, capsys):
        code = main(
            [
                "assess",
                "--url",
                "http://127.0.0.1:9",
                "--layers",
                "environment",
                "--authorized-by",
                "pytest",
            ]
        )
        assert code == EXIT_ERROR
        assert "private" in capsys.readouterr().err


class TestAssessArchitectureLayer:
    """A full run over the bundled insecure example, architecture layer only.

    No external scanner binaries are needed, so this runs anywhere.
    """

    @pytest.fixture
    def reports(self, tmp_path, capsys):
        code = main(
            [
                "assess",
                "--arch",
                str(EXAMPLE_DIR / "docs" / "architecture.md"),
                "--arch-manifest",
                str(EXAMPLE_DIR / "architecture.yaml"),
                "--layers",
                "architecture",
                "--out",
                str(tmp_path / "reports"),
                "--workdir",
                str(tmp_path / "work"),
                "--format",
                "json,markdown",
                "--quiet",
            ]
        )
        capsys.readouterr()
        data = json.loads((tmp_path / "reports" / "markna-report.json").read_text())
        return code, data

    def test_insecure_example_blocks(self, reports):
        code, data = reports
        assert code == EXIT_BLOCK
        assert data["verdict"] == "BLOCK"

    def test_findings_come_from_both_architecture_scanners(self, reports):
        _, data = reports
        sources = {finding["source"] for finding in data["findings"]}
        assert "arch-manifest" in sources
        assert "arch-doc-checklist" in sources

    def test_no_ai_findings_without_the_ai_flag(self, reports):
        _, data = reports
        assert data["summary"]["advisory_ai_findings"] == 0

    def test_architecture_coverage_is_satisfied(self, reports):
        _, data = reports
        coverage = {entry["capability"]: entry["satisfied"] for entry in data["coverage"]}
        assert coverage == {"architecture-review": True}

    def test_every_finding_carries_remediation(self, reports):
        _, data = reports
        assert all(finding["remediation"] for finding in data["findings"])

    def test_markdown_report_is_written(self, tmp_path, reports):
        report = tmp_path / "reports" / "markna-report.md"
        assert report.is_file()
        assert "MARKNA Security Gate Report" in report.read_text()


class TestConfigFile:
    def test_config_file_supplies_the_target(self, tmp_path, capsys):
        config = tmp_path / "markna.yaml"
        config.write_text(
            "architecture:\n"
            f"  manifest: {EXAMPLE_DIR / 'architecture.yaml'}\n"
            "layers:\n  - architecture\n"
        )
        code = main(
            [
                "assess",
                "--config",
                str(config),
                "--out",
                str(tmp_path / "reports"),
                "--workdir",
                str(tmp_path / "work"),
                "--format",
                "json",
                "--quiet",
            ]
        )
        capsys.readouterr()
        assert code == EXIT_BLOCK
        assert (tmp_path / "reports" / "markna-report.json").is_file()

    def test_missing_config_file_is_reported(self, capsys):
        assert main(["assess", "--config", "/nonexistent/markna.yaml"]) == EXIT_ERROR
        assert "config file not found" in capsys.readouterr().err
