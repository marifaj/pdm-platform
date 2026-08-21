"""H2 — workspace and symlink confinement.

The repository under assessment is hostile input. These tests build repositories
that attack the gate: file symlinks to /etc/passwd, directory symlinks out of the
tree, relative ../.. escapes, and a scanner that lies about where a finding came
from. In every case the assertion is that MARKNA neither walks out of the
project nor quotes external content into a report.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from markna.confinement import (
    confine_findings,
    find_escaping_symlinks,
    iter_safe_files,
    path_within,
    resolve_within,
    safe_read_text,
)
from markna.models import Finding, Layer, Location, Severity
from markna.scanners.util import read_snippet

SECRET = "TOP-SECRET-HOST-FILE-CONTENTS"


@pytest.fixture
def hostile(tmp_path: Path) -> Path:
    """A project root containing every escape we know how to write."""
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "passwd").write_text(f"root:x:0:0:{SECRET}\n")
    (outside / "id_rsa").write_text(SECRET)

    project = tmp_path / "project"
    (project / "src").mkdir(parents=True)
    (project / "src" / "app.py").write_text("print('legitimate source')\n")

    # 1. file symlink to a file outside the workspace
    os.symlink(outside / "passwd", project / "src" / "passwd-link.py")
    # 2. relative traversal out of the tree
    os.symlink("../../outside/id_rsa", project / "src" / "key-link.py")
    # 3. directory symlink out of the tree
    os.symlink(outside, project / "escape-dir")
    # 4. a symlink that stays inside — legitimate, must keep working
    os.symlink(project / "src" / "app.py", project / "src" / "alias.py")
    return project


class TestResolution:
    def test_a_file_inside_the_root_resolves(self, hostile: Path):
        assert resolve_within(hostile / "src" / "app.py", hostile) is not None

    def test_a_symlink_out_of_the_root_does_not(self, hostile: Path):
        assert resolve_within(hostile / "src" / "passwd-link.py", hostile) is None

    def test_a_relative_escape_does_not(self, hostile: Path):
        assert resolve_within(hostile / "src" / "key-link.py", hostile) is None

    def test_a_dotdot_string_cannot_climb_out(self, hostile: Path):
        assert resolve_within("../outside/passwd", hostile) is None

    def test_an_internal_symlink_is_still_allowed(self, hostile: Path):
        assert path_within(hostile / "src" / "alias.py", hostile)

    def test_the_root_itself_is_within_itself(self, hostile: Path):
        assert path_within(hostile, hostile)

    def test_a_sibling_with_a_shared_prefix_is_not_within(self, tmp_path: Path):
        (tmp_path / "proj").mkdir()
        (tmp_path / "proj-evil").mkdir()
        assert not path_within(tmp_path / "proj-evil" / "f.py", tmp_path / "proj")


class TestReading:
    def test_reading_an_escaping_symlink_returns_nothing(self, hostile: Path):
        assert safe_read_text(hostile / "src" / "passwd-link.py", hostile) is None

    def test_the_secret_never_appears_in_a_snippet(self, hostile: Path):
        snippet = read_snippet(hostile / "src" / "passwd-link.py", 1, root=hostile)
        assert SECRET not in (snippet or "")

    def test_a_snippet_of_a_legitimate_file_still_works(self, hostile: Path):
        snippet = read_snippet(hostile / "src" / "app.py", 1, root=hostile)
        assert "legitimate source" in (snippet or "")

    def test_without_a_root_the_caller_gets_no_confinement(self, hostile: Path):
        """Documents the contract: the root is what confines, so adapters pass it."""
        assert safe_read_text(hostile / "src" / "app.py", hostile) is not None


class TestTraversal:
    def test_the_walk_yields_real_files(self, hostile: Path):
        names = {p.name for p in iter_safe_files(hostile)}
        assert "app.py" in names

    def test_the_walk_skips_escaping_file_symlinks(self, hostile: Path):
        names = {p.name for p in iter_safe_files(hostile)}
        assert "passwd-link.py" not in names
        assert "key-link.py" not in names

    def test_the_walk_never_descends_a_symlinked_directory(self, hostile: Path):
        walked = list(iter_safe_files(hostile))
        assert not any("outside" in str(p.resolve()) for p in walked)
        assert not any(p.name == "id_rsa" for p in walked)

    def test_no_walked_file_resolves_outside_the_root(self, hostile: Path):
        assert all(path_within(p, hostile) for p in iter_safe_files(hostile))

    def test_a_symlink_loop_does_not_hang_the_walk(self, tmp_path: Path):
        project = tmp_path / "loop"
        project.mkdir()
        (project / "a.py").write_text("x = 1\n")
        os.symlink(project, project / "self")
        assert {p.name for p in iter_safe_files(project)} == {"a.py"}


class TestDetection:
    def test_every_escaping_link_is_reported(self, hostile: Path):
        found = {link.name for link, _ in find_escaping_symlinks(hostile)}
        assert found == {"passwd-link.py", "key-link.py", "escape-dir"}

    def test_an_internal_link_is_not_reported(self, hostile: Path):
        assert "alias.py" not in {link.name for link, _ in find_escaping_symlinks(hostile)}

    def test_a_clean_repository_reports_nothing(self, tmp_path: Path):
        clean = tmp_path / "clean"
        clean.mkdir()
        (clean / "app.py").write_text("x = 1\n")
        assert find_escaping_symlinks(clean) == []


class TestReportConfinement:
    def _finding(self, path: Path) -> Finding:
        return Finding(
            source="semgrep",
            layer=Layer.CODE,
            severity=Severity.HIGH,
            title="Hardcoded credential",
            explanation="A scanner followed a link and reported this.",
            evidence=SECRET,
            location=Location(file=str(path), line=1),
            remediation="n/a",
            rule_id="test/leak",
        )

    def test_evidence_from_outside_the_root_is_withheld(self, hostile: Path):
        result = confine_findings([self._finding(hostile / "escape-dir" / "passwd")], hostile)
        leaked = [f for f in result if SECRET in (f.evidence or "")]
        assert not leaked, "external file contents must never reach the report"

    def test_the_escape_itself_becomes_a_finding(self, hostile: Path):
        result = confine_findings([self._finding(hostile / "escape-dir" / "passwd")], hostile)
        assert any(f.rule_id == "confinement/path-outside-project-root" for f in result)

    def test_the_offending_finding_is_tagged_not_dropped(self, hostile: Path):
        result = confine_findings([self._finding(hostile / "escape-dir" / "passwd")], hostile)
        original = next(f for f in result if f.source == "semgrep")
        assert "outside-project-root" in original.tags
        assert original.location.file, "the location is kept so the escape is diagnosable"

    def test_a_legitimate_finding_is_untouched(self, hostile: Path):
        original = self._finding(hostile / "src" / "app.py")
        result = confine_findings([original], hostile)
        assert result == [original]
        assert result[0].evidence == SECRET


class TestNoAdapterReadsWithoutARoot:
    """Structural, not behavioural: a new adapter must not reintroduce the gap.

    ``read_snippet`` confines its read only when given a root, so a call site
    that forgets it is a silent hole. Parsing the call sites is the only way to
    assert that none of them do, including ones nobody has written yet.
    """

    def _call_sites(self):
        import ast
        from pathlib import Path as _Path

        engine = _Path(__file__).resolve().parent.parent / "markna"
        for source_file in engine.rglob("*.py"):
            tree = ast.parse(source_file.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
                if name == "read_snippet":
                    yield source_file, node

    def test_every_read_snippet_call_passes_a_root(self):
        offenders = [
            f"{path.name}:{node.lineno}"
            for path, node in self._call_sites()
            if "root" not in {keyword.arg for keyword in node.keywords}
        ]
        assert not offenders, f"read_snippet called without confinement at: {offenders}"

    def test_the_check_finds_the_call_sites_at_all(self):
        assert list(self._call_sites()), "the AST walk found nothing — the check is vacuous"


class TestFileDiscoveryIsConfined:
    def test_a_symlinked_manifest_is_not_discovered(self, tmp_path: Path):
        """A repository can name a file `requirements.txt` and aim it elsewhere."""
        from markna.scanners.util import find_files

        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "real.txt").write_text("requests==2.0.0\n")
        project = tmp_path / "project"
        project.mkdir()
        (project / "requirements.txt").write_text("flask==1.0\n")
        os.symlink(outside / "real.txt", project / "requirements-dev.txt")

        found = {p.name for p in find_files(project, ["requirements*.txt"])}
        assert found == {"requirements.txt"}

    def test_legitimate_manifests_are_still_discovered(self, tmp_path: Path):
        from markna.scanners.util import find_files

        project = tmp_path / "project"
        (project / "svc").mkdir(parents=True)
        (project / "requirements.txt").write_text("a\n")
        (project / "svc" / "requirements.txt").write_text("b\n")
        assert len(find_files(project, ["**/requirements.txt", "requirements.txt"])) >= 2
