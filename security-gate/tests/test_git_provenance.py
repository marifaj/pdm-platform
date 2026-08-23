"""P778-IR-002 — the recorded commit must belong to the repository assessed.

Reproduced before the fix, by running the shipped runner against a prepared
directory:

* ``.git`` replaced by a ``gitdir:`` pointer file naming an unrelated
  repository -> that repository's commit and branch were recorded;
* ``.git`` replaced by a symlink to an unrelated repository -> likewise;
* the project directory not being a repository at all -> its *parent's* history
  was recorded, from outside the assessed directory;
* ``.git`` replaced between the two ``git rev-parse`` calls -> the commit of one
  repository was recorded against the branch name of another.

All four are the same defect: provenance was answered by a program whose job is
to search for a repository, reading a mutable name twice. It is now answered by
reading ``HEAD`` and the ref it names through a descriptor held on the project's
own ``.git``.

The properties asserted here are what that buys, stated as properties rather
than as a list of blocked mechanisms:

P1  Provenance comes from ``<project>/.git`` or from nowhere. Nothing is
    followed out of the assessed directory and nothing is discovered upward.
P2  Reads are relative to a held descriptor, so changing the ``.git`` *name*
    after capture begins cannot change what was read.
P3  A repository that is present but not trustworthy produces a stated reason,
    never a silent absence. An absent result is not a clean result.
"""

from __future__ import annotations

import os
import shutil
import subprocess

import pytest

from markna.provenance import GitProvenance, capture

_COMMIT_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@example",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@example",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": os.devnull,
}


def _git(cwd, *args):
    result = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        env={**os.environ, **_COMMIT_ENV},
    )
    if result.returncode != 0:
        pytest.skip(f"git unavailable or failed: {result.stderr.strip()[:200]}")
    return result.stdout.strip()


def _repository(path, *, content, branch):
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "-q", "-b", branch)
    (path / "file.txt").write_text(content)
    _git(path, "add", "-A")
    _git(path, "commit", "-qm", content)
    return _git(path, "rev-parse", "HEAD")


@pytest.fixture
def repositories(tmp_path):
    """An assessed repository and an unrelated one to substitute for it."""
    assessed = tmp_path / "assessed"
    unrelated = tmp_path / "unrelated"
    assessed_head = _repository(assessed, content="assessed", branch="main")
    unrelated_head = _repository(unrelated, content="unrelated", branch="attacker-branch")
    assert assessed_head != unrelated_head
    return assessed, assessed_head, unrelated, unrelated_head


# --------------------------------------------------------- the honest answer


class TestTheOrdinaryCase:
    def test_the_projects_own_history_is_recorded(self, repositories):
        assessed, head, _, _ = repositories
        provenance = capture(assessed)
        assert provenance.commit == head
        assert provenance.branch == "main"
        assert provenance.note is None
        assert provenance.established

    def test_a_packed_ref_is_resolved(self, repositories):
        """`git gc` moves refs out of refs/heads and into packed-refs."""
        assessed, head, _, _ = repositories
        _git(assessed, "gc", "-q", "--prune=now")
        assert not (assessed / ".git" / "refs" / "heads" / "main").exists()
        assert capture(assessed).commit == head

    def test_a_detached_head_records_the_commit_and_says_so(self, repositories):
        assessed, head, _, _ = repositories
        _git(assessed, "checkout", "-q", "--detach")
        provenance = capture(assessed)
        assert provenance.commit == head
        assert provenance.branch is None
        assert "detached" in provenance.note

    def test_a_repository_with_no_commits_records_no_commit(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        _git(empty, "init", "-q", "-b", "main")
        provenance = capture(empty)
        assert provenance.commit is None
        assert provenance.branch == "main"
        assert provenance.present and "no commit" in provenance.note


# ---------------------------------------------- P1: no history from elsewhere


class TestNoHistoryFromElsewhere:
    def test_a_gitdir_pointer_file_is_refused(self, repositories):
        """A worktree's `.git`, and the shape the reviewer used."""
        assessed, _, unrelated, unrelated_head = repositories
        shutil.rmtree(assessed / ".git")
        (assessed / ".git").write_text(f"gitdir: {unrelated / '.git'}\n")

        provenance = capture(assessed)

        assert provenance.commit is None and provenance.branch is None
        assert provenance.commit != unrelated_head
        assert provenance.present and "pointer" in provenance.note

    def test_a_symlinked_gitdir_is_refused(self, repositories):
        assessed, _, unrelated, unrelated_head = repositories
        shutil.rmtree(assessed / ".git")
        (assessed / ".git").symlink_to(unrelated / ".git")

        provenance = capture(assessed)

        assert provenance.commit is None
        assert provenance.commit != unrelated_head
        assert provenance.present and "symbolic link" in provenance.note

    def test_a_commondir_delegation_is_refused(self, repositories):
        """The refs live in another repository, so the history is not this one's."""
        assessed, _, unrelated, _ = repositories
        (assessed / ".git" / "commondir").write_text(f"{unrelated / '.git'}\n")

        provenance = capture(assessed)

        assert provenance.commit is None
        assert provenance.present and "commondir" in provenance.note

    def test_a_project_that_is_not_a_repository_reports_nothing(self, tmp_path):
        """Discovery used to walk upward and report the parent's history."""
        outer = tmp_path / "outer"
        outer_head = _repository(outer, content="outer", branch="outer-branch")
        inner = outer / "inner"
        inner.mkdir()

        provenance = capture(inner)

        assert provenance.commit is None and provenance.branch is None
        assert provenance.commit != outer_head
        assert provenance.present is False
        assert provenance.note is None  # not a repository is not a refusal

    def test_a_symlinked_refs_directory_is_refused(self, repositories):
        """`refs` swapped for a link into another repository after HEAD is read."""
        assessed, _, unrelated, unrelated_head = repositories
        shutil.rmtree(assessed / ".git" / "refs")
        (assessed / ".git" / "refs").symlink_to(unrelated / ".git" / "refs")

        provenance = capture(assessed)

        assert provenance.commit != unrelated_head
        assert provenance.commit is None
        assert "symbolic link" in provenance.note

    def test_a_symlinked_ref_file_is_refused(self, repositories):
        assessed, _, unrelated, unrelated_head = repositories
        loose = assessed / ".git" / "refs" / "heads" / "main"
        if not loose.exists():  # pragma: no cover - depends on git's packing
            loose.parent.mkdir(parents=True, exist_ok=True)
        else:
            loose.unlink()
        loose.symlink_to(unrelated / ".git" / "refs" / "heads" / "attacker-branch")

        provenance = capture(assessed)

        assert provenance.commit != unrelated_head
        assert provenance.commit is None
        assert "symbolic link" in provenance.note

    def test_a_symlinked_head_is_refused(self, repositories):
        assessed, _, unrelated, unrelated_head = repositories
        (assessed / ".git" / "HEAD").unlink()
        (assessed / ".git" / "HEAD").symlink_to(unrelated / ".git" / "HEAD")

        provenance = capture(assessed)

        assert provenance.commit != unrelated_head
        assert provenance.commit is None
        assert "symbolic link" in provenance.note

    @pytest.mark.parametrize(
        "ref",
        [
            "ref: ../../../../etc/passwd",
            "ref: refs/../../../etc/passwd",
            "ref: /etc/passwd",
            "ref: refs/heads/../../../secret",
            "ref: objects/info/alternates",
        ],
    )
    def test_head_cannot_be_used_to_read_outside_refs(self, repositories, ref):
        """HEAD is repository-controlled, so it is a traversal primitive."""
        assessed, _, _, _ = repositories
        (assessed / ".git" / "HEAD").write_text(ref + "\n")

        provenance = capture(assessed)

        assert provenance.commit is None
        assert provenance.present and provenance.note
        assert "HEAD" in provenance.note, "the refusal must name the file that caused it"

    def test_head_pointing_at_a_non_ref_file_is_refused_before_it_is_read(
        self, repositories
    ):
        """The decisive case: a plausible object name in a file that is not a ref.

        Every other traversal attempt is also caught by the later check that a
        ref must contain an object name. This one is not -- the decoy contains a
        perfectly good commit id -- so only refusing the *path* stops it.
        """
        assessed, assessed_head, _, unrelated_head = repositories
        (assessed / ".git" / "decoy").write_text(unrelated_head + "\n")
        (assessed / ".git" / "HEAD").write_text("ref: decoy\n")

        provenance = capture(assessed)

        assert provenance.commit != unrelated_head
        assert provenance.commit is None
        assert "outside refs/" in provenance.note


# ------------------------------------------ P2: no time-of-check/time-of-use


class TestNoTimeOfCheckGap:
    """A mid-capture replacement can empty provenance; it cannot falsify it.

    That distinction is the whole property, so it is asserted rather than
    glossed: the recorded commit is either the assessed repository's or absent
    with a reason, and is never the repository that was swapped in.
    """

    def test_replacing_the_gitdir_mid_capture_cannot_substitute_a_history(
        self, repositories, monkeypatch
    ):
        """The reviewer's race, made deterministic.

        The swap is triggered from inside the read of HEAD, i.e. after the
        descriptor is held and before the ref is resolved -- the exact window
        the two-subprocess implementation lost.
        """
        assessed, assessed_head, unrelated, unrelated_head = repositories
        import markna.provenance as provenance_module

        original = provenance_module._read_small
        state = {"swapped": False}

        def swapping(fd, components, device, limit):
            result = original(fd, components, device, limit)
            if not state["swapped"]:
                state["swapped"] = True
                shutil.rmtree(assessed / ".git")
                shutil.copytree(unrelated / ".git", assessed / ".git")
            return result

        monkeypatch.setattr(provenance_module, "_read_small", swapping)
        provenance = capture(assessed)

        assert state["swapped"], "the test did not exercise the window it claims to"
        assert provenance.commit != unrelated_head, "the substituted history was admitted"
        assert provenance.branch != "attacker-branch", "the substituted branch was admitted"
        assert provenance.commit in (assessed_head, None)
        assert provenance.branch == "main"  # from the single HEAD read, before the swap
        if provenance.commit is None:
            assert provenance.note, "an emptied provenance must say why"

    def test_deleting_the_gitdir_mid_capture_empties_provenance_with_a_reason(
        self, repositories, monkeypatch
    ):
        assessed, assessed_head, _, _ = repositories
        import markna.provenance as provenance_module

        original = provenance_module._read_small
        state = {"removed": False}

        def removing(fd, components, device, limit):
            result = original(fd, components, device, limit)
            if not state["removed"]:
                state["removed"] = True
                shutil.rmtree(assessed / ".git")
            return result

        monkeypatch.setattr(provenance_module, "_read_small", removing)
        provenance = capture(assessed)

        assert state["removed"]
        assert provenance.commit in (assessed_head, None)
        if provenance.commit is None:
            assert provenance.note, "an emptied provenance must say why"

    def test_a_replacement_that_leaves_the_gitdir_alone_is_still_the_project_s_own(
        self, repositories, monkeypatch
    ):
        """The other half of the property: no false *emptiness* either.

        Replacing a sibling of `.git` -- anything the capture does not read --
        must not disturb a capture that is already under way.
        """
        assessed, assessed_head, unrelated, _ = repositories
        import markna.provenance as provenance_module

        original = provenance_module._read_small
        state = {"touched": False}

        def touching(fd, components, device, limit):
            result = original(fd, components, device, limit)
            if not state["touched"]:
                state["touched"] = True
                shutil.rmtree(assessed / ".git" / "objects")
                shutil.copytree(unrelated / ".git" / "objects", assessed / ".git" / "objects")
            return result

        monkeypatch.setattr(provenance_module, "_read_small", touching)
        provenance = capture(assessed)

        assert state["touched"]
        assert provenance.commit == assessed_head
        assert provenance.note is None

    def test_a_directory_swapped_between_inspection_and_open_is_refused(
        self, repositories, monkeypatch
    ):
        """The lstat-then-open window, closed by comparing inodes.

        `refs/heads` is replaced with a different *real* directory after it has
        been inspected as a directory and before it is opened, so `O_NOFOLLOW`
        has nothing to object to. The substitute holds a `main` naming the
        unrelated repository's commit: without the identity re-check, that
        commit is what gets recorded.
        """
        assessed, assessed_head, unrelated, unrelated_head = repositories
        import markna.provenance as provenance_module

        substitute = unrelated / "substitute-heads"
        substitute.mkdir()
        (substitute / "main").write_text(unrelated_head + "\n")

        heads = assessed / ".git" / "refs" / "heads"
        heads.mkdir(parents=True, exist_ok=True)
        (heads / "main").write_text(assessed_head + "\n")

        original = provenance_module._lstat_at
        state = {"swapped": False}

        def swapping(name, fd):
            info = original(name, fd)
            if name == "heads" and not state["swapped"]:
                state["swapped"] = True
                shutil.rmtree(heads)
                os.rename(substitute, heads)
            return info

        monkeypatch.setattr(provenance_module, "_lstat_at", swapping)
        provenance = capture(assessed)

        assert state["swapped"], "the test did not exercise the window it claims to"
        assert provenance.commit != unrelated_head, "the substituted directory was read"
        assert provenance.commit is None
        assert "changed while it was being read" in provenance.note

    def test_provenance_is_read_in_one_pass_not_two(self, repositories):
        """Commit and branch come from one HEAD read, so they cannot be stitched.

        The defect was two independent `git rev-parse` invocations. This asserts
        the structural property that made it possible is gone: no subprocess is
        run to answer the question at all.
        """
        import markna.provenance as provenance_module

        assert not hasattr(provenance_module, "run_command")
        source = (provenance_module.__file__ or "")
        assert source
        with open(source, "r", encoding="utf-8") as stream:
            text = stream.read()
        body = text.split('"""', 2)[-1]  # exclude the module docstring's narrative
        assert "subprocess" not in body
        assert "rev-parse" not in body


# ------------------------------------ the object store is simply not consulted


class TestTheObjectStoreIsNotConsulted:
    """Provenance is refs, not objects, so object substitution cannot move it."""

    def test_a_symlinked_object_store_does_not_affect_provenance(self, repositories):
        assessed, assessed_head, unrelated, _ = repositories
        shutil.rmtree(assessed / ".git" / "objects")
        (assessed / ".git" / "objects").symlink_to(unrelated / ".git" / "objects")

        assert capture(assessed).commit == assessed_head

    def test_a_replaced_object_store_does_not_affect_provenance(self, repositories):
        assessed, assessed_head, unrelated, _ = repositories
        shutil.rmtree(assessed / ".git" / "objects")
        shutil.copytree(unrelated / ".git" / "objects", assessed / ".git" / "objects")

        assert capture(assessed).commit == assessed_head

    def test_an_alternates_file_does_not_affect_provenance(self, repositories):
        assessed, assessed_head, unrelated, _ = repositories
        info = assessed / ".git" / "objects" / "info"
        info.mkdir(parents=True, exist_ok=True)
        (info / "alternates").write_text(f"{unrelated / '.git' / 'objects'}\n")

        assert capture(assessed).commit == assessed_head


# --------------------------------------------- P3: refusals are never silent


class TestRefusalsAreVisible:
    def test_an_unreadable_head_is_refused_with_a_reason(self, repositories):
        assessed, _, _, _ = repositories
        (assessed / ".git" / "HEAD").write_text("not a ref and not a sha\n")
        provenance = capture(assessed)
        assert provenance.commit is None
        assert provenance.present and provenance.note

    def test_an_empty_head_is_refused_with_a_reason(self, repositories):
        assessed, _, _, _ = repositories
        (assessed / ".git" / "HEAD").write_text("")
        provenance = capture(assessed)
        assert provenance.commit is None and "empty" in provenance.note

    def test_a_ref_that_is_not_an_object_name_is_refused(self, repositories):
        assessed, _, _, _ = repositories
        loose = assessed / ".git" / "refs" / "heads" / "main"
        loose.parent.mkdir(parents=True, exist_ok=True)
        loose.write_text("../../../etc/passwd\n")
        provenance = capture(assessed)
        assert provenance.commit is None and provenance.note

    def test_an_oversized_ref_file_is_refused_rather_than_read(self, repositories):
        assessed, _, _, _ = repositories
        (assessed / ".git" / "HEAD").write_text("ref: refs/heads/main\n" + "x" * 8192)
        provenance = capture(assessed)
        assert provenance.commit is None and "larger than a ref file" in provenance.note

    def test_a_head_that_is_a_directory_is_refused(self, repositories):
        assessed, _, _, _ = repositories
        (assessed / ".git" / "HEAD").unlink()
        (assessed / ".git" / "HEAD").mkdir()
        provenance = capture(assessed)
        assert provenance.commit is None and provenance.note

    def test_no_project_path_is_not_a_refusal(self):
        assert capture(None) == GitProvenance()

    def test_capture_never_raises(self, tmp_path):
        """An unreadable repository must not fail an assessment about other things."""
        missing = tmp_path / "does-not-exist"
        assert capture(missing) == GitProvenance()


# ---------------------------------------------------- the runner records it


class TestTheRunnerRecordsTheDecision:
    def test_the_target_carries_the_refusal_reason(self, repositories):
        from markna.models import Layer
        from markna.runner import RunConfig, Runner

        assessed, _, unrelated, unrelated_head = repositories
        shutil.rmtree(assessed / ".git")
        (assessed / ".git").symlink_to(unrelated / ".git")

        target = Runner(RunConfig(project_path=assessed, layers={Layer.CODE}))._build_target()

        assert target.git_commit is None
        assert target.git_commit != unrelated_head
        assert target.git_provenance_note and "symbolic link" in target.git_provenance_note
        assert target.to_dict()["git_provenance_note"] == target.git_provenance_note

    def test_a_clean_repository_records_no_note(self, repositories):
        from markna.models import Layer
        from markna.runner import RunConfig, Runner

        assessed, assessed_head, _, _ = repositories
        target = Runner(RunConfig(project_path=assessed, layers={Layer.CODE}))._build_target()

        assert target.git_commit == assessed_head
        assert target.git_branch == "main"
        assert target.git_provenance_note is None
