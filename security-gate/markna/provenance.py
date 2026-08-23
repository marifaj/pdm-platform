"""Repository provenance for the code layer.

The report says which commit was assessed. That line is evidence: it is what a
release record points at months later when someone asks what was reviewed. It
is therefore worth attacking, and the repository under assessment is the thing
attacking it.

MARKNA used to answer the question by running ``git rev-parse`` inside the
assessed directory. That is unsafe for three separate reasons, all of which
were reproduced before this module was written:

* ``.git`` may be a symlink, or a ``gitdir:`` pointer file, naming a repository
  somewhere else entirely -- so the recorded commit belongs to a repository
  nobody assessed;
* git discovery walks *upwards*, so a project directory that is not itself a
  repository silently reports its parent's history, which may be outside the
  workspace;
* two invocations (``HEAD`` and ``--abbrev-ref HEAD``) read ``.git`` twice, so
  a replacement between them stitches a commit from one repository to a branch
  name from another.

The fix is not a better-guarded subprocess. It is to stop running a program
whose job is to search for a repository, and instead read the two files that
answer the question -- ``HEAD`` and the ref it names -- through a descriptor
opened once on the assessed directory's own ``.git``.

That gives three properties:

1. **No discovery.** Provenance comes from ``<project>/.git`` or from nowhere.
   Nothing walks upward, and no pointer is followed out of the tree.
2. **A concurrent replacement cannot substitute a history.** Every read is
   relative to a held directory descriptor, so replacing, deleting or
   re-pointing the ``.git`` *name* after the descriptor is open cannot redirect
   a read into the repository that replaced it; the descriptor pins the inode,
   so it cannot be recycled underneath us either. Stated precisely, because the
   difference matters: a mid-capture replacement can make provenance come out
   **empty**, and cannot make it come out **wrong**. Emptiness carries a stated
   reason (property 3); a stitched commit-and-branch from two repositories --
   the shape the two-subprocess implementation produced -- is not reachable at
   all, because commit and branch both come from a single ``HEAD`` read.
3. **No third-party parser.** No git process runs against hostile input, so
   repository-controlled configuration (``include.path``, ``core.fsmonitor``,
   alternates, hooks) has no bearing on the answer.

What this deliberately does *not* support is a repository whose history lives
elsewhere: worktrees, submodule gitdir pointers, ``commondir`` delegation. Those
are refused with a stated reason rather than resolved, and the reason is carried
into the report. An absent result is not a clean result: "provenance refused"
must never render as "no repository".
"""

from __future__ import annotations

import errno
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

#: The only place provenance is read from.
GITDIR_NAME = ".git"

#: ``HEAD`` and a loose ref are one short line each. Anything larger is not one
#: of those files, and is refused rather than parsed.
MAX_REF_BYTES = 4096

#: ``packed-refs`` is a real file that can legitimately be large on a busy
#: repository, but not unbounded: this is a read into the gate's own memory.
MAX_PACKED_REFS_BYTES = 8_000_000

#: Object names, SHA-1 and SHA-256 repositories alike. Lowercase hex only --
#: anything else is not a commit id and must not reach a report.
_OBJECT_NAME = re.compile(r"\A(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")

_SYMREF_PREFIX = "ref: "

#: A ``.git`` containing this delegates its refs to another repository, so the
#: history it names is not this directory's.
COMMONDIR_MARKER = "commondir"


class ProvenanceRefused(Exception):
    """A repository is present but its history cannot be trusted as this one's."""


@dataclass(frozen=True)
class GitProvenance:
    """What the gate is willing to say about the assessed repository."""

    commit: Optional[str] = None
    branch: Optional[str] = None
    #: Present when a repository was found but nothing (or not everything) could
    #: be recorded. Rendered in the report so the gap is visible.
    note: Optional[str] = None
    #: True when a ``.git`` entry existed at all, whatever came of it.
    present: bool = False

    @property
    def established(self) -> bool:
        return self.commit is not None


def capture(project_path: Optional[os.PathLike]) -> GitProvenance:
    """Read the assessed directory's own git provenance, or refuse.

    Never raises: a repository that cannot be read is a reporting fact, not a
    reason to fail an assessment that has nothing to do with git.
    """
    if project_path is None:
        return GitProvenance()
    root = Path(project_path)
    gitdir = root / GITDIR_NAME

    try:
        entry = os.lstat(gitdir)
    except OSError:
        # No repository here. Note that this is *not* "look in the parent":
        # a directory that is not a repository has no provenance to report.
        return GitProvenance()

    try:
        return _capture(root, gitdir, entry)
    except ProvenanceRefused as exc:
        return GitProvenance(note=str(exc), present=True)
    except OSError as exc:
        return GitProvenance(
            note=f"the project's .git could not be read: {exc.strerror or exc}", present=True
        )


def _capture(root: Path, gitdir: Path, entry: os.stat_result) -> GitProvenance:
    if stat.S_ISLNK(entry.st_mode):
        raise ProvenanceRefused(
            "the project's .git is a symbolic link, so the history it names is not this "
            "directory's own; provenance was not recorded"
        )
    if not stat.S_ISDIR(entry.st_mode):
        raise ProvenanceRefused(
            "the project's .git is a file (a gitdir pointer), which names history stored "
            "outside the assessed directory; provenance was not recorded"
        )

    root_device = os.stat(root).st_dev
    fd = _open_directory(gitdir, follow=False)
    try:
        if os.fstat(fd).st_dev != root_device:
            raise ProvenanceRefused(
                "the project's .git is on a different filesystem from the project itself, "
                "so it is not part of the assessed tree; provenance was not recorded"
            )
        if _entry_exists(fd, COMMONDIR_MARKER):
            raise ProvenanceRefused(
                "the project's .git delegates its refs to another repository (commondir), "
                "so its history is not self-contained; provenance was not recorded"
            )
        return _read_head(fd, root_device)
    finally:
        os.close(fd)


# ------------------------------------------------------------------ HEAD / refs


def _read_head(fd: int, device: int) -> GitProvenance:
    text = _read_small(fd, ["HEAD"], device, MAX_REF_BYTES).strip()
    if not text:
        raise ProvenanceRefused(
            "the project's .git/HEAD is empty; provenance was not recorded"
        )

    if _OBJECT_NAME.match(text):
        # Detached HEAD: a commit with no branch. Real, and worth recording.
        return GitProvenance(
            commit=text,
            branch=None,
            note="HEAD is detached, so no branch name was recorded",
            present=True,
        )

    if not text.startswith(_SYMREF_PREFIX):
        raise ProvenanceRefused(
            "the project's .git/HEAD is neither an object name nor a symbolic ref; "
            "provenance was not recorded"
        )

    ref = text[len(_SYMREF_PREFIX) :].strip()
    _validate_ref(ref)
    branch = ref[len("refs/heads/") :] if ref.startswith("refs/heads/") else None

    commit = _resolve_ref(fd, ref, device)
    if commit is None:
        return GitProvenance(
            commit=None,
            branch=branch,
            note=(
                f"no commit was recorded for {ref}: the ref exists neither loose nor packed "
                "(an unborn branch, or the repository changed while it was being read)"
            ),
            present=True,
        )
    return GitProvenance(commit=commit, branch=branch, present=True)


def _validate_ref(ref: str) -> None:
    """Refuse anything that is not a plain ref path under ``refs/``.

    ``HEAD`` is repository-controlled, so it is a path-traversal primitive
    unless it is constrained here: ``ref: ../../../../etc/passwd`` must not
    become a file read.
    """
    if not ref.startswith("refs/"):
        raise ProvenanceRefused(
            f"the project's .git/HEAD points outside refs/ ({ref!r}); "
            "provenance was not recorded"
        )
    components = ref.split("/")
    if any(part in ("", ".", "..") for part in components):
        raise ProvenanceRefused(
            f"the project's .git/HEAD names an unusable ref ({ref!r}); "
            "provenance was not recorded"
        )
    if any(character in ref for character in ("\\", "\0", ":")) or ref.endswith(".lock"):
        raise ProvenanceRefused(
            f"the project's .git/HEAD names an unusable ref ({ref!r}); "
            "provenance was not recorded"
        )


def _resolve_ref(fd: int, ref: str, device: int) -> Optional[str]:
    """The object a ref names: loose file first, then ``packed-refs``."""
    try:
        text = _read_small(fd, ref.split("/"), device, MAX_REF_BYTES).strip()
    except FileNotFoundError:
        text = ""   # no loose ref; it may still be packed
    if text:
        return _object_name(text, ref)

    for line in _packed_refs(fd, device):
        if line.startswith(("#", "^")):
            continue  # header, or the peeled tag object of the line above it
        candidate, separator, target = line.partition(" ")
        if not separator or target.strip() != ref:
            continue
        return _object_name(candidate, ref)
    return None


def _object_name(text: str, ref: str) -> str:
    candidate = text.strip().split()[0] if text.strip() else ""
    if not _OBJECT_NAME.match(candidate):
        raise ProvenanceRefused(
            f"the project's {ref} does not contain an object name; "
            "provenance was not recorded"
        )
    return candidate


def _packed_refs(fd: int, device: int) -> List[str]:
    try:
        blob = _read_small(fd, ["packed-refs"], device, MAX_PACKED_REFS_BYTES)
    except FileNotFoundError:
        return []
    return blob.splitlines()


# ------------------------------------------------------------------- descriptors


def _open_directory(path, *, follow: bool, dir_fd: Optional[int] = None) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
    if not follow:
        flags |= os.O_NOFOLLOW
    return os.open(path, flags, dir_fd=dir_fd)


def _entry_exists(fd: int, name: str) -> bool:
    try:
        os.lstat(name, dir_fd=fd)
    except OSError:
        return False
    return True


def _lstat_at(name: str, fd: int) -> os.stat_result:
    """``lstat`` one component relative to ``fd``. Raises ``FileNotFoundError``."""
    return os.lstat(name, dir_fd=fd)


def _read_small(fd: int, components: Sequence[str], device: int, limit: int) -> str:
    """Read ``components`` relative to ``fd``, following nothing and leaving nothing.

    Each component is inspected with ``lstat`` and then opened through the
    previous component's descriptor, and the opened inode is compared with the
    inspected one: a symlink is refused rather than followed, a component
    swapped between the two operations is refused rather than read, and every
    component must be on the same filesystem as the project so a mount cannot be
    substituted for a directory mid-path.

    Raises ``FileNotFoundError`` when an entry simply is not there -- callers
    distinguish "no such ref" from "refused" -- and :class:`ProvenanceRefused`
    for everything a repository can do to make a path untrustworthy.
    """
    if not components:
        raise ProvenanceRefused("internal: empty path")

    display = "/".join(components)
    current = fd
    opened: List[int] = []
    try:
        for name in components[:-1]:
            info = _lstat_at(name, current)
            if stat.S_ISLNK(info.st_mode):
                raise ProvenanceRefused(
                    f"a symbolic link stands in for {name!r} inside the project's .git, so "
                    f"{display} is not this repository's; provenance was not recorded"
                )
            if not stat.S_ISDIR(info.st_mode):
                raise ProvenanceRefused(
                    f"{name!r} inside the project's .git is not a directory; "
                    "provenance was not recorded"
                )
            try:
                current = _open_directory(name, follow=False, dir_fd=current)
            except OSError as exc:
                if exc.errno == errno.ENOENT:
                    raise FileNotFoundError(exc.errno, exc.strerror, name) from exc
                raise ProvenanceRefused(
                    f"{name!r} inside the project's .git could not be opened without "
                    f"following a link ({exc.strerror or exc}); provenance was not recorded"
                ) from exc
            opened.append(current)
            _require_same_entry(os.fstat(current), info, device, name)

        leaf = components[-1]
        info = _lstat_at(leaf, current)
        if stat.S_ISLNK(info.st_mode):
            raise ProvenanceRefused(
                f"the project's .git/{display} is a symbolic link, so it does not hold this "
                "repository's own state; provenance was not recorded"
            )
        if not stat.S_ISREG(info.st_mode):
            raise ProvenanceRefused(
                f"the project's .git/{display} is not a regular file; "
                "provenance was not recorded"
            )
        if info.st_size > limit:
            raise ProvenanceRefused(
                f"the project's .git/{display} is larger than a ref file can be "
                f"({info.st_size} bytes); provenance was not recorded"
            )
        try:
            handle = os.open(
                leaf,
                os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
                dir_fd=current,
            )
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                raise FileNotFoundError(exc.errno, exc.strerror, leaf) from exc
            raise ProvenanceRefused(
                f"the project's .git/{display} could not be opened without following a link "
                f"({exc.strerror or exc}); provenance was not recorded"
            ) from exc
        try:
            _require_same_entry(os.fstat(handle), info, device, display)
            with os.fdopen(os.dup(handle), "rb") as stream:
                blob = stream.read(limit + 1)
        finally:
            os.close(handle)
        if len(blob) > limit:
            raise ProvenanceRefused(
                f"the project's .git/{display} is larger than a ref file can be; "
                "provenance was not recorded"
            )
        return blob.decode("utf-8", errors="replace")
    finally:
        for descriptor in opened:
            os.close(descriptor)


def _require_same_entry(
    opened: os.stat_result, inspected: os.stat_result, device: int, name: str
) -> None:
    """The thing we opened must be the thing we inspected, on the right filesystem.

    Closes the gap between ``lstat`` and ``open``: without this, a component
    replaced in between is inspected as a directory and opened as something
    else.
    """
    if (opened.st_dev, opened.st_ino) != (inspected.st_dev, inspected.st_ino):
        raise ProvenanceRefused(
            f"{name!r} inside the project's .git changed while it was being read; "
            "provenance was not recorded"
        )
    if opened.st_dev != device:
        raise ProvenanceRefused(
            f"{name!r} inside the project's .git is on another filesystem; "
            "provenance was not recorded"
        )
