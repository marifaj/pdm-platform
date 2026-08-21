"""Filesystem confinement for the code layer.

The repository under assessment is hostile input. It is written by whoever the
gate is reviewing, it may be a fork, and it may contain a symlink pointing at
``/etc``, at another tenant's workspace, or at the worker's own database.

Three controls live here, and they are deliberately independent so that a gap in
one does not become a disclosure:

1. **Traversal** — MARKNA's own file walk never leaves the project root, and
   never follows a symlink that resolves outside it.
2. **Reading** — every read MARKNA performs on a scanner-reported path is
   confined to the root. A third-party scanner may still follow a symlink; what
   it cannot do is make MARKNA quote the contents back into a report.
3. **Reporting** — any finding whose location resolves outside the root has its
   evidence stripped and is replaced by an explicit escape finding, so the
   attempt is visible rather than silently rendered.

Confinement is by resolved path, not by string prefix: ``..`` segments,
symlinked parents and relative paths all resolve before the comparison.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple

from .models import Finding, Layer, Location, Severity

#: Never walked, whatever the repository says.
SKIP_DIRECTORIES = {
    ".git", ".hg", ".svn", "node_modules", "venv", ".venv", "env",
    "__pycache__", ".mypy_cache", ".pytest_cache", ".tox", "dist", "build",
    ".markna", "site-packages",
}

#: A single file MARKNA will read for evidence. Larger files are skipped rather
#: than truncated: evidence is a few lines, never a payload.
MAX_READ_BYTES = 2_000_000


def resolve_within(candidate: os.PathLike, root: os.PathLike) -> Optional[Path]:
    """Resolve ``candidate`` and return it only if it stays inside ``root``.

    Returns ``None`` when the path escapes, cannot be resolved, or the root is
    unusable. Callers treat ``None`` as "refuse", never as "carry on".
    """
    try:
        resolved_root = Path(root).resolve(strict=False)
        path = Path(candidate)
        resolved = (
            path.resolve(strict=False)
            if path.is_absolute()
            else (resolved_root / path).resolve(strict=False)
        )
    except (OSError, RuntimeError, ValueError):
        return None
    if resolved == resolved_root or resolved_root in resolved.parents:
        return resolved
    return None


def path_within(candidate: os.PathLike, root: os.PathLike) -> bool:
    return resolve_within(candidate, root) is not None


def safe_read_text(
    candidate: os.PathLike,
    root: os.PathLike,
    *,
    max_bytes: int = MAX_READ_BYTES,
) -> Optional[str]:
    """Read a file, but only a regular file that resolves inside ``root``."""
    resolved = resolve_within(candidate, root)
    if resolved is None:
        return None
    try:
        if not resolved.is_file() or resolved.stat().st_size > max_bytes:
            return None
        return resolved.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def iter_safe_files(
    root: os.PathLike,
    *,
    skip_directories: Iterable[str] = SKIP_DIRECTORIES,
    max_files: int = 200_000,
) -> Iterator[Path]:
    """Walk ``root`` without leaving it.

    ``os.walk`` with ``followlinks=False`` keeps directory symlinks from being
    descended into; the explicit per-file check then rejects file symlinks whose
    target resolves outside the root.
    """
    resolved_root = Path(root).resolve(strict=False)
    skip = set(skip_directories)
    seen = 0
    for directory, subdirectories, filenames in os.walk(resolved_root, followlinks=False):
        subdirectories[:] = [name for name in subdirectories if name not in skip]
        for filename in filenames:
            candidate = Path(directory) / filename
            if candidate.is_symlink() and resolve_within(candidate, resolved_root) is None:
                continue  # a symlink pointing out of the tree is not part of the tree
            if not path_within(candidate, resolved_root):
                continue
            seen += 1
            if seen > max_files:
                return
            yield candidate


def find_escaping_symlinks(
    root: os.PathLike, *, limit: int = 100
) -> List[Tuple[Path, Path]]:
    """Every symlink in the tree whose target resolves outside ``root``.

    Reported as a finding by the runner: a repository that tries to read outside
    its own directory is worth knowing about regardless of whether any scanner
    followed it.
    """
    resolved_root = Path(root).resolve(strict=False)
    escaping: List[Tuple[Path, Path]] = []
    for directory, subdirectories, filenames in os.walk(resolved_root, followlinks=False):
        subdirectories[:] = [name for name in subdirectories if name not in SKIP_DIRECTORIES]
        for name in list(subdirectories) + filenames:
            candidate = Path(directory) / name
            if not candidate.is_symlink():
                continue
            if resolve_within(candidate, resolved_root) is None:
                try:
                    destination = candidate.resolve(strict=False)
                except (OSError, RuntimeError):
                    destination = Path(os.readlink(candidate))
                escaping.append((candidate, destination))
                if len(escaping) >= limit:
                    return escaping
    return escaping


def confine_findings(
    findings: Sequence[Finding], root: Optional[os.PathLike], *, source: str = "markna:confinement"
) -> List[Finding]:
    """Strip evidence from findings that point outside ``root``.

    A third-party scanner following a symlink is not something MARKNA can
    prevent. Quoting the result into a report is, and this is where that stops:
    the location is kept (so the escape is diagnosable), the evidence is
    replaced, and one aggregate finding records what happened.
    """
    if root is None:
        return list(findings)

    resolved_root = Path(root).resolve(strict=False)
    confined: List[Finding] = []
    escaped: List[str] = []

    for finding in findings:
        file_path = finding.location.file
        if not file_path or path_within(file_path, resolved_root):
            confined.append(finding)
            continue
        escaped.append(f"{finding.source}: {file_path}")
        finding.evidence = (
            "[evidence withheld: this path resolves outside the project root, so MARKNA did "
            "not read it back. The scanner that reported it followed a link out of the "
            "repository.]"
        )
        finding.tags = list(finding.tags) + ["outside-project-root"]
        finding.confidence = "low"
        confined.append(finding)

    if escaped:
        confined.append(
            Finding(
                source=source,
                layer=Layer.CODE,
                severity=Severity.HIGH,
                title=f"Scanner results referenced {len(escaped)} path(s) outside the project root",
                explanation=(
                    "A scanner reported findings for files that resolve outside the assessed "
                    "project directory — normally a symlink in the repository pointing elsewhere "
                    "on the host. MARKNA withheld the contents of those files from this report. "
                    "Treat the repository as attempting to read outside its own tree until "
                    "proven otherwise."
                ),
                evidence="project root: {}\n{}".format(
                    resolved_root, "\n".join(f"- {entry}" for entry in escaped[:20])
                ),
                location=Location(component="project-root"),
                remediation=(
                    "Inspect the repository for symlinks leaving the tree, remove them, and "
                    "re-run. If the link is legitimate, vendor the content into the repository "
                    "instead of linking to it."
                ),
                rule_id="confinement/path-outside-project-root",
                cwe=["CWE-59", "CWE-22"],
                tags=["confinement", "symlink"],
            )
        )
    return confined


def escaping_symlink_finding(root: os.PathLike, escaping: Sequence[Tuple[Path, Path]]) -> Finding:
    """A finding describing symlinks that leave the project root."""
    resolved_root = Path(root).resolve(strict=False)
    listing = "\n".join(
        f"- {link.relative_to(resolved_root) if path_within(link.parent, resolved_root) else link}"
        f" -> {destination}"
        for link, destination in escaping[:20]
    )
    return Finding(
        source="markna:confinement",
        layer=Layer.CODE,
        severity=Severity.HIGH,
        title=f"Repository contains {len(escaping)} symlink(s) pointing outside the project",
        explanation=(
            "Symbolic links in the repository resolve outside the project directory. Anything "
            "that follows them — this gate's scanners, a build step, a container image build — "
            "reads files the repository does not own. On a shared build host that is a file "
            "disclosure primitive."
        ),
        evidence=f"project root: {resolved_root}\n{listing}",
        location=Location(component="project-root"),
        remediation=(
            "Remove the links or replace them with vendored copies. If a build genuinely needs "
            "external content, fetch it explicitly rather than linking to it from the source tree."
        ),
        rule_id="confinement/escaping-symlink",
        cwe=["CWE-59"],
        tags=["confinement", "symlink"],
    )
