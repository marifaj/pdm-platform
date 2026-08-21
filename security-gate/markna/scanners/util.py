"""Helpers shared by scanner adapters."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..confinement import path_within, safe_read_text

#: Directories that are never worth scanning and that skew results badly.
DEFAULT_EXCLUDES = (
    ".git",
    ".hg",
    ".svn",
    "node_modules",
    "venv",
    ".venv",
    "env",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".tox",
    "dist",
    "build",
    ".markna",
    "site-packages",
)


def load_json(text: str) -> Any:
    """Parse JSON, tolerating tools that print a banner before their output."""
    text = (text or "").strip()
    if not text:
        raise ValueError("no output to parse")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    for opener, closer in (("{", "}"), ("[", "]")):
        start = text.find(opener)
        end = text.rfind(closer)
        if start != -1 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                continue
    raise ValueError(f"could not parse tool output as JSON: {text[:300]}")


def load_json_file(path: Path) -> Any:
    return load_json(path.read_text(encoding="utf-8", errors="replace"))


def relative_path(path: Optional[str], root: Optional[Path]) -> Optional[str]:
    """Express a scanner-reported path relative to the project root."""
    if not path:
        return None
    candidate = str(path)
    if root is None:
        return candidate
    try:
        resolved = Path(candidate)
        if not resolved.is_absolute():
            resolved = (root / candidate).resolve()
        else:
            resolved = resolved.resolve()
        return os.path.relpath(resolved, root.resolve())
    except (OSError, ValueError):
        return candidate


def find_files(
    root: Path,
    patterns: Iterable[str],
    *,
    excludes: Iterable[str] = DEFAULT_EXCLUDES,
    limit: int = 200,
) -> List[Path]:
    """Locate files by glob pattern, skipping vendored and cache directories.

    A match that resolves outside ``root`` is discarded: a repository can name a
    file ``requirements.txt`` and point it at something the worker owns, and an
    adapter that then reads or parses it would be reading outside the workspace.
    """
    excluded = set(excludes)
    found: List[Path] = []
    for pattern in patterns:
        for path in sorted(root.rglob(pattern)):
            if any(part in excluded for part in path.parts):
                continue
            if not path_within(path, root):
                continue
            if path.is_file():
                found.append(path)
            if len(found) >= limit:
                return found
    return found


def read_snippet(
    path: Optional[Path],
    start_line: Optional[int],
    end_line: Optional[int] = None,
    *,
    max_lines: int = 8,
    root: Optional[Path] = None,
) -> str:
    """Read the source lines a finding points at, for use as evidence.

    Several scanners either omit the offending source or (newer Semgrep) return a
    placeholder, so MARKNA reads it back from disk itself.

    ``root`` confines that read. A scanner may follow a symlink out of the
    repository; this function will not quote what it finds there. Callers that
    have a project root must pass it — omitting it is only correct for paths
    MARKNA itself constructed.
    """
    if not path or not start_line:
        return ""
    if root is not None:
        text = safe_read_text(path, root)
        if text is None:
            return ""
        lines = text.splitlines()
    else:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return ""
    start = max(1, int(start_line))
    end = min(len(lines), max(start, int(end_line or start_line)))
    end = min(end, start + max_lines - 1)
    if start > len(lines):
        return ""
    width = len(str(end))
    return "\n".join(
        f"{number:>{width}}| {lines[number - 1]}" for number in range(start, end + 1)
    )


def first_line(text: Optional[str], limit: int = 300) -> str:
    if not text:
        return ""
    return text.strip().splitlines()[0][:limit] if text.strip() else ""


def as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def cvss_score(entries: Iterable[Dict[str, Any]]) -> Optional[float]:
    """Extract a numeric CVSS base score from an OSV-style severity array."""
    for entry in entries or []:
        raw = entry.get("score") if isinstance(entry, dict) else None
        if raw is None:
            continue
        text = str(raw)
        if text.startswith("CVSS:"):
            score = _score_from_vector(text)
            if score is not None:
                return score
            continue
        try:
            return float(text)
        except ValueError:
            continue
    return None


#: CVSS v3 base-score computation, used when a tool reports a vector but no score.
_AV = {"N": 0.85, "A": 0.62, "L": 0.55, "P": 0.2}
_AC = {"L": 0.77, "H": 0.44}
_PR_U = {"N": 0.85, "L": 0.62, "H": 0.27}
_PR_C = {"N": 0.85, "L": 0.68, "H": 0.5}
_UI = {"N": 0.85, "R": 0.62}
_CIA = {"H": 0.56, "L": 0.22, "N": 0.0}


def _score_from_vector(vector: str) -> Optional[float]:
    parts = dict(
        item.split(":", 1) for item in vector.split("/")[1:] if ":" in item
    )
    try:
        scope_changed = parts["S"] == "C"
        privileges = (_PR_C if scope_changed else _PR_U)[parts["PR"]]
        exploitability = (
            8.22 * _AV[parts["AV"]] * _AC[parts["AC"]] * privileges * _UI[parts["UI"]]
        )
        impact_base = 1 - (
            (1 - _CIA[parts["C"]]) * (1 - _CIA[parts["I"]]) * (1 - _CIA[parts["A"]])
        )
    except KeyError:
        return None
    if impact_base <= 0:
        return 0.0
    if scope_changed:
        impact = 7.52 * (impact_base - 0.029) - 3.25 * (impact_base - 0.02) ** 15
        raw = min(1.08 * (impact + exploitability), 10.0)
    else:
        impact = 6.42 * impact_base
        raw = min(impact + exploitability, 10.0)
    # CVSS rounds up to one decimal place.
    return round(raw + 0.049999, 1)
