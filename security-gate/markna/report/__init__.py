"""Report renderers.

Four formats, one assessment object:

* ``json``     — the complete record, and the only lossless format.
* ``markdown`` — the human review document.
* ``sarif``    — for code-scanning dashboards and CI annotations.
* ``html``     — a self-contained page to attach to a release record.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from ..models import Assessment
from .html import render_html
from .json_report import render_json
from .markdown import render_markdown
from .sarif import render_sarif

FORMATS = ("json", "markdown", "sarif", "html")

_EXTENSIONS = {"json": ".json", "markdown": ".md", "sarif": ".sarif", "html": ".html"}


def write_reports(
    assessment: Assessment,
    output_dir: Path,
    formats: List[str],
    *,
    include_raw: bool = False,
    basename: str = "markna-report",
) -> Dict[str, Path]:
    """Render each requested format into ``output_dir``. Returns format -> path."""
    unknown = set(formats) - set(FORMATS)
    if unknown:
        raise ValueError(
            f"unknown report format(s): {', '.join(sorted(unknown))}. "
            f"Available: {', '.join(FORMATS)}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    written: Dict[str, Path] = {}
    for fmt in formats:
        path = output_dir / f"{basename}{_EXTENSIONS[fmt]}"
        if fmt == "json":
            content = render_json(assessment, include_raw=include_raw)
        elif fmt == "markdown":
            content = render_markdown(assessment)
        elif fmt == "sarif":
            content = render_sarif(assessment)
        else:
            content = render_html(assessment)
        path.write_text(content, encoding="utf-8")
        written[fmt] = path
    return written


__all__ = [
    "FORMATS",
    "render_html",
    "render_json",
    "render_markdown",
    "render_sarif",
    "write_reports",
]
