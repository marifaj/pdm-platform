"""JSON report — the canonical, machine-readable record of an assessment."""

from __future__ import annotations

import json

from ..models import Assessment


def render_json(assessment: Assessment, *, include_raw: bool = False) -> str:
    """Serialise the full assessment.

    ``include_raw`` embeds each scanner's original record. It is off by default
    because raw tool output can contain unredacted matches.
    """
    return json.dumps(
        assessment.to_dict(include_raw=include_raw),
        indent=2,
        sort_keys=False,
        ensure_ascii=False,
        default=str,
    )
