"""Evidence redaction.

Security reports get emailed, attached to tickets and pasted into chat. A secrets
scanner that prints the secret it found has moved the secret somewhere new, so
every piece of evidence passes through :func:`redact` before it reaches a report.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Pattern

_PATTERNS: List[Pattern[str]] = [
    # Common provider key shapes, redacted whole.
    re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,}\b"),
    re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9_\-]{16,}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_\-]{20,}\b"),
    re.compile(r"\beyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\b"),
    re.compile(r"-----BEGIN[ A-Z]*PRIVATE KEY-----[\s\S]*?-----END[ A-Z]*PRIVATE KEY-----"),
    # key = value assignments where the key name implies a secret. The leading
    # [\w.-]* matters: DATABASE_PASSWORD has no word boundary before "PASSWORD",
    # so a bare \b would miss the most common spelling in a .env file.
    re.compile(
        r"(?i)(\b[\w.\-]*(?:password|passwd|pwd|secret|token|api[_-]?key|apikey"
        r"|access[_-]?key|private[_-]?key|client[_-]?secret|auth[_-]?token|credential)"
        r"s?[\w]*\s*[:=]\s*)(['\"]?)([^\s'\"&,;]{4,})\2"
    ),
    # Credentials embedded in URLs.
    re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://[^\s:/@]+):([^\s@/]{2,})@"),
]

_REDACTED = "[REDACTED]"


def redact(text: str, extra_values: Iterable[str] = ()) -> str:
    """Replace anything that looks like a credential with ``[REDACTED]``.

    ``extra_values`` lets a caller redact known-sensitive literals (for example a
    session cookie value observed during an environment probe).
    """
    if not text:
        return ""
    redacted = text
    for value in extra_values:
        if value and len(str(value)) >= 4:
            redacted = redacted.replace(str(value), _REDACTED)

    redacted = _PATTERNS[-1].sub(lambda m: f"{m.group(1)}:{_REDACTED}@", redacted)
    redacted = _PATTERNS[-2].sub(
        lambda m: f"{m.group(1)}{m.group(2)}{_REDACTED}{m.group(2)}", redacted
    )
    for pattern in _PATTERNS[:-2]:
        redacted = pattern.sub(_REDACTED, redacted)
    return redacted


def truncate(text: str, limit: int = 4000) -> str:
    """Bound evidence size so one noisy finding cannot swamp a report."""
    if text is None:
        return ""
    text = str(text)
    if len(text) <= limit:
        return text
    return f"{text[:limit]}\n... [truncated, {len(text) - limit} more characters]"


def clean_evidence(text: str, *, limit: int = 4000, extra_values: Iterable[str] = ()) -> str:
    """Redact then truncate. The only function scanners should need."""
    return truncate(redact(text or "", extra_values=extra_values), limit=limit)
