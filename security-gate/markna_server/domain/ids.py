"""Identifier generation.

Every entity gets a prefixed, time-sortable identifier: ``prj_01a2b3c4d5e6f7``.
The prefix makes an id self-describing in logs, URLs and error messages, and
makes it obvious when the wrong kind of id has been passed somewhere.

Ids are opaque to callers. Nothing may parse them for meaning beyond the prefix.
"""

from __future__ import annotations

import re
import secrets
import time

ORGANIZATION = "org"
PROJECT = "prj"
TARGET = "tgt"
POLICY = "pol"
RUN = "run"
FINDING = "fnd"
REPORT = "rpt"
USER = "usr"
TOKEN = "tok"
AUDIT = "aud"

_ID_PATTERN = re.compile(r"^[a-z]{3}_[0-9a-f]{18}$")


def new_id(prefix: str) -> str:
    """A time-ordered identifier, so natural id order matches creation order."""
    if len(prefix) != 3 or not prefix.isalpha():
        raise ValueError(f"id prefix must be three letters, got {prefix!r}")
    milliseconds = int(time.time() * 1000)
    return f"{prefix}_{milliseconds:012x}{secrets.token_hex(3)}"


def is_id(value: object, prefix: str = "") -> bool:
    if not isinstance(value, str) or not _ID_PATTERN.match(value):
        return False
    return value.startswith(f"{prefix}_") if prefix else True


def require_id(value: str, prefix: str, field: str) -> str:
    if not is_id(value, prefix):
        raise ValueError(f"{field} must be a {prefix}_ identifier, got {value!r}")
    return value
