"""Credential records: API tokens and browser sessions.

Both are stored server-side and revocable. Tokens are kept only as a SHA-256
hash; sessions are opaque identifiers with no data in the cookie, so a leaked
cookie cannot be reused after the session row is deleted.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from markna.models import utc_now

from ..domain import ids

DEFAULT_SESSION_HOURS = 12


@dataclass
class ApiToken:
    id: str = field(default_factory=lambda: ids.new_id(ids.TOKEN))
    organization_id: str = ""
    user_id: str = ""
    name: str = ""
    token_hash: str = ""
    created_at: str = field(default_factory=utc_now)
    expires_at: Optional[str] = None
    last_used_at: Optional[str] = None
    revoked_at: Optional[str] = None

    @property
    def is_usable(self) -> bool:
        return self.revoked_at is None and not is_expired(self.expires_at)


@dataclass
class Session:
    id: str = ""
    organization_id: str = ""
    user_id: str = ""
    created_at: str = field(default_factory=utc_now)
    expires_at: str = ""
    user_agent: str = ""

    @property
    def is_usable(self) -> bool:
        return not is_expired(self.expires_at)


def expiry_from_now(hours: int = DEFAULT_SESSION_HOURS) -> str:
    moment = datetime.now(timezone.utc) + timedelta(hours=hours)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def is_expired(timestamp: Optional[str], now: Optional[datetime] = None) -> bool:
    if not timestamp:
        return False
    try:
        moment = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return True  # an unparseable expiry is treated as expired, never as valid
    return moment <= (now or datetime.now(timezone.utc))
