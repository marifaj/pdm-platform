"""Local password hashing.

PBKDF2-HMAC-SHA256 from the standard library. Deliberately small: local accounts
exist so a single-organisation deployment works out of the box, and are expected
to be replaced by a directory once one is wired in.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets

ITERATIONS = 240_000
_ALGORITHM = "pbkdf2_sha256"


def hash_password(password: str, *, iterations: int = ITERATIONS) -> str:
    if len(password) < 12:
        raise ValueError("password must be at least 12 characters")
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return "$".join(
        [
            _ALGORITHM,
            str(iterations),
            base64.b64encode(salt).decode("ascii"),
            base64.b64encode(digest).decode("ascii"),
        ]
    )


def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, iterations, salt_b64, digest_b64 = encoded.split("$")
        if algorithm != _ALGORITHM:
            return False
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(digest_b64)
    except (ValueError, TypeError):
        return False
    candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(iterations))
    return hmac.compare_digest(candidate, expected)


def generate_api_token() -> str:
    """A bearer token. Shown once, stored only as a hash."""
    return f"mkna_{secrets.token_urlsafe(32)}"


def hash_api_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
