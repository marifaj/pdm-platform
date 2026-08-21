"""Domain-level errors.

Each maps to exactly one HTTP status at the edge, so no handler has to guess.
"""

from __future__ import annotations

from typing import Optional


class DomainError(Exception):
    """Base class. ``status`` is the HTTP status the API surface should return."""

    status = 400
    code = "domain_error"

    def __init__(self, message: str, *, detail: Optional[dict] = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}


class ValidationError(DomainError):
    status = 422
    code = "validation_error"


class NotFound(DomainError):
    status = 404
    code = "not_found"

    def __init__(self, kind: str, identifier: str) -> None:
        super().__init__(f"{kind} {identifier} was not found", detail={"kind": kind, "id": identifier})


class Conflict(DomainError):
    status = 409
    code = "conflict"


class Unauthenticated(DomainError):
    status = 401
    code = "unauthenticated"


class PermissionDenied(DomainError):
    status = 403
    code = "permission_denied"


class TenantIsolationError(DomainError):
    """Raised when a lookup would cross an organisation boundary.

    This is deliberately distinct from :class:`NotFound`: internally it must be
    loud, because it means a caller tried to reach another tenant's data. The
    API surface still renders it as 404 so it cannot be used to probe for the
    existence of records in another organisation.
    """

    status = 404
    code = "not_found"
