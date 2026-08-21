"""Authorisation.

One object decides every access question, so there is a single place to audit
and a single place to change when roles come from a directory instead of a
column.

Two checks, always in this order:

1. **Tenant** — does the caller belong to the organisation that owns the record?
   A failure here is :class:`TenantIsolationError`, which the edge renders as
   404 so it cannot be used to probe another organisation's data.
2. **Permission** — does the caller's role grant the action?
"""

from __future__ import annotations

from typing import Any, Optional

from ..domain import PermissionDenied, TenantIsolationError, TenantScope
from .principal import Permission, Principal


class AccessControl:
    """Stateless policy object; safe to share across requests."""

    def scope_for(self, principal: Principal, organization_id: Optional[str] = None) -> TenantScope:
        """The tenant scope a request may operate in.

        Passing an explicit ``organization_id`` is how a caller says which
        tenant it *intends* to act on; a mismatch is an isolation failure rather
        than a silent reinterpretation.
        """
        if organization_id and organization_id != principal.organization_id:
            raise TenantIsolationError(
                f"principal {principal.id} may not act in organization {organization_id}"
            )
        return principal.scope

    def require(
        self,
        principal: Principal,
        permission: Permission,
        *,
        organization_id: Optional[str] = None,
    ) -> TenantScope:
        scope = self.scope_for(principal, organization_id)
        if not principal.has(permission):
            raise PermissionDenied(
                f"{permission.value} is not granted to roles "
                f"{', '.join(sorted(role.value for role in principal.roles)) or 'none'}",
                detail={"permission": permission.value},
            )
        return scope

    def require_owns(self, principal: Principal, entity: Any) -> None:
        """Assert an already-loaded record belongs to the caller's organisation.

        Repositories are scoped, so this is defence in depth — it catches an
        object that arrived from somewhere unscoped, such as a cache or a job
        payload.
        """
        owner = getattr(entity, "organization_id", None)
        if owner != principal.organization_id:
            raise TenantIsolationError(
                f"{type(entity).__name__} belongs to organization {owner}, "
                f"not {principal.organization_id}"
            )

    def can(self, principal: Principal, permission: Permission) -> bool:
        return principal.has(permission)
