"""Principals, roles and permissions.

Authentication answers *who is calling*; authorisation answers *what they may
do*. Both are deliberately expressed in terms this application defines, not in
terms any particular identity provider defines — so swapping the provider does
not ripple into the services.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Optional, Set

from ..domain import TenantScope, ids


class Permission(str, enum.Enum):
    ORG_READ = "org:read"
    ORG_ADMIN = "org:admin"
    PROJECT_READ = "project:read"
    PROJECT_WRITE = "project:write"
    POLICY_READ = "policy:read"
    POLICY_WRITE = "policy:write"
    RUN_READ = "run:read"
    RUN_CREATE = "run:create"
    RUN_CANCEL = "run:cancel"
    REPORT_READ = "report:read"
    TOKEN_MANAGE = "token:manage"


class Role(str, enum.Enum):
    """Three roles is enough for v1.0 and maps onto directory groups later."""

    VIEWER = "viewer"
    MAINTAINER = "maintainer"
    ADMIN = "admin"

    @property
    def permissions(self) -> FrozenSet[Permission]:
        return _ROLE_PERMISSIONS[self]


_VIEWER: FrozenSet[Permission] = frozenset(
    {
        Permission.ORG_READ,
        Permission.PROJECT_READ,
        Permission.POLICY_READ,
        Permission.RUN_READ,
        Permission.REPORT_READ,
    }
)

_MAINTAINER: FrozenSet[Permission] = _VIEWER | frozenset(
    {
        Permission.PROJECT_WRITE,
        Permission.POLICY_WRITE,
        Permission.RUN_CREATE,
        Permission.RUN_CANCEL,
    }
)

_ADMIN: FrozenSet[Permission] = _MAINTAINER | frozenset(
    {Permission.ORG_ADMIN, Permission.TOKEN_MANAGE}
)

_ROLE_PERMISSIONS: Dict[Role, FrozenSet[Permission]] = {
    Role.VIEWER: _VIEWER,
    Role.MAINTAINER: _MAINTAINER,
    Role.ADMIN: _ADMIN,
}


@dataclass
class User:
    """A person in one organisation.

    ``subject`` is the external identity-provider subject claim when one is in
    use, and ``None`` for a locally-managed account. Keeping it separate from
    ``id`` is what allows an account to be re-bound to Entra ID later without
    rewriting every foreign key that points at the user.
    """

    id: str = field(default_factory=lambda: ids.new_id(ids.USER))
    organization_id: str = ""
    email: str = ""
    display_name: str = ""
    roles: Set[Role] = field(default_factory=lambda: {Role.VIEWER})
    subject: Optional[str] = None
    issuer: Optional[str] = None
    password_hash: Optional[str] = None
    is_active: bool = True
    created_at: str = ""
    last_login_at: Optional[str] = None

    def to_principal(self, auth_method: str) -> "Principal":
        return Principal(
            id=self.id,
            organization_id=self.organization_id,
            display_name=self.display_name or self.email,
            email=self.email,
            roles=frozenset(self.roles),
            auth_method=auth_method,
            subject=self.subject,
        )


@dataclass(frozen=True)
class Principal:
    """An authenticated caller, resolved into this application's own terms."""

    id: str
    organization_id: str
    display_name: str
    roles: FrozenSet[Role]
    email: str = ""
    auth_method: str = "unknown"
    subject: Optional[str] = None
    #: True for machine callers (API tokens, the worker), used only for display
    #: and audit; it grants nothing on its own.
    is_service: bool = False

    @property
    def scope(self) -> TenantScope:
        return TenantScope(self.organization_id)

    @property
    def permissions(self) -> FrozenSet[Permission]:
        granted: Set[Permission] = set()
        for role in self.roles:
            granted |= role.permissions
        return frozenset(granted)

    def has(self, permission: Permission) -> bool:
        return permission in self.permissions

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "organization_id": self.organization_id,
            "display_name": self.display_name,
            "email": self.email,
            "roles": sorted(role.value for role in self.roles),
            "auth_method": self.auth_method,
            "is_service": self.is_service,
        }
