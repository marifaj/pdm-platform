"""Server configuration.

Read from a YAML/JSON file, from environment variables, or both — file first,
environment last, so a container can override a baked-in file.

Two settings are security controls rather than preferences:

* ``workspace_root`` bounds every server-side path a project may reference. A
  project is administrator-configured data, but it names paths the worker will
  read, so it is treated as untrusted input and confined to one directory.
* ``auth_providers`` decides who can call the application at all. The
  ``trusted-header`` provider is never enabled by default, because a
  header-trusting provider reachable without its proxy is an auth bypass.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_DATABASE = "markna.db"
DEFAULT_WORKSPACE = "workspaces"
DEFAULT_REPORT_FORMATS = ("json", "markdown", "html", "sarif")


class ConfigError(ValueError):
    """Raised when the configuration cannot produce a runnable server."""


@dataclass
class ServerConfig:
    database_path: str = DEFAULT_DATABASE
    #: Every project path must resolve inside this directory.
    workspace_root: str = DEFAULT_WORKSPACE
    host: str = "127.0.0.1"
    port: int = 8080
    #: Authentication providers, tried in order. See markna_server.identity.
    auth_providers: List[str] = field(default_factory=lambda: ["session", "api-token"])
    auth_options: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    session_hours: int = 12
    #: Set false only for local HTTP development; it drops the Secure cookie flag.
    secure_cookies: bool = True
    #: Extra directories searched for scanner binaries (worker only).
    scanner_tool_path: List[str] = field(default_factory=list)
    report_formats: List[str] = field(default_factory=lambda: list(DEFAULT_REPORT_FORMATS))
    scanner_timeout_seconds: int = 900
    offline: bool = False
    ai_enabled: bool = False
    ai_model: Optional[str] = None
    worker_poll_seconds: float = 3.0
    worker_workdir: str = "work"
    #: Bytes of report content held in the database per format.
    max_report_bytes: int = 8 * 1024 * 1024

    # ------------------------------------------------------------------ load

    @classmethod
    def load(cls, path: Optional[str] = None, environ: Optional[Dict[str, str]] = None) -> "ServerConfig":
        environ = dict(os.environ if environ is None else environ)
        data: Dict[str, Any] = {}
        config_path = path or environ.get("MARKNA_CONFIG")
        if config_path:
            data = _read_structured(Path(config_path))
        config = cls.from_dict(data)
        config._apply_environment(environ)
        config.validate()
        return config

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ServerConfig":
        known = {field_name for field_name in cls.__dataclass_fields__}
        unknown = set(data) - known
        if unknown:
            raise ConfigError(f"unknown configuration key(s): {', '.join(sorted(unknown))}")
        return cls(**data)

    def _apply_environment(self, environ: Dict[str, str]) -> None:
        mapping = {
            "MARKNA_DATABASE": ("database_path", str),
            "MARKNA_WORKSPACE_ROOT": ("workspace_root", str),
            "MARKNA_HOST": ("host", str),
            "MARKNA_PORT": ("port", int),
            "MARKNA_SESSION_HOURS": ("session_hours", int),
            "MARKNA_SECURE_COOKIES": ("secure_cookies", _as_bool),
            "MARKNA_OFFLINE": ("offline", _as_bool),
            "MARKNA_AI_ENABLED": ("ai_enabled", _as_bool),
            "MARKNA_AI_MODEL": ("ai_model", str),
            "MARKNA_SCANNER_TIMEOUT": ("scanner_timeout_seconds", int),
            "MARKNA_WORKER_WORKDIR": ("worker_workdir", str),
        }
        for variable, (attribute, caster) in mapping.items():
            if variable in environ and environ[variable] != "":
                setattr(self, attribute, caster(environ[variable]))
        if environ.get("MARKNA_AUTH_PROVIDERS"):
            self.auth_providers = _split(environ["MARKNA_AUTH_PROVIDERS"])
        if environ.get("MARKNA_TOOL_PATH"):
            self.scanner_tool_path = [
                entry for entry in environ["MARKNA_TOOL_PATH"].split(os.pathsep) if entry
            ]
        if environ.get("MARKNA_REPORT_FORMATS"):
            self.report_formats = _split(environ["MARKNA_REPORT_FORMATS"])

    def validate(self) -> None:
        if not self.auth_providers:
            raise ConfigError(
                "auth_providers must not be empty: an application with no authentication "
                "provider would accept nobody, and one that defaulted to accepting everybody "
                "would be worse"
            )
        if "trusted-header" in self.auth_providers:
            options = self.auth_options.get("trusted-header", {})
            if not options.get("organization_id") and not options.get("issuer"):
                raise ConfigError(
                    "the trusted-header provider needs auth_options.trusted-header."
                    "organization_id (or issuer + subject_header). Enable it only when the "
                    "application is unreachable except through the authenticating proxy."
                )
        from markna.report import FORMATS

        unknown = set(self.report_formats) - set(FORMATS)
        if unknown:
            raise ConfigError(f"unknown report format(s): {', '.join(sorted(unknown))}")
        if not 1 <= self.port <= 65535:
            raise ConfigError(f"port {self.port} is out of range")

    # ------------------------------------------------------------- accessors

    @property
    def workspace_path(self) -> Path:
        return Path(self.workspace_root).resolve()

    def resolve_in_workspace(self, candidate: str) -> Path:
        """Resolve a project-supplied path, refusing anything outside the workspace.

        Project records are written by administrators, but they name paths the
        worker will open. Confining them to one directory keeps a mistyped or
        malicious value from turning the worker into a file-disclosure tool.
        """
        root = self.workspace_path
        path = Path(candidate)
        resolved = (root / path).resolve() if not path.is_absolute() else path.resolve()
        if resolved != root and root not in resolved.parents:
            raise ConfigError(
                f"path {candidate!r} resolves outside the workspace root {root}"
            )
        return resolved

    def to_dict(self) -> Dict[str, Any]:
        return {
            field_name: getattr(self, field_name) for field_name in self.__dataclass_fields__
        }


def _read_structured(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"configuration file not found: {path}")
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text)
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - PyYAML is a declared dependency
        raise ConfigError("PyYAML is required to read a YAML configuration file") from exc
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"configuration file {path} must contain a mapping")
    return data


def _as_bool(value: str) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _split(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]
