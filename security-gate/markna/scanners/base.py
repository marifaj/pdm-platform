"""Scanner interface and registry.

A scanner is a thin adapter: it decides whether it can run, runs a mature tool
(or a deterministic check), and normalises the output into :class:`Finding`
objects. It never decides whether a finding blocks a release — that is policy.

Every scanner declares ``capabilities``. The policy engine uses those declared
capabilities to prove that the required classes of evidence were actually
gathered, so a capability name is a contract, not a label.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Type

from ..authorization import Authorization, Scope
from ..exec import ToolPath
from ..http import HttpClient
from ..models import Finding, Layer

#: Capability vocabulary. Policies reference these names, so they are stable.
CAPABILITIES = {
    # code layer
    "sast": "Static analysis of first-party source code",
    "dependency-vulnerabilities": "Known-vulnerable third-party dependencies",
    "secrets": "Hard-coded credentials and key material",
    "sbom": "Software bill of materials generation",
    "insecure-configuration": "Insecure infrastructure / application configuration",
    "license-risk": "Dependency licence obligations",
    # architecture layer
    "architecture-review": "Structured review of the architecture description",
    "threat-model": "Threat-model checklist coverage",
    # environment layer
    "transport-security": "TLS configuration and certificate validity",
    "http-security-headers": "Browser-enforced security response headers",
    "cookie-security": "Session cookie attributes",
    "cors": "Cross-origin resource sharing configuration",
    "information-exposure": "Unintentionally exposed paths and metadata",
    "http-methods": "Enabled HTTP verbs",
    "dast": "Dynamic application security testing",
    # cross-cutting
    "ai-advisory": "AI reasoning layer (advisory only, never sole evidence)",
}


@dataclass
class ScannerContext:
    """Everything a scanner is allowed to know about the assessment."""

    workdir: Path
    tool_path: ToolPath = field(default_factory=ToolPath)
    timeout: int = 900

    # Code layer
    project_path: Optional[Path] = None

    # Architecture layer
    architecture_docs: List[Path] = field(default_factory=list)
    architecture_manifest: Optional[Dict[str, Any]] = None
    architecture_manifest_path: Optional[Path] = None

    # Environment layer
    target_url: Optional[str] = None
    authorization: Optional[Authorization] = None
    scope: Optional[Scope] = None
    http: Optional[HttpClient] = None

    # Per-scanner settings, keyed by scanner name.
    settings: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    offline: bool = False
    verbose: bool = False

    #: Findings from the deterministic scanners, populated by the runner before
    #: cross-layer scanners run. Only the AI advisory layer reads this.
    prior_findings: List["Finding"] = field(default_factory=list)

    #: Commands executed during the current scanner, for the run record.
    commands: List[str] = field(default_factory=list)

    @property
    def confinement_root(self) -> Optional[Path]:
        """The directory every code-layer read must stay inside.

        Scanner adapters pass this to :func:`markna.scanners.util.read_snippet`
        so that a symlink out of the repository cannot pull external file
        contents into a report.
        """
        return self.project_path

    def setting(self, scanner: str, key: str, default: Any = None) -> Any:
        return self.settings.get(scanner, {}).get(key, default)

    def scanner_workdir(self, scanner: str) -> Path:
        path = self.workdir / scanner
        path.mkdir(parents=True, exist_ok=True)
        return path


class Scanner(abc.ABC):
    """Base class for every MARKNA scanner."""

    #: Unique registry name, also used as the ``source`` on findings.
    name: str = ""
    layer: Layer = Layer.CODE
    capabilities: Sequence[str] = ()
    #: False only for the AI reasoning layer.
    deterministic: bool = True
    #: Cross-layer scanners run whenever they are applicable, regardless of which
    #: layers were selected, and always after the deterministic scanners.
    cross_layer: bool = False
    description: str = ""
    #: External executables required, in preference order (first match wins).
    requires_executable: Sequence[str] = ()
    install_hint: str = ""

    # ------------------------------------------------------------- lifecycle

    def applicable(self, ctx: ScannerContext) -> tuple:
        """Is there anything for this scanner to look at? -> (bool, reason)."""
        return True, ""

    def available(self, ctx: ScannerContext) -> tuple:
        """Are the scanner's dependencies present? -> (bool, reason)."""
        if not self.requires_executable:
            return True, ""
        if self.resolve_executable(ctx):
            return True, ""
        names = " / ".join(self.requires_executable)
        hint = f" {self.install_hint}" if self.install_hint else ""
        return False, f"required executable not found: {names}.{hint}"

    def resolve_executable(self, ctx: ScannerContext) -> Optional[str]:
        for executable in self.requires_executable:
            resolved = ctx.tool_path.which(executable)
            if resolved:
                return resolved
        return None

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        return None

    @abc.abstractmethod
    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        """Run the scanner and yield normalised findings."""

    # ---------------------------------------------------------------- helpers

    def exec(self, ctx: ScannerContext, command: Sequence[str], **kwargs: Any):
        """Run an external tool, recording the command in the run record.

        Applies the run's tool path and timeout unless the caller overrides them.
        """
        from ..exec import run_command

        kwargs.setdefault("tool_path", ctx.tool_path)
        kwargs.setdefault("timeout", ctx.timeout)
        result = run_command(command, **kwargs)
        ctx.commands.append(result.display_command)
        return result

    def finding(self, **kwargs: Any) -> Finding:
        """Create a finding pre-tagged with this scanner's identity."""
        kwargs.setdefault("source", self.name)
        kwargs.setdefault("layer", self.layer)
        kwargs.setdefault("ai_generated", not self.deterministic)
        return Finding(**kwargs)

    def describe(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "layer": self.layer.value,
            "capabilities": list(self.capabilities),
            "deterministic": self.deterministic,
            "description": self.description,
            "requires_executable": list(self.requires_executable),
            "install_hint": self.install_hint,
        }


_REGISTRY: Dict[str, Scanner] = {}


def register(scanner_cls: Type[Scanner]) -> Type[Scanner]:
    """Class decorator that adds a scanner to the global registry."""
    instance = scanner_cls()
    if not instance.name:
        raise ValueError(f"{scanner_cls.__name__} must define a name")
    if instance.name in _REGISTRY:
        raise ValueError(f"duplicate scanner name: {instance.name}")
    unknown = set(instance.capabilities) - set(CAPABILITIES)
    if unknown:
        raise ValueError(
            f"{instance.name} declares unknown capabilities: {', '.join(sorted(unknown))}"
        )
    _REGISTRY[instance.name] = instance
    return scanner_cls


def all_scanners() -> List[Scanner]:
    from . import load_all  # local import to avoid a cycle at module import time

    load_all()
    return sorted(_REGISTRY.values(), key=lambda s: (s.layer.value, s.name))


def scanners_for_layer(layer: Layer) -> List[Scanner]:
    return [scanner for scanner in all_scanners() if scanner.layer is layer]


def get_scanner(name: str) -> Optional[Scanner]:
    all_scanners()
    return _REGISTRY.get(name)
