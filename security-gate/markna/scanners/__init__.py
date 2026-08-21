"""Scanner registry.

Importing a scanner module is what registers it, so :func:`load_all` is the
single place that knows which modules exist.
"""

from __future__ import annotations

import importlib
from typing import List

_MODULES: List[str] = [
    # Architecture layer
    "arch_manifest",
    "arch_checklist",
    # Code layer
    "code_semgrep",
    "code_bandit",
    "code_secrets",
    "code_deps",
    "code_trivy",
    "code_config",
    "code_sbom",
    # Environment layer
    "env_tls",
    "env_http",
    "env_exposure",
    "env_zap",
    # Cross-cutting advisory layer
    "ai_review",
]

_loaded = False


def load_all() -> None:
    """Import every scanner module exactly once."""
    global _loaded
    if _loaded:
        return
    _loaded = True
    for module in _MODULES:
        importlib.import_module(f"{__name__}.{module}")
