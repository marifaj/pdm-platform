"""SBOM generation.

An SBOM is not a finding source; it is evidence that the release's component
inventory was captured. The gate records the artefact path, the component count
and the format, and fails the SBOM coverage requirement if no generator ran.

Syft is preferred (multi-ecosystem, container-aware); cyclonedx-py covers Python
projects and installs with pip.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..exec import probe_version
from ..models import Finding, Layer, Location, Severity
from .base import Scanner, ScannerContext, register
from .util import find_files, relative_path


def _sbom_finding(
    scanner: str,
    sbom_path: Path,
    component_count: int,
    fmt: str,
    detail: str,
    project: Path,
) -> Finding:
    return Finding(
        source=scanner,
        layer=Layer.CODE,
        severity=Severity.INFO,
        title=f"SBOM generated ({component_count} components, {fmt})",
        explanation=(
            "A software bill of materials was produced for this release. Attach it to the "
            "release record so that a future advisory can be matched against the exact "
            "component set that was deployed."
        ),
        evidence=detail,
        location=Location(file=str(sbom_path), component=fmt),
        remediation=(
            "Archive the SBOM alongside the build artefact and feed it to a continuous "
            "vulnerability monitor (for example Dependency-Track or Grype)."
        ),
        rule_id="sbom/generated",
        tags=["sbom", "inventory"],
    )


@register
class SyftSbomScanner(Scanner):
    name = "syft"
    layer = Layer.CODE
    capabilities = ("sbom",)
    description = "Anchore Syft: multi-ecosystem CycloneDX SBOM generation."
    requires_executable = ("syft",)
    install_hint = "Install from https://github.com/anchore/syft/releases"

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        return probe_version("syft", tool_path=ctx.tool_path)

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        sbom_path = ctx.scanner_workdir(self.name) / "sbom.cyclonedx.json"
        command = [
            "syft",
            f"dir:{project}",
            "-o",
            f"cyclonedx-json={sbom_path}",
            "-q",
        ]
        result = self.exec(ctx, command, cwd=project)
        if not result.ok or not sbom_path.exists():
            raise RuntimeError(f"syft failed: {result.failure_message()}")

        data = json.loads(sbom_path.read_text(encoding="utf-8", errors="replace") or "{}")
        components = data.get("components", []) or []
        return [
            _sbom_finding(
                self.name,
                sbom_path,
                len(components),
                f"CycloneDX {data.get('specVersion', '?')}",
                f"generator: syft\nartefact: {sbom_path}\ncomponents: {len(components)}",
                project,
            )
        ]


@register
class CycloneDxPythonSbomScanner(Scanner):
    name = "cyclonedx-py"
    layer = Layer.CODE
    capabilities = ("sbom",)
    description = "cyclonedx-py: CycloneDX SBOM for Python requirement files."
    requires_executable = ("cyclonedx-py",)
    install_hint = "Install with: pip install cyclonedx-bom"

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.project_path:
            return False, "no project path supplied"
        if ctx.tool_path.which("syft"):
            return False, "syft is installed and produces a broader SBOM"
        if not self._requirement_files(ctx.project_path):
            return False, "no non-empty requirements*.txt files found"
        return True, ""

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        return probe_version("cyclonedx-py", tool_path=ctx.tool_path)

    def _requirement_files(self, project: Path) -> List[Path]:
        found = find_files(project, ["requirements*.txt", "requirements/*.txt"])
        return [path for path in found if path.stat().st_size > 0]

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        project = ctx.project_path
        assert project is not None
        workdir = ctx.scanner_workdir(self.name)
        findings: List[Finding] = []
        failures: List[str] = []

        for index, requirements in enumerate(self._requirement_files(project)):
            sbom_path = workdir / f"sbom-{index}-{requirements.stem}.cyclonedx.json"
            command = [
                "cyclonedx-py",
                "requirements",
                str(requirements),
                "--of",
                "JSON",
                "-o",
                str(sbom_path),
                "--no-validate",
            ]
            result = self.exec(ctx, command, cwd=project)
            if not result.ok or not sbom_path.exists():
                failures.append(f"{requirements.name}: {result.failure_message()}")
                continue
            data: Dict[str, Any] = json.loads(
                sbom_path.read_text(encoding="utf-8", errors="replace") or "{}"
            )
            components = data.get("components", []) or []
            findings.append(
                _sbom_finding(
                    self.name,
                    sbom_path,
                    len(components),
                    f"CycloneDX {data.get('specVersion', '?')}",
                    (
                        f"generator: cyclonedx-py\n"
                        f"source: {relative_path(str(requirements), project)}\n"
                        f"artefact: {sbom_path}\ncomponents: {len(components)}"
                    ),
                    project,
                )
            )

        if not findings:
            raise RuntimeError("; ".join(failures) or "no SBOM was produced")
        return findings
