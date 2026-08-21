"""Command-line interface for the MARKNA Security Gate.

Exit codes are the contract with CI:

* ``0`` — PASS (or WARN, unless ``--fail-on warn``)
* ``1`` — WARN and ``--fail-on warn`` was requested
* ``2`` — BLOCK
* ``3`` — the assessment could not be run (configuration or setup error)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from . import __version__
from .authorization import Authorization, AuthorizationError
from .exec import ToolPath
from .models import Assessment, Layer, RunStatus, Verdict
from .policy import Policy, PolicyError, load_structured_file
from .report import FORMATS, write_reports
from .runner import ConfigurationError, RunConfig, Runner
from .scanners.base import CAPABILITIES, ScannerContext, all_scanners
from .templates import TEMPLATES

EXIT_PASS = 0
EXIT_WARN = 1
EXIT_BLOCK = 2
EXIT_ERROR = 3

_LAYER_NAMES = [layer.value for layer in Layer]


# --------------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="markna",
        description=(
            "MARKNA Security Gate — independent pre-UAT security review across architecture, "
            "source code and the deployed environment."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exit codes: 0 pass, 1 warn (with --fail-on warn), 2 block, 3 setup error.\n"
            "The environment layer sends traffic to a live system and requires --authorized-by."
        ),
    )
    parser.add_argument("--version", action="version", version=f"markna {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    assess = subparsers.add_parser("assess", help="run an assessment and write reports")
    _add_assess_arguments(assess)

    scanners = subparsers.add_parser(
        "scanners", help="list the scanner registry and what is installed"
    )
    scanners.add_argument("--tool-path", action="append", default=[], metavar="DIR")
    scanners.add_argument("--json", action="store_true", help="machine-readable output")

    init = subparsers.add_parser("init", help="write configuration templates")
    init.add_argument(
        "--dir", default=".", metavar="DIR", help="directory to write templates into"
    )
    init.add_argument("--force", action="store_true", help="overwrite existing files")

    capabilities = subparsers.add_parser(
        "capabilities", help="list the capability vocabulary used by policies"
    )
    capabilities.add_argument("--json", action="store_true")

    return parser


def _add_assess_arguments(parser: argparse.ArgumentParser) -> None:
    target = parser.add_argument_group("target")
    target.add_argument("--config", metavar="FILE", help="run configuration file (YAML or JSON)")
    target.add_argument("--project", metavar="DIR", help="path to the source tree")
    target.add_argument(
        "--arch", action="append", default=[], metavar="FILE",
        help="architecture document; repeatable",
    )
    target.add_argument("--arch-manifest", metavar="FILE", help="structured architecture manifest")
    target.add_argument("--url", metavar="URL", help="permitted UAT/demo URL")
    target.add_argument("--name", metavar="NAME", help="project name for the report")
    target.add_argument(
        "--layers", metavar="LIST",
        help=f"comma-separated subset of: {', '.join(_LAYER_NAMES)} (default: whatever has a target)",
    )

    auth = parser.add_argument_group("environment authorisation (required for --url)")
    auth.add_argument("--authorized-by", metavar="NAME", help="who permitted this environment test")
    auth.add_argument("--auth-reference", metavar="REF", help="change ticket or engagement id")
    auth.add_argument("--auth-expires", metavar="YYYY-MM-DD", help="date the permission lapses")
    auth.add_argument(
        "--scope-host", action="append", default=[], metavar="HOST",
        help="additional in-scope host; repeatable",
    )
    auth.add_argument(
        "--allow-private-targets", action="store_true",
        help="permit RFC1918 / loopback targets (internal UAT hosts)",
    )

    policy_group = parser.add_argument_group("policy and scanners")
    policy_group.add_argument("--policy", metavar="FILE", help="policy file (YAML or JSON)")
    policy_group.add_argument(
        "--only", action="append", default=[], metavar="SCANNER", help="run only these scanners"
    )
    policy_group.add_argument(
        "--skip", action="append", default=[], metavar="SCANNER", help="skip these scanners"
    )
    policy_group.add_argument(
        "--tool-path", action="append", default=[], metavar="DIR",
        help="extra directory to search for scanner binaries; repeatable",
    )
    policy_group.add_argument(
        "--offline", action="store_true",
        help="skip scanners that need network access to an advisory database or rule registry",
    )
    policy_group.add_argument(
        "--timeout", type=int, default=900, metavar="SECONDS", help="per-scanner timeout"
    )
    policy_group.add_argument(
        "--ai", action="store_true",
        help="enable the AI advisory layer (advisory findings only; never blocking)",
    )
    policy_group.add_argument("--ai-model", metavar="MODEL", help="model id for the AI layer")

    output = parser.add_argument_group("output")
    output.add_argument("--out", default="markna-reports", metavar="DIR", help="report directory")
    output.add_argument(
        "--format", default=None, metavar="LIST",
        help=f"comma-separated subset of: {', '.join(FORMATS)} (default: json,markdown)",
    )
    output.add_argument(
        "--include-raw", action="store_true",
        help="embed raw scanner output in the JSON report (may contain unredacted matches)",
    )
    output.add_argument(
        "--workdir", default=".markna", metavar="DIR", help="scratch directory for scanner artefacts"
    )
    output.add_argument(
        "--fail-on", choices=("block", "warn", "never"), default="block",
        help="which verdict produces a non-zero exit code (default: block)",
    )
    output.add_argument("--quiet", action="store_true", help="only print the verdict line")
    output.add_argument("--verbose", action="store_true", help="print scanner progress detail")


# ------------------------------------------------------------------ commands


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "assess":
            return _command_assess(args)
        if args.command == "scanners":
            return _command_scanners(args)
        if args.command == "init":
            return _command_init(args)
        if args.command == "capabilities":
            return _command_capabilities(args)
    except (ConfigurationError, PolicyError, AuthorizationError, ValueError) as exc:
        print(f"markna: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except KeyboardInterrupt:  # pragma: no cover - interactive
        print("markna: interrupted", file=sys.stderr)
        return EXIT_ERROR
    parser.error("no command")
    return EXIT_ERROR


def _command_assess(args: argparse.Namespace) -> int:
    file_config = _load_config_file(args.config)
    config = _build_run_config(args, file_config)

    quiet = args.quiet
    def progress(event: str, message: str) -> None:
        if quiet:
            return
        if event in ("skip", "unavailable") and not args.verbose:
            return
        prefix = {
            "start": "  ...",
            "done": "  ok ",
            "skip": "  -- ",
            "unavailable": "  !! ",
            "error": "  !! ",
            "verdict": "==> ",
        }.get(event, "    ")
        print(f"{prefix}{message}", flush=True)

    if not quiet:
        print(f"MARKNA Security Gate {__version__}")
        print(f"Layers: {', '.join(layer.value for layer in config.layers)}")
        print(f"Policy: {config.policy.name}")
        print("")

    assessment = Runner(config, progress).run()

    reports_config = file_config.get("reports", {}) if file_config else {}
    formats = _split_list(args.format) or list(reports_config.get("formats") or ["json", "markdown"])
    output_dir = Path(args.out if args.out != "markna-reports" else reports_config.get("directory", args.out))
    include_raw = args.include_raw or bool(reports_config.get("include_raw", False))
    written = write_reports(assessment, output_dir, formats, include_raw=include_raw)

    if not quiet:
        print("")
        _print_summary(assessment)
        print("")
        for fmt, path in written.items():
            print(f"  {fmt:<9} {path}")
        print("")
    print(f"MARKNA verdict: {assessment.verdict.value}")

    return _exit_code(assessment.verdict, args.fail_on)


def _command_scanners(args: argparse.Namespace) -> int:
    tool_path = ToolPath.from_env(args.tool_path)
    context = ScannerContext(workdir=Path(".markna"), tool_path=tool_path)
    rows = []
    for scanner in all_scanners():
        available, reason = scanner.available(context)
        rows.append(
            {
                **scanner.describe(),
                "available": available,
                "detail": reason or (scanner.resolve_executable(context) or "built-in"),
            }
        )

    if args.json:
        import json

        print(json.dumps(rows, indent=2))
        return EXIT_PASS

    print(f"MARKNA Security Gate {__version__} — scanner registry\n")
    for layer in Layer:
        layer_rows = [row for row in rows if row["layer"] == layer.value]
        if not layer_rows:
            continue
        print(f"{layer.value.upper()}")
        for row in layer_rows:
            mark = "available" if row["available"] else "MISSING  "
            kind = "" if row["deterministic"] else " [AI advisory, non-blocking]"
            print(f"  [{mark}] {row['name']}{kind}")
            print(f"              {row['description']}")
            print(f"              capabilities: {', '.join(row['capabilities'])}")
            if not row["available"]:
                print(f"              {row['detail']}")
        print("")
    print("A capability with no available scanner becomes a coverage-gap finding at run time.")
    return EXIT_PASS


def _command_init(args: argparse.Namespace) -> int:
    directory = Path(args.dir)
    directory.mkdir(parents=True, exist_ok=True)
    written, skipped = [], []
    for filename, content in TEMPLATES.items():
        path = directory / filename
        if path.exists() and not args.force:
            skipped.append(path)
            continue
        path.write_text(content, encoding="utf-8")
        written.append(path)

    for path in written:
        print(f"wrote    {path}")
    for path in skipped:
        print(f"skipped  {path} (exists; use --force to overwrite)")
    if written:
        print(
            "\nNext: fill in architecture.yaml with what is actually true, then run\n"
            "  markna assess --config markna.yaml"
        )
    return EXIT_PASS


def _command_capabilities(args: argparse.Namespace) -> int:
    if args.json:
        import json

        print(json.dumps(CAPABILITIES, indent=2))
        return EXIT_PASS
    print("Capabilities a policy can require (required_capabilities in the policy file):\n")
    width = max(len(name) for name in CAPABILITIES)
    for name, description in CAPABILITIES.items():
        print(f"  {name:<{width}}  {description}")
    return EXIT_PASS


# ------------------------------------------------------------------- helpers


def _load_config_file(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    file_path = Path(path)
    if not file_path.is_file():
        raise ConfigurationError(f"config file not found: {path}")
    data = load_structured_file(file_path)
    if not isinstance(data, dict):
        raise ConfigurationError(f"config file must contain a mapping: {path}")
    return data


def _build_run_config(args: argparse.Namespace, file_config: Dict[str, Any]) -> RunConfig:
    architecture = file_config.get("architecture") or {}
    environment = file_config.get("environment") or {}

    project = args.project or file_config.get("project")
    project_path = Path(project).resolve() if project else None
    if project_path and not project_path.is_dir():
        raise ConfigurationError(f"project path is not a directory: {project_path}")

    documents = [Path(doc) for doc in (args.arch or architecture.get("documents") or [])]
    manifest_value = args.arch_manifest or architecture.get("manifest")
    manifest = Path(manifest_value) if manifest_value else None
    url = args.url or environment.get("url") or None

    authorization = _build_authorization(args, environment)
    layers = _resolve_layers(args, file_config, project_path, documents, manifest, url)

    policy_path = args.policy or file_config.get("policy")
    policy = Policy.load(policy_path)

    settings: Dict[str, Dict[str, Any]] = {
        name: dict(values or {})
        for name, values in (file_config.get("scanners") or {}).items()
    }
    if args.ai:
        settings.setdefault("ai-advisory", {})["enabled"] = True
    if args.ai_model:
        settings.setdefault("ai-advisory", {})["model"] = args.ai_model

    return RunConfig(
        project_path=project_path,
        architecture_docs=documents,
        architecture_manifest_path=manifest,
        target_url=url,
        layers=layers,
        authorization=authorization,
        policy=policy,
        workdir=Path(args.workdir).resolve(),
        tool_path=ToolPath.from_env(args.tool_path),
        timeout=args.timeout,
        settings=settings,
        offline=args.offline,
        verbose=args.verbose,
        only_scanners=list(args.only),
        skip_scanners=list(args.skip),
        project_name=args.name or (file_config.get("name")),
    )


def _build_authorization(
    args: argparse.Namespace, environment: Dict[str, Any]
) -> Optional[Authorization]:
    from_file = dict(environment.get("authorization") or {})
    authorized_by = args.authorized_by or from_file.get("authorized_by")
    if not authorized_by:
        return None
    data = {
        **from_file,
        "authorized_by": authorized_by,
        "reference": args.auth_reference or from_file.get("reference"),
        "expires": args.auth_expires or from_file.get("expires"),
        "scope_hosts": list(args.scope_host) + list(from_file.get("scope_hosts") or []),
        "allow_private_targets": args.allow_private_targets
        or bool(from_file.get("allow_private_targets", False)),
    }
    return Authorization.from_dict(data)


def _resolve_layers(
    args: argparse.Namespace,
    file_config: Dict[str, Any],
    project_path: Optional[Path],
    documents: List[Path],
    manifest: Optional[Path],
    url: Optional[str],
) -> List[Layer]:
    requested = _split_list(args.layers) or list(file_config.get("layers") or [])
    if requested:
        unknown = set(requested) - set(_LAYER_NAMES)
        if unknown:
            raise ConfigurationError(
                f"unknown layer(s): {', '.join(sorted(unknown))}. Valid: {', '.join(_LAYER_NAMES)}"
            )
        layers = [Layer(name) for name in _LAYER_NAMES if name in requested]
    else:
        layers = []
        if documents or manifest:
            layers.append(Layer.ARCHITECTURE)
        if project_path:
            layers.append(Layer.CODE)
        if url:
            layers.append(Layer.ENVIRONMENT)

    if not layers:
        raise ConfigurationError(
            "nothing to assess: supply --project, --arch/--arch-manifest, or --url "
            "(or a --config file that does)"
        )

    missing_targets = []
    if Layer.ARCHITECTURE in layers and not (documents or manifest):
        missing_targets.append("architecture layer requested but no --arch or --arch-manifest given")
    if Layer.CODE in layers and not project_path:
        missing_targets.append("code layer requested but no --project given")
    if Layer.ENVIRONMENT in layers and not url:
        missing_targets.append("environment layer requested but no --url given")
    if missing_targets:
        raise ConfigurationError("; ".join(missing_targets))
    return layers


def _print_summary(assessment: Assessment) -> None:
    summary = assessment.to_dict()["summary"]
    counts = summary["by_severity"]
    print(
        "Findings: "
        + ", ".join(
            f"{counts[severity]} {severity}"
            for severity in ("critical", "high", "medium", "low", "info")
        )
    )
    print(f"Blocking: {summary['blocking_findings']}")

    gaps = [entry for entry in assessment.coverage if not entry.satisfied]
    if gaps:
        print("")
        print("Coverage gaps (no deterministic evidence gathered):")
        for entry in gaps:
            print(f"  - {entry.layer.value}: {entry.capability}")

    unavailable = [run for run in assessment.runs if run.status is RunStatus.UNAVAILABLE]
    if unavailable:
        print("")
        print("Scanners not installed on this runner:")
        for run in unavailable:
            print(f"  - {run.name}: {run.message}")

    for reason in assessment.verdict_reasons:
        print(f"  {reason}")


def _split_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _exit_code(verdict: Verdict, fail_on: str) -> int:
    if fail_on == "never":
        return EXIT_PASS
    if verdict is Verdict.BLOCK:
        return EXIT_BLOCK
    if verdict is Verdict.WARN and fail_on == "warn":
        return EXIT_WARN
    return EXIT_PASS


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
