"""Markdown report — the document a human reviewer reads before signing off."""

from __future__ import annotations

from typing import Dict, List

from ..models import Assessment, Finding, Layer, RunStatus, Severity, Verdict

_VERDICT_LINE = {
    Verdict.PASS: "**PASS** — no findings at or above the warn threshold, and required coverage was met.",
    Verdict.WARN: "**WARN** — issues were found that do not block the release under this policy, but require triage.",
    Verdict.BLOCK: "**BLOCK** — one or more findings block release under this policy.",
}

_SEVERITY_ORDER = [
    Severity.CRITICAL,
    Severity.HIGH,
    Severity.MEDIUM,
    Severity.LOW,
    Severity.INFO,
]


def render_markdown(assessment: Assessment) -> str:
    data = assessment.to_dict()
    summary = data["summary"]
    lines: List[str] = []

    lines.append("# MARKNA Security Gate Report")
    lines.append("")
    lines.append(f"## Verdict: {assessment.verdict.value}")
    lines.append("")
    lines.append(_VERDICT_LINE[assessment.verdict])
    lines.append("")
    for reason in assessment.verdict_reasons:
        lines.append(f"- {reason}")
    lines.append("")

    lines.extend(_metadata_section(assessment))
    lines.extend(_summary_section(summary))
    lines.extend(_coverage_section(assessment))
    lines.extend(_findings_sections(assessment))
    lines.extend(_scanner_section(assessment))
    lines.extend(_suppressed_section(assessment))
    lines.extend(_footer(assessment))
    return "\n".join(lines).rstrip() + "\n"


# ------------------------------------------------------------------- sections


def _metadata_section(assessment: Assessment) -> List[str]:
    target = assessment.target
    rows = [
        ("Assessment id", assessment.assessment_id),
        ("Policy", assessment.policy_name),
        ("Started", assessment.started_at),
        ("Finished", assessment.finished_at),
        ("Layers assessed", ", ".join(layer.value for layer in assessment.layers_requested)),
        ("Project", target.name or "n/a"),
        ("Project path", target.project_path or "not assessed"),
        ("Git commit", target.git_commit or "n/a"),
        ("Git branch", target.git_branch or "n/a"),
        ("Architecture documents", ", ".join(target.architecture_docs) or "none"),
        ("Architecture manifest", target.architecture_manifest or "none"),
        ("Environment URL", target.environment_url or "not assessed"),
        ("Gate version", assessment.tool_version),
    ]
    lines = ["## Assessment", "", "| Field | Value |", "| --- | --- |"]
    lines.extend(f"| {label} | {_escape(str(value))} |" for label, value in rows)
    lines.append("")
    return lines


def _summary_section(summary: Dict) -> List[str]:
    by_severity = summary["by_severity"]
    lines = ["## Findings summary", "", "| Severity | Count |", "| --- | --- |"]
    for severity in _SEVERITY_ORDER:
        lines.append(f"| {severity.value} | {by_severity.get(severity.value, 0)} |")
    lines.append(f"| **total** | **{summary['total_findings']}** |")
    lines.append("")
    lines.append(f"- Blocking findings: **{summary['blocking_findings']}**")
    lines.append(f"- Deterministic findings: {summary['deterministic_findings']}")
    lines.append(f"- AI advisory findings (non-blocking): {summary['advisory_ai_findings']}")
    lines.append(f"- Suppressed by policy: {summary['suppressed_findings']}")
    lines.append(
        f"- Scanners completed: {summary['scanners_ok']} of {summary['scanners_total']} considered"
    )
    lines.append("")
    lines.append("By layer:")
    lines.append("")
    for layer, count in summary["by_layer"].items():
        lines.append(f"- {layer}: {count}")
    lines.append("")
    return lines


def _coverage_section(assessment: Assessment) -> List[str]:
    if not assessment.coverage:
        return []
    lines = [
        "## Evidence coverage",
        "",
        "Required deterministic capabilities and the scanner that provided each. A gap here means "
        "the gate has no evidence about that class of risk.",
        "",
        "| Layer | Capability | Covered | Provided by |",
        "| --- | --- | --- | --- |",
    ]
    for entry in assessment.coverage:
        mark = "yes" if entry.satisfied else "**NO**"
        providers = ", ".join(entry.satisfied_by) or "—"
        lines.append(f"| {entry.layer.value} | {entry.capability} | {mark} | {providers} |")
    lines.append("")
    return lines


def _findings_sections(assessment: Assessment) -> List[str]:
    active = [finding for finding in assessment.active_findings]
    if not active:
        return ["## Findings", "", "No findings were reported.", ""]

    lines = ["## Findings", ""]
    blocking = [finding for finding in active if finding.blocking]
    if blocking:
        lines.append(f"### Blocking ({len(blocking)})")
        lines.append("")
        for finding in blocking:
            lines.extend(_finding_block(finding))

    for layer in Layer:
        layer_findings = [
            finding for finding in active if finding.layer is layer and not finding.blocking
        ]
        if not layer_findings:
            continue
        lines.append(f"### {layer.value.capitalize()} layer ({len(layer_findings)})")
        lines.append("")
        for finding in layer_findings:
            lines.extend(_finding_block(finding))
    return lines


def _finding_block(finding: Finding) -> List[str]:
    flags = []
    if finding.blocking:
        flags.append("BLOCKING")
    if finding.ai_generated:
        flags.append("AI ADVISORY — not security evidence on its own")
    flag_text = f" — _{'; '.join(flags)}_" if flags else ""

    lines = [
        f"#### `{finding.id}` {finding.title}",
        "",
        f"- **Severity**: {finding.severity.value}{flag_text}",
        f"- **Layer**: {finding.layer.value}",
        f"- **Source**: {finding.source}"
        + (f" (`{finding.rule_id}`)" if finding.rule_id else ""),
        f"- **Location**: `{finding.location.describe()}`",
        f"- **Confidence**: {finding.confidence}",
        f"- **Detected**: {finding.timestamp}",
    ]
    if finding.cwe:
        lines.append(f"- **CWE**: {', '.join(finding.cwe)}")
    lines.append("")
    lines.append(finding.explanation.strip() or "_No explanation supplied._")
    lines.append("")
    if finding.evidence:
        lines.append("**Evidence**")
        lines.append("")
        lines.append("```")
        lines.append(finding.evidence.rstrip())
        lines.append("```")
        lines.append("")
    if finding.remediation:
        lines.append(f"**Remediation** — {finding.remediation.strip()}")
        lines.append("")
    if finding.references:
        lines.append("**References**")
        lines.append("")
        lines.extend(f"- {reference}" for reference in finding.references)
        lines.append("")
    return lines


def _scanner_section(assessment: Assessment) -> List[str]:
    lines = [
        "## Scanner execution record",
        "",
        "| Scanner | Layer | Status | Kind | Findings | Duration | Detail |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for run in assessment.runs:
        kind = "deterministic" if run.deterministic else "AI advisory"
        detail = run.message or (run.tool_version or "")
        lines.append(
            f"| {run.name} | {run.layer.value} | {run.status.value} | {kind} | "
            f"{run.findings_count} | {run.duration_seconds:.1f}s | {_escape(detail)} |"
        )
    lines.append("")

    missing = [run for run in assessment.runs if run.status is RunStatus.UNAVAILABLE]
    if missing:
        lines.append("Scanners that were not available on this runner:")
        lines.append("")
        for run in missing:
            lines.append(f"- **{run.name}** — {_escape(run.message or 'unavailable')}")
        lines.append("")
    return lines


def _suppressed_section(assessment: Assessment) -> List[str]:
    suppressed = [finding for finding in assessment.findings if finding.suppressed]
    if not suppressed:
        return []
    lines = [
        "## Suppressed findings",
        "",
        "These were reported by a scanner and excluded by policy. Each remains in the JSON report.",
        "",
        "| Id | Severity | Title | Reason |",
        "| --- | --- | --- | --- |",
    ]
    for finding in suppressed:
        lines.append(
            f"| `{finding.id}` | {finding.severity.value} | {_escape(finding.title)} | "
            f"{_escape(finding.suppression_reason or '')} |"
        )
    lines.append("")
    return lines


def _footer(assessment: Assessment) -> List[str]:
    return [
        "---",
        "",
        "### How to read this report",
        "",
        "- **Deterministic findings** come from open-source scanners and MARKNA's own rule engines. "
        "They are reproducible: the same input produces the same finding id.",
        "- **AI advisory findings** are produced by a language model reasoning over the "
        "architecture material and the deterministic results. They are labelled, they cannot block "
        "a release under the default policy, and they are not security evidence on their own — "
        "each one needs human confirmation.",
        "- **Coverage gaps** are findings too. A scanner that did not run produced no findings, "
        "and that is not the same as finding nothing.",
        "",
        f"Generated by MARKNA Security Gate {assessment.tool_version} at {assessment.finished_at}.",
        "",
    ]


def _escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ").strip()
