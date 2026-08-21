"""Self-contained HTML report, suitable for attaching to a release record."""

from __future__ import annotations

import html
from typing import List

from ..models import Assessment, Finding, Layer, Severity, Verdict

_VERDICT_COLOUR = {
    Verdict.PASS: ("#0b6b3a", "#e6f6ec"),
    Verdict.WARN: ("#8a5a00", "#fdf3e0"),
    Verdict.BLOCK: ("#8a1220", "#fdeaec"),
}

_SEVERITY_COLOUR = {
    Severity.CRITICAL: "#8a1220",
    Severity.HIGH: "#c0392b",
    Severity.MEDIUM: "#b8860b",
    Severity.LOW: "#31708f",
    Severity.INFO: "#5a6570",
}

_CSS = """
:root { color-scheme: light dark; --fg:#1c2024; --bg:#ffffff; --muted:#5a6570;
        --line:#dde2e7; --card:#f7f9fb; }
@media (prefers-color-scheme: dark) {
  :root { --fg:#e6e9ec; --bg:#14181c; --muted:#9aa4ae; --line:#2a3138; --card:#1c2228; }
}
* { box-sizing: border-box; }
body { margin:0; padding:2rem 1.25rem 4rem; background:var(--bg); color:var(--fg);
       font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }
main { max-width: 62rem; margin: 0 auto; }
h1 { font-size:1.75rem; margin:0 0 .25rem; }
h2 { font-size:1.25rem; margin:2.5rem 0 .75rem; padding-bottom:.35rem; border-bottom:1px solid var(--line); }
h3 { font-size:1.05rem; margin:1.75rem 0 .5rem; }
.sub { color:var(--muted); margin:0 0 1.5rem; }
.verdict { display:inline-block; padding:.6rem 1.1rem; border-radius:.5rem; font-weight:700;
           font-size:1.15rem; letter-spacing:.04em; }
.reasons { margin:.9rem 0 0; padding-left:1.2rem; color:var(--muted); }
table { width:100%; border-collapse:collapse; margin:.5rem 0 1rem; font-size:.92rem; }
th, td { text-align:left; padding:.45rem .6rem; border-bottom:1px solid var(--line); vertical-align:top; }
th { color:var(--muted); font-weight:600; }
td.no { color:#c0392b; font-weight:700; }
.finding { border:1px solid var(--line); border-left-width:4px; border-radius:.4rem;
           padding:.9rem 1.1rem; margin:.85rem 0; background:var(--card); }
.finding h4 { margin:0 0 .4rem; font-size:1rem; }
.meta { color:var(--muted); font-size:.85rem; margin:0 0 .6rem; }
.badge { display:inline-block; padding:.1rem .45rem; border-radius:.25rem; font-size:.72rem;
         font-weight:700; letter-spacing:.03em; color:#fff; margin-right:.4rem; }
.badge.block { background:#8a1220; }
.badge.ai { background:#5a4fcf; }
pre { background:var(--bg); border:1px solid var(--line); border-radius:.35rem; padding:.7rem;
      overflow-x:auto; font-size:.83rem; margin:.5rem 0; white-space:pre-wrap; word-break:break-word; }
code { font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:.9em; }
.rem { margin:.5rem 0 0; }
footer { margin-top:3rem; padding-top:1rem; border-top:1px solid var(--line);
         color:var(--muted); font-size:.85rem; }
"""


def render_html(assessment: Assessment) -> str:
    data = assessment.to_dict()
    summary = data["summary"]
    fg, bg = _VERDICT_COLOUR[assessment.verdict]
    parts: List[str] = []

    parts.append("<!doctype html><html lang='en'><head><meta charset='utf-8'>")
    parts.append("<meta name='viewport' content='width=device-width,initial-scale=1'>")
    parts.append(f"<title>MARKNA Security Gate — {_e(assessment.target.name or 'assessment')}</title>")
    parts.append(f"<style>{_CSS}</style></head><body><main>")

    parts.append("<h1>MARKNA Security Gate Report</h1>")
    parts.append(
        f"<p class='sub'>{_e(assessment.target.name or 'Unnamed project')} · "
        f"{_e(assessment.assessment_id)} · policy <code>{_e(assessment.policy_name)}</code></p>"
    )
    parts.append(
        f"<div class='verdict' style='color:{fg};background:{bg}'>{assessment.verdict.value}</div>"
    )
    parts.append("<ul class='reasons'>")
    parts.extend(f"<li>{_e(reason)}</li>" for reason in assessment.verdict_reasons)
    parts.append("</ul>")

    parts.append("<h2>Assessment</h2>")
    parts.append(
        _table(
            ["Field", "Value"],
            [
                ["Started", assessment.started_at],
                ["Finished", assessment.finished_at],
                ["Layers", ", ".join(layer.value for layer in assessment.layers_requested)],
                ["Project path", assessment.target.project_path or "not assessed"],
                ["Git commit", assessment.target.git_commit or "n/a"],
                ["Architecture manifest", assessment.target.architecture_manifest or "none"],
                [
                    "Architecture documents",
                    ", ".join(assessment.target.architecture_docs) or "none",
                ],
                ["Environment URL", assessment.target.environment_url or "not assessed"],
                ["Gate version", assessment.tool_version],
            ],
        )
    )

    parts.append("<h2>Findings summary</h2>")
    parts.append(
        _table(
            ["Severity", "Count"],
            [[severity, summary["by_severity"][severity]] for severity in
             ("critical", "high", "medium", "low", "info")]
            + [["total", summary["total_findings"]]],
        )
    )
    parts.append(
        "<p>"
        f"Blocking: <strong>{summary['blocking_findings']}</strong> · "
        f"deterministic: {summary['deterministic_findings']} · "
        f"AI advisory (non-blocking): {summary['advisory_ai_findings']} · "
        f"suppressed: {summary['suppressed_findings']} · "
        f"scanners completed: {summary['scanners_ok']}/{summary['scanners_total']}"
        "</p>"
    )

    if assessment.coverage:
        parts.append("<h2>Evidence coverage</h2>")
        parts.append(
            "<p class='sub'>Required deterministic capabilities. A gap means the gate gathered no "
            "evidence about that class of risk.</p>"
        )
        rows = []
        for entry in assessment.coverage:
            rows.append(
                [
                    entry.layer.value,
                    entry.capability,
                    "yes" if entry.satisfied else "NO",
                    ", ".join(entry.satisfied_by) or "—",
                ]
            )
        parts.append(_table(["Layer", "Capability", "Covered", "Provided by"], rows, no_column=2))

    parts.append("<h2>Findings</h2>")
    active = assessment.active_findings
    if not active:
        parts.append("<p>No findings were reported.</p>")
    else:
        blocking = [finding for finding in active if finding.blocking]
        if blocking:
            parts.append(f"<h3>Blocking ({len(blocking)})</h3>")
            parts.extend(_finding_html(finding) for finding in blocking)
        for layer in Layer:
            rest = [f for f in active if f.layer is layer and not f.blocking]
            if not rest:
                continue
            parts.append(f"<h3>{layer.value.capitalize()} layer ({len(rest)})</h3>")
            parts.extend(_finding_html(finding) for finding in rest)

    parts.append("<h2>Scanner execution record</h2>")
    parts.append(
        _table(
            ["Scanner", "Layer", "Status", "Kind", "Findings", "Duration", "Detail"],
            [
                [
                    run.name,
                    run.layer.value,
                    run.status.value,
                    "deterministic" if run.deterministic else "AI advisory",
                    run.findings_count,
                    f"{run.duration_seconds:.1f}s",
                    run.message or run.tool_version or "",
                ]
                for run in assessment.runs
            ],
        )
    )

    parts.append(
        "<footer>"
        "<p><strong>Deterministic findings</strong> come from open-source scanners and MARKNA's "
        "rule engines; they are reproducible. <strong>AI advisory findings</strong> come from a "
        "language model reasoning over the architecture material and the deterministic results: "
        "they are labelled, cannot block a release under the default policy, and require human "
        "confirmation. A <strong>coverage gap</strong> is a finding in its own right — a scanner "
        "that did not run found nothing, which is not the same as there being nothing to find.</p>"
        f"<p>Generated by MARKNA Security Gate {_e(assessment.tool_version)} at "
        f"{_e(assessment.finished_at)}.</p></footer>"
    )
    parts.append("</main></body></html>")
    return "".join(parts)


def _finding_html(finding: Finding) -> str:
    colour = _SEVERITY_COLOUR[finding.severity]
    badges = ""
    if finding.blocking:
        badges += "<span class='badge block'>BLOCKING</span>"
    if finding.ai_generated:
        badges += "<span class='badge ai'>AI ADVISORY</span>"

    parts = [f"<div class='finding' style='border-left-color:{colour}'>"]
    parts.append(f"<h4>{badges}<code>{_e(finding.id)}</code> — {_e(finding.title)}</h4>")
    meta = (
        f"{finding.severity.value} · {finding.layer.value} · {finding.source}"
        + (f" · <code>{_e(finding.rule_id)}</code>" if finding.rule_id else "")
        + f" · <code>{_e(finding.location.describe())}</code>"
        + f" · confidence {finding.confidence} · {finding.timestamp}"
    )
    if finding.cwe:
        meta += f" · {_e(', '.join(finding.cwe))}"
    parts.append(f"<p class='meta'>{meta}</p>")
    parts.append(f"<p>{_e(finding.explanation).replace(chr(10), '<br>')}</p>")
    if finding.evidence:
        parts.append(f"<pre>{_e(finding.evidence)}</pre>")
    if finding.remediation:
        parts.append(f"<p class='rem'><strong>Remediation.</strong> {_e(finding.remediation)}</p>")
    if finding.references:
        links = " · ".join(
            f"<a href='{_e(reference)}'>{_e(reference)}</a>" for reference in finding.references
        )
        parts.append(f"<p class='meta'>{links}</p>")
    parts.append("</div>")
    return "".join(parts)


def _table(headers: List[str], rows: List[List], no_column: int = -1) -> str:
    head = "".join(f"<th>{_e(header)}</th>" for header in headers)
    body = []
    for row in rows:
        cells = []
        for index, cell in enumerate(row):
            classes = " class='no'" if index == no_column and str(cell) == "NO" else ""
            cells.append(f"<td{classes}>{_e(str(cell))}</td>")
        body.append(f"<tr>{''.join(cells)}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def _e(value) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)
