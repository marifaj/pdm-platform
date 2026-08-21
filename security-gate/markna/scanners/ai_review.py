"""AI advisory scanner.

Runs last, sees what the deterministic scanners produced, and adds reasoning on
top. Its findings are marked ``ai_generated`` and are advisory under the default
policy: they inform a human reviewer, they do not decide the gate.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from ..ai.reviewer import DEFAULT_MODEL, AiReviewError, AiReviewer
from ..models import Finding, Layer, Location, Severity
from .arch_checklist import TEXT_SUFFIXES
from .base import Scanner, ScannerContext, register


@register
class AiAdvisoryScanner(Scanner):
    name = "ai-advisory"
    layer = Layer.ARCHITECTURE
    capabilities = ("ai-advisory",)
    deterministic = False
    cross_layer = True
    description = (
        "Claude reasoning layer over the architecture description and the deterministic results. "
        "Advisory only — never counts as security evidence on its own."
    )
    install_hint = "Install with: pip install anthropic, and set ANTHROPIC_API_KEY."

    def applicable(self, ctx: ScannerContext) -> tuple:
        if not ctx.setting(self.name, "enabled", False):
            return False, "AI advisory layer is disabled (enable with --ai)"
        if not (ctx.architecture_docs or ctx.architecture_manifest or ctx.prior_findings):
            return False, "nothing to reason about: no architecture material and no findings"
        return True, ""

    def available(self, ctx: ScannerContext) -> tuple:
        ok, reason = AiReviewer.dependency_available()
        if not ok:
            return False, reason
        return AiReviewer.credentials_available()

    def tool_version(self, ctx: ScannerContext) -> Optional[str]:
        try:
            import anthropic

            return f"anthropic {anthropic.__version__}"
        except Exception:  # pragma: no cover - availability already checked
            return None

    def scan(self, ctx: ScannerContext) -> Iterable[Finding]:
        reviewer = AiReviewer(
            model=str(ctx.setting(self.name, "model", DEFAULT_MODEL)),
            max_tokens=int(ctx.setting(self.name, "max_tokens", 16_000)),
            effort=str(ctx.setting(self.name, "effort", "high")),
            include_evidence=bool(ctx.setting(self.name, "include_evidence", False)),
            timeout=float(ctx.setting(self.name, "timeout", 600.0)),
        )

        documents: Dict[str, str] = {}
        for path in ctx.architecture_docs:
            if path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            try:
                documents[str(path)] = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue

        try:
            result = reviewer.review(
                architecture_documents=documents,
                architecture_manifest=ctx.architecture_manifest,
                deterministic_findings=ctx.prior_findings,
                target_description=self._describe_target(ctx),
            )
        except AiReviewError as exc:
            raise RuntimeError(str(exc)) from exc

        findings: List[Finding] = list(result.findings)
        findings.append(self._provenance_finding(reviewer, result, ctx))
        if result.unanswered_questions:
            findings.append(self._questions_finding(result))
        return findings

    # ---------------------------------------------------------------- helpers

    def _describe_target(self, ctx: ScannerContext) -> str:
        lines = [
            f"project path: {ctx.project_path or 'not supplied'}",
            f"architecture documents: {', '.join(str(p) for p in ctx.architecture_docs) or 'none'}",
            f"architecture manifest: {ctx.architecture_manifest_path or 'none'}",
            f"deployed environment URL: {ctx.target_url or 'not assessed'}",
            f"deterministic findings supplied: {len(ctx.prior_findings)}",
        ]
        return "\n".join(lines)

    def _provenance_finding(
        self, reviewer: AiReviewer, result, ctx: ScannerContext
    ) -> Finding:
        return Finding(
            source=self.name,
            layer=Layer.ARCHITECTURE,
            severity=Severity.INFO,
            title="AI advisory review completed (advisory evidence only)",
            explanation=(
                "An AI reasoning pass ran over the architecture material and the deterministic "
                "findings. Its conclusions are labelled ai_generated and do not block the gate: "
                "they are a prompt for human review, not security evidence.\n\n"
                f"Model summary: {result.summary or 'none provided'}"
            ),
            evidence=(
                f"model: {result.model}\n"
                f"advisory findings returned: {len(result.findings)}\n"
                f"open questions returned: {len(result.unanswered_questions)}\n"
                f"finding evidence sent to the API: {reviewer.include_evidence}\n"
                f"source code sent to the API: False\n"
                f"tokens: {result.input_tokens} in / {result.output_tokens} out"
            ),
            location=Location(component="ai-advisory"),
            remediation=(
                "Triage each advisory finding: confirm it against the system, then either fix it, "
                "raise a deterministic check that would catch it, or dismiss it with a reason."
            ),
            rule_id="ai-advisory/provenance",
            tags=["ai-advisory", "provenance"],
            ai_generated=True,
            confidence="high",
        )

    def _questions_finding(self, result) -> Finding:
        questions = "\n".join(f"- {question}" for question in result.unanswered_questions)
        return Finding(
            source=self.name,
            layer=Layer.ARCHITECTURE,
            severity=Severity.INFO,
            title=f"{len(result.unanswered_questions)} question(s) the supplied material could not answer",
            explanation=(
                "The reviewing model identified information it needed but did not have. Each "
                "question is a gap in the evidence base for this gate, not a finding in itself."
            ),
            evidence=questions,
            location=Location(component="ai-advisory"),
            remediation=(
                "Answer the questions in the architecture manifest or documents, then re-run the "
                "gate so the answers are part of the assessed evidence."
            ),
            rule_id="ai-advisory/open-questions",
            tags=["ai-advisory", "evidence-gap"],
            ai_generated=True,
            confidence="high",
        )
