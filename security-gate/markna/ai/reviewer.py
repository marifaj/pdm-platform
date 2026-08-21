"""Anthropic-backed reasoning layer over the deterministic results.

What this layer is for: reading the architecture description alongside what the
scanners actually found, and pointing out the things a pattern matcher cannot —
a control that is described but contradicted by a finding, a data flow nobody
modelled, an authorisation gap that only shows up when two documents are read
together.

What it is explicitly not for: replacing a scanner. Its output is advisory,
labelled as such in every report, and cannot block a release under the default
policy.

Data handling: by default only the architecture documents, the manifest and the
*titles and locations* of deterministic findings are sent to the API. Finding
evidence (which may contain secrets or customer data) and source code are not
sent unless the operator opts in.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from ..models import Finding, Layer, Location, Severity

#: Default model. Override with the `model` setting or MARKNA_AI_MODEL.
DEFAULT_MODEL = "claude-opus-5"
MAX_DOCUMENT_CHARS = 120_000
MAX_FINDINGS_IN_PROMPT = 200

SYSTEM_PROMPT = """\
You are an independent application security reviewer working for MARKNA Security \
Gate. A software project is being assessed before it is released to UAT or \
production. Deterministic scanners have already run; their results are given to \
you.

Your job is the reasoning that scanners cannot do:
- contradictions between what the architecture claims and what the scanners found;
- missing controls that no scanner would flag because there is nothing to flag \
(an absent authorisation model, an unmodelled data flow, a trust boundary that \
exists in the deployment but not in the design);
- threat-model blind spots, especially chains where two individually-minor issues \
combine;
- risks specific to this system's domain and data.

Rules you must follow:
1. Ground every finding in the material provided. Quote the document line, the \
manifest field, or the scanner finding id that supports it in the `evidence` field.
2. Do not repeat a finding a scanner already reported unless you are adding \
materially new reasoning about its impact; if you do, say which finding id you \
are building on.
3. If the material is insufficient to judge something, do not guess. Put it in \
`unanswered_questions` instead.
4. Severity reflects impact on this system if the issue is real, using the \
scale critical/high/medium/low/info.
5. Set `confidence` honestly. Low confidence is a useful answer; a confident \
wrong answer is not.
6. Prefer a small number of substantive findings over a long list of generic \
advice. Never pad.

Your output is advisory. It will be labelled as AI-generated in the report and \
cannot by itself block a release, so write for a human reviewer who has to decide \
what to do next.
"""

FINDING_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "summary": {
            "type": "string",
            "description": "Two to four sentences on the security posture of this release.",
        },
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "layer": {
                        "type": "string",
                        "enum": ["architecture", "code", "environment"],
                    },
                    "severity": {
                        "type": "string",
                        "enum": ["critical", "high", "medium", "low", "info"],
                    },
                    "title": {"type": "string"},
                    "explanation": {"type": "string"},
                    "evidence": {
                        "type": "string",
                        "description": "Quoted line, manifest field or finding id from the supplied material.",
                    },
                    "location": {
                        "type": "string",
                        "description": "File path, manifest path, URL or component name.",
                    },
                    "remediation": {"type": "string"},
                    "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                    "builds_on_finding_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": [
                    "layer",
                    "severity",
                    "title",
                    "explanation",
                    "evidence",
                    "location",
                    "remediation",
                    "confidence",
                    "builds_on_finding_ids",
                ],
                "additionalProperties": False,
            },
        },
        "unanswered_questions": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Questions a human reviewer must answer that the material could not.",
        },
    },
    "required": ["summary", "findings", "unanswered_questions"],
    "additionalProperties": False,
}


class AiReviewError(RuntimeError):
    """Raised when the AI layer cannot run or returns something unusable."""


@dataclass
class AiReviewResult:
    summary: str = ""
    findings: List[Finding] = field(default_factory=list)
    unanswered_questions: List[str] = field(default_factory=list)
    model: str = ""
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass
class AiReviewer:
    """Thin wrapper over the Anthropic Messages API."""

    model: str = DEFAULT_MODEL
    max_tokens: int = 16_000
    effort: str = "high"
    include_evidence: bool = False
    timeout: float = 600.0

    # ------------------------------------------------------------ availability

    @staticmethod
    def dependency_available() -> tuple:
        try:
            import anthropic  # noqa: F401
        except ImportError:
            return False, "the 'anthropic' package is not installed (pip install anthropic)"
        return True, ""

    @staticmethod
    def credentials_available() -> tuple:
        if os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_AUTH_TOKEN"):
            return True, ""
        # The SDK also resolves an `ant auth login` profile; let it try.
        config_home = os.getenv("XDG_CONFIG_HOME") or os.path.expanduser("~/.config")
        if os.path.isdir(os.path.join(config_home, "anthropic")):
            return True, ""
        return False, (
            "no Anthropic credentials found (set ANTHROPIC_API_KEY, or run `ant auth login`)"
        )

    # ------------------------------------------------------------------- review

    def review(
        self,
        *,
        architecture_documents: Dict[str, str],
        architecture_manifest: Optional[Dict[str, Any]],
        deterministic_findings: Sequence[Finding],
        target_description: str,
    ) -> AiReviewResult:
        try:
            import anthropic
        except ImportError as exc:  # pragma: no cover - guarded by dependency_available
            raise AiReviewError("the 'anthropic' package is not installed") from exc

        prompt = self._build_prompt(
            architecture_documents=architecture_documents,
            architecture_manifest=architecture_manifest,
            deterministic_findings=deterministic_findings,
            target_description=target_description,
        )
        client = anthropic.Anthropic(timeout=self.timeout)
        payload: Dict[str, Any] = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system": SYSTEM_PROMPT,
            "thinking": {"type": "adaptive"},
            "messages": [{"role": "user", "content": prompt}],
            "output_config": {
                "effort": self.effort,
                "format": {"type": "json_schema", "schema": FINDING_SCHEMA},
            },
        }

        try:
            response = client.messages.create(**payload)
        except anthropic.BadRequestError as exc:
            # Older models and some deployments reject structured output or
            # adaptive thinking; retry with a plain request and parse the text.
            response = self._retry_plain(client, anthropic, prompt, exc)
        except anthropic.AuthenticationError as exc:
            raise AiReviewError(f"Anthropic authentication failed: {exc}") from exc
        except anthropic.APIStatusError as exc:
            raise AiReviewError(f"Anthropic API error ({exc.status_code}): {exc}") from exc
        except anthropic.APIConnectionError as exc:
            raise AiReviewError(f"could not reach the Anthropic API: {exc}") from exc

        if getattr(response, "stop_reason", None) == "refusal":
            raise AiReviewError(
                "the model declined to complete this review "
                f"({getattr(getattr(response, 'stop_details', None), 'category', 'unspecified')})"
            )

        text = "".join(block.text for block in response.content if block.type == "text")
        data = _parse_json(text)
        usage = getattr(response, "usage", None)
        return AiReviewResult(
            summary=str(data.get("summary", "")).strip(),
            findings=[
                finding
                for entry in data.get("findings", []) or []
                if (finding := self._to_finding(entry)) is not None
            ],
            unanswered_questions=[str(q) for q in data.get("unanswered_questions", []) or []],
            model=getattr(response, "model", self.model),
            input_tokens=getattr(usage, "input_tokens", 0) if usage else 0,
            output_tokens=getattr(usage, "output_tokens", 0) if usage else 0,
        )

    def _retry_plain(self, client: Any, anthropic: Any, prompt: str, original: Exception) -> Any:
        instruction = (
            f"{prompt}\n\nRespond with a single JSON object matching this schema, and nothing "
            f"else:\n{json.dumps(FINDING_SCHEMA)}"
        )
        try:
            return client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": instruction}],
            )
        except anthropic.APIStatusError as exc:
            raise AiReviewError(
                f"Anthropic request failed (structured attempt: {original}; plain attempt: {exc})"
            ) from exc

    # ------------------------------------------------------------------ prompt

    def _build_prompt(
        self,
        *,
        architecture_documents: Dict[str, str],
        architecture_manifest: Optional[Dict[str, Any]],
        deterministic_findings: Sequence[Finding],
        target_description: str,
    ) -> str:
        sections: List[str] = [f"# Target under assessment\n{target_description}"]

        if architecture_manifest:
            sections.append(
                "# Structured architecture manifest\n```json\n"
                + json.dumps(architecture_manifest, indent=2, default=str)[:MAX_DOCUMENT_CHARS]
                + "\n```"
            )
        else:
            sections.append(
                "# Structured architecture manifest\nNot supplied. Note this as a gap if it "
                "limits your review."
            )

        if architecture_documents:
            budget = MAX_DOCUMENT_CHARS // max(1, len(architecture_documents))
            document_blocks = []
            for path, text in architecture_documents.items():
                truncated = text[:budget]
                suffix = "\n... [truncated]" if len(text) > budget else ""
                document_blocks.append(f"## {path}\n```\n{truncated}{suffix}\n```")
            sections.append("# Architecture documents\n" + "\n\n".join(document_blocks))
        else:
            sections.append("# Architecture documents\nNone supplied.")

        sections.append(self._findings_section(deterministic_findings))
        sections.append(
            "# Your task\n"
            "Review the material above and report what the deterministic scanners could not "
            "establish. Follow the rules in your instructions. Return the JSON object described "
            "by the output schema."
        )
        return "\n\n".join(sections)

    def _findings_section(self, findings: Sequence[Finding]) -> str:
        if not findings:
            return (
                "# Deterministic scanner findings\n"
                "None reported. Consider whether that reflects a genuinely clean result or "
                "missing scanner coverage."
            )
        lines = []
        for finding in list(findings)[:MAX_FINDINGS_IN_PROMPT]:
            line = (
                f"- [{finding.id}] {finding.severity.value.upper()} ({finding.layer.value}, "
                f"{finding.source}): {finding.title} @ {finding.location.describe()}"
            )
            if self.include_evidence and finding.evidence:
                line += f"\n    evidence: {finding.evidence[:500]}"
            lines.append(line)
        omitted = max(0, len(findings) - MAX_FINDINGS_IN_PROMPT)
        footer = f"\n({omitted} further findings omitted for length.)" if omitted else ""
        note = (
            ""
            if self.include_evidence
            else "\n\nFinding evidence was withheld from this prompt by policy; reason from the "
            "titles, locations and sources."
        )
        return "# Deterministic scanner findings\n" + "\n".join(lines) + footer + note

    # ----------------------------------------------------------------- mapping

    def _to_finding(self, entry: Dict[str, Any]) -> Optional[Finding]:
        title = str(entry.get("title", "")).strip()
        if not title:
            return None
        try:
            layer = Layer(str(entry.get("layer", "architecture")).lower())
        except ValueError:
            layer = Layer.ARCHITECTURE
        location_text = str(entry.get("location", "")).strip()
        location = (
            Location(url=location_text)
            if location_text.startswith(("http://", "https://"))
            else Location(component=location_text or None)
        )
        builds_on = [str(item) for item in entry.get("builds_on_finding_ids", []) or []]
        explanation = str(entry.get("explanation", "")).strip()
        if builds_on:
            explanation = f"{explanation}\n\nBuilds on deterministic finding(s): {', '.join(builds_on)}"

        return Finding(
            source="ai-advisory",
            layer=layer,
            severity=Severity.parse(entry.get("severity"), Severity.LOW),
            title=title,
            explanation=explanation,
            evidence=str(entry.get("evidence", "")).strip(),
            location=location,
            remediation=str(entry.get("remediation", "")).strip(),
            rule_id="ai-advisory/reasoning",
            tags=["ai-advisory", "requires-human-triage"],
            confidence=str(entry.get("confidence", "medium")).lower(),
            ai_generated=True,
        )


def _parse_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise AiReviewError("the model returned an empty response")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError as exc:
                raise AiReviewError(f"the model response was not valid JSON: {exc}") from exc
        raise AiReviewError("the model response contained no JSON object")
