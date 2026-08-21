"""SARIF 2.1.0 output, for code-scanning dashboards and CI annotations."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from ..models import Assessment, Finding, Severity

SARIF_VERSION = "2.1.0"
SARIF_SCHEMA = "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json"

_LEVELS = {
    Severity.CRITICAL: "error",
    Severity.HIGH: "error",
    Severity.MEDIUM: "warning",
    Severity.LOW: "note",
    Severity.INFO: "note",
}


def render_sarif(assessment: Assessment) -> str:
    findings = assessment.active_findings
    rules: Dict[str, Dict[str, Any]] = {}
    results: List[Dict[str, Any]] = []

    for finding in findings:
        rule_id = finding.rule_id or f"{finding.source}/unspecified"
        if rule_id not in rules:
            rules[rule_id] = _rule(rule_id, finding)
        results.append(_result(rule_id, finding))

    return json.dumps(
        {
            "$schema": SARIF_SCHEMA,
            "version": SARIF_VERSION,
            "runs": [
                {
                    "tool": {
                        "driver": {
                            "name": "MARKNA Security Gate",
                            "version": assessment.tool_version or "1.0.0",
                            "informationUri": "https://example.invalid/markna-security-gate",
                            "rules": list(rules.values()),
                        }
                    },
                    "invocations": [
                        {
                            "executionSuccessful": True,
                            "startTimeUtc": assessment.started_at,
                            "endTimeUtc": assessment.finished_at,
                            "properties": {
                                "verdict": assessment.verdict.value,
                                "policy": assessment.policy_name,
                                "assessmentId": assessment.assessment_id,
                            },
                        }
                    ],
                    "results": results,
                }
            ],
        },
        indent=2,
        ensure_ascii=False,
        default=str,
    )


def _rule(rule_id: str, finding: Finding) -> Dict[str, Any]:
    return {
        "id": rule_id,
        "name": rule_id.replace("/", "-"),
        "shortDescription": {"text": finding.title[:200]},
        "fullDescription": {"text": (finding.explanation or finding.title)[:2000]},
        "help": {"text": finding.remediation or "See the MARKNA report for remediation."},
        "properties": {
            "layer": finding.layer.value,
            "source": finding.source,
            "tags": [finding.layer.value, *finding.tags, *finding.cwe],
            "security-severity": _security_severity(finding.severity),
            "aiGenerated": finding.ai_generated,
        },
        "defaultConfiguration": {"level": _LEVELS[finding.severity]},
    }


def _result(rule_id: str, finding: Finding) -> Dict[str, Any]:
    location: Dict[str, Any] = {}
    if finding.location.file:
        region: Dict[str, Any] = {}
        if finding.location.line:
            region["startLine"] = int(finding.location.line)
            if finding.location.end_line and finding.location.end_line >= finding.location.line:
                region["endLine"] = int(finding.location.end_line)
        location = {
            "physicalLocation": {
                "artifactLocation": {"uri": _uri(finding.location.file)},
                **({"region": region} if region else {}),
            }
        }
    else:
        location = {
            "logicalLocations": [
                {
                    "name": finding.location.describe(),
                    "kind": "resource" if finding.location.url else "component",
                }
            ]
        }

    message = finding.explanation.strip() or finding.title
    if finding.ai_generated:
        message = f"[AI ADVISORY — not deterministic evidence] {message}"

    return {
        "ruleId": rule_id,
        "level": _LEVELS[finding.severity],
        "message": {"text": message[:4000]},
        "locations": [location],
        "partialFingerprints": {"marknaFingerprint": finding.fingerprint},
        "properties": {
            "marknaId": finding.id,
            "severity": finding.severity.value,
            "blocking": finding.blocking,
            "aiGenerated": finding.ai_generated,
            "confidence": finding.confidence,
            "evidence": finding.evidence[:2000],
            "timestamp": finding.timestamp,
            "url": finding.location.url,
        },
    }


def _uri(path: str) -> str:
    normalised = path.replace("\\", "/")
    return normalised[2:] if normalised.startswith("./") else normalised


def _security_severity(severity: Severity) -> str:
    """GitHub code scanning reads this numeric score to bucket alerts."""
    return {
        Severity.CRITICAL: "9.5",
        Severity.HIGH: "8.0",
        Severity.MEDIUM: "5.5",
        Severity.LOW: "3.0",
        Severity.INFO: "0.0",
    }[severity]
