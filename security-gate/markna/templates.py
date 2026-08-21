"""Scaffolding written by ``markna init``.

Kept as module constants so the templates ship with the package and can be
written anywhere without packaging data files.
"""

from __future__ import annotations

CONFIG_TEMPLATE = """\
# MARKNA Security Gate — run configuration.
# Every value here can be overridden by a command-line flag.
#
#   markna assess --config markna.yaml

project: .

architecture:
  # Free-text design documents (Markdown / reStructuredText / plain text).
  documents:
    - docs/architecture.md
  # Structured manifest — this is what makes the architecture layer deterministic.
  manifest: architecture.yaml

environment:
  # The permitted UAT/demo URL. Leave empty to skip the environment layer.
  url: ""
  authorization:
    # Required before any traffic is sent. Record who permitted the test.
    authorized_by: ""
    reference: ""            # change ticket / engagement reference
    expires: ""              # YYYY-MM-DD; the gate refuses to run after this date
    scope_hosts: []          # extra hosts in scope (the target host is always included)
    allow_private_targets: false   # set true for an internal UAT host (RFC1918/loopback)
    note: ""

layers:
  - architecture
  - code
  - environment

policy: markna-policy.yaml

reports:
  directory: markna-reports
  formats:
    - json
    - markdown
    - html
  include_raw: false         # embeds raw scanner output; may contain unredacted matches

# Per-scanner settings. Run `markna scanners` for the registry.
scanners:
  semgrep:
    # Set true when the runner can reach semgrep.dev; adds several thousand rules
    # on top of the bundled offline baseline.
    use_registry: false
    registry_packs:
      - p/default
      - p/owasp-top-ten
  zap-baseline:
    enabled: true
    spider_minutes: 2
  ai-advisory:
    # The AI layer is advisory: it cannot block a release under the default policy.
    enabled: false
    model: claude-opus-5
    # Finding evidence may contain secrets or customer data. Off by default.
    include_evidence: false
"""


POLICY_TEMPLATE = """\
# MARKNA Security Gate — release policy.
#
# This file decides what blocks a release. Keep it in version control next to the
# project it gates, and treat changes to it as security-relevant changes.

name: markna-default-v1
description: Default pre-UAT security gate policy.

# Severities that block the release.
block_on:
  - critical
  - high

# Severities that produce WARN rather than BLOCK.
warn_on:
  - medium
  - low

# An AI finding is a reasoning aid, not evidence. Leave this false unless you
# have a specific reason and a human in the loop.
ai_findings_can_block: false

# Deterministic evidence that must exist before the gate can vouch for a layer.
# A missing capability becomes a finding at coverage_gap_severity.
required_capabilities:
  architecture:
    - architecture-review
  code:
    - sast
    - dependency-vulnerabilities
    - secrets
    - sbom
  environment:
    - transport-security
    - http-security-headers

coverage_gap_severity: high
scanner_error_severity: medium
max_findings_per_scanner: 1000

# Re-rate specific findings. Use sparingly and always with a reason.
severity_overrides: []
#  - match:
#      source: bandit
#      rule_id: B101
#    severity: low
#    reason: "assert usage in test helpers only"

# Suppress findings that have been reviewed and accepted. An expired suppression
# is ignored, so the finding comes back for re-review.
suppressions: []
#  - match:
#      fingerprint: 0123456789abcdef
#    reason: "false positive: the value is a fixture, confirmed by A. Reviewer"
#    expires: 2026-12-31
"""


ARCHITECTURE_TEMPLATE = """\
# MARKNA Security Gate — architecture manifest.
#
# Free-text documents cannot be checked mechanically; this file can. Every field
# below feeds a deterministic rule, so an honest "none" here produces a finding
# rather than silence. Fill it in with what is true, not with what should be true.

system:
  name: My System
  version: "1.0"
  owner: team-or-person
  classification: internal        # public | internal | confidential | restricted
  description: One paragraph on what the system does and for whom.

# Where trust changes hands, and what is enforced at each crossing.
trust_boundaries:
  - id: tb-public
    name: Internet to application
    description: Untrusted clients reaching the public endpoint.
    controls:
      - tls-1.3
      - authentication
      - rate-limiting

components:
  - id: api
    name: Application API
    type: service                 # service | datastore | queue | job | client | external
    trust_zone: dmz
    authentication: oauth2        # none | api-key | basic | session | jwt | oauth2 | mtls
    authorization: rbac           # none | rbac | abac | ownership | tenant-scoped
    internet_facing: true
    handles_sensitive_data: true

data_flows:
  - id: df-client-api
    name: Client to API
    source: client
    destination: api
    data: [credentials, user-content]
    classification: confidential  # public | internal | confidential | restricted
    crosses_boundary: tb-public
    encryption_in_transit: tls1.3 # none | tls1.2 | tls1.3 | mtls | vpn
    authentication: oauth2

data_stores:
  - id: primary-db
    name: Primary database
    classification: confidential
    contains_pii: true
    encryption_at_rest: aes-256   # none | aes-256 | provider-managed | field-level
    backup: daily
    retention: 90 days

integrations:
  - id: payments
    name: Payment provider
    provider: Example Payments Ltd
    data_shared: [order-total, customer-reference]
    authentication: api-key
    transport: tls1.2
    data_processing_agreement: DPA-2026-014

secrets:
  management: secret-manager      # secret-manager | vault | kms | env-var | env-file | hardcoded | unknown
  rotation: 90 days               # or "none"
  storage_locations:
    - AWS Secrets Manager (production)

exposure:
  - id: public-api
    component: api
    url: https://uat.example.com
    environment: uat              # production | uat | demo | staging | dev
    network: internet             # internet | vpn | lan | localhost
    authentication: oauth2
    tls: true
    waf: true

logging:
  security_events: true           # authn outcomes, authz denials, admin actions
  centralised: true
  pii_redaction: true
  retention: 365 days

threat_model:
  performed: true
  methodology: STRIDE
  date: "2026-08-01"
  reviewed_by: security-team
  threats:
    - id: T-01
      stride: spoofing
      description: Attacker replays a captured bearer token.
      mitigation: Short token lifetime plus refresh-token rotation.
      status: mitigated           # mitigated | accepted | planned | open
      owner: api-team
    - id: T-02
      stride: tampering
      description: Request body modified in transit.
      mitigation: TLS 1.3 everywhere; no plaintext listener.
      status: mitigated
      owner: platform-team
    # Cover every STRIDE category: spoofing, tampering, repudiation,
    # information_disclosure, denial_of_service, elevation_of_privilege.
"""


WORKFLOW_TEMPLATE = """\
# Example CI wiring for the MARKNA Security Gate.
#
# The code and architecture layers run on every pull request. The environment
# layer runs only where an authorised UAT URL exists, because it sends traffic to
# a live system.
name: security-gate

on:
  pull_request:
  workflow_dispatch:

jobs:
  markna:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      security-events: write      # for the SARIF upload below
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install the gate and its scanners
        run: |
          pip install ./security-gate
          pip install -r security-gate/requirements-scanners.txt

      - name: Run the security gate
        run: |
          markna assess \\
            --project . \\
            --arch docs/architecture.md \\
            --arch-manifest architecture.yaml \\
            --layers architecture,code \\
            --policy security-gate/markna-policy.yaml \\
            --format json,markdown,sarif \\
            --out markna-reports

      - name: Publish findings to code scanning
        if: always()
        uses: github/codeql-action/upload-sarif@v3
        with:
          sarif_file: markna-reports/markna-report.sarif

      - name: Keep the report with the build
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: markna-report
          path: markna-reports/
"""


TEMPLATES = {
    "markna.yaml": CONFIG_TEMPLATE,
    "markna-policy.yaml": POLICY_TEMPLATE,
    "architecture.yaml": ARCHITECTURE_TEMPLATE,
    "markna-ci-example.yml": WORKFLOW_TEMPLATE,
}
