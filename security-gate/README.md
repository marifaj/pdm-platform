# MARKNA Security Gate

A server-hosted web application that runs an independent security review of a
software project before it reaches UAT or production, across three layers:

| Layer | What it assesses | How |
| --- | --- | --- |
| **Architecture** | Trust boundaries, authn/authz, sensitive-data flows, external integrations, secrets handling, deployment exposure, threat model | A deterministic rule engine over a structured architecture manifest, plus a topic-coverage checklist and stated-risk extraction over the design documents |
| **Code** | SAST, dependency vulnerabilities, secrets, SBOM, insecure configuration, common OWASP weaknesses | Mature open-source scanners (Semgrep, Bandit, Gitleaks, detect-secrets, Trivy, pip-audit, OSV-Scanner, npm audit, Checkov, Syft, cyclonedx-py) |
| **Environment** | TLS and certificates, security headers, cookie attributes, CORS, exposed paths, enabled HTTP methods, passive DAST | Read-only probes from the standard library, plus an OWASP ZAP baseline scan |

It is two processes over one database: a web process that serves the UI and the
JSON API, and a worker process that actually runs the scanners. Queueing a run
writes a row; the web process never executes a scanner.

**v1.0 is single-organisation and internal** — no billing, no signup, no tenant
selection. The data model, the access checks and the process boundaries are
nonetheless tenant-shaped throughout, so those options stay open. See
[`docs/architecture.md`](docs/architecture.md) for the boundaries and for what
productisation would actually involve.

Every run ends in one of three verdicts:

```
PASS    no findings at or above the warn threshold, and every required
        deterministic capability was actually exercised
WARN    findings that need triage but do not block release under this policy
BLOCK   at least one finding blocks release under this policy
```

## The two rules that shape everything else

**1. An AI review is not security evidence.** The AI layer reads the
architecture material and the deterministic results and reasons about what a
pattern matcher cannot see — a control the design claims but a finding
contradicts, a data flow nobody modelled, two minor issues that chain. Its
findings are labelled `ai_generated`, they say so in every report format, and
under the default policy they **cannot block a release**. They are input to a
human reviewer, not a substitute for one.

**2. A scanner that did not run found nothing — which is not the same as there
being nothing to find.** The policy declares which deterministic capabilities
each layer requires. If no scanner provided one, MARKNA emits a coverage-gap
finding at `high` and blocks. A gate that quietly skipped SAST cannot honestly
return PASS.

## Install

```bash
pip install ./security-gate                        # engine + server
pip install -r security-gate/requirements-scanners.txt   # the pip-installable scanners
```

Binary scanners (Gitleaks, Trivy, Syft, OSV-Scanner) are found on `PATH`, or
point at them with `--tool-path DIR` / the `MARKNA_TOOL_PATH` environment
variable. Check what the runner actually has:

```bash
markna scanners
```

## Running the server

```bash
markna-server initdb
markna-server bootstrap --org-slug acme --org-name "Acme" \
  --admin-email security@acme.example --admin-password '…'

# web process — a plain WSGI callable, so any WSGI server works
gunicorn -w 4 -b 127.0.0.1:8080 markna_server.app:application
# ...or, for local work only:
markna-server serve

# worker process — one or more; this is where scanners run
markna-server worker
```

Then sign in, create a project, and queue an assessment. A project points at a
source path, architecture documents and a manifest — all resolved inside a
configured `workspace_root` and refused if they escape it — plus any number of
authorised environment targets.

```bash
# The same thing over the API
TOKEN=$(markna-server token create --name ci | sed -n 2p)
curl -sX POST localhost:8080/api/v1/projects/$PROJECT/runs \
  -H "Authorization: Bearer $TOKEN" -d '{"layers":["architecture","code"]}'
# -> 202 {"status":"queued", ...}
```

`markna-server status` shows configuration, projects and queue state;
`markna-server --help` lists user, token, project and policy management.

### Concepts

| Concept | What it is |
| --- | --- |
| **Organization** | The tenant boundary. Every other record belongs to exactly one. |
| **Project** | One assessed system: source, architecture material, targets, policy. |
| **AssessmentRun** | One execution against one project under one policy version. Also the queue entry. |
| **Finding** | One observation belonging to a run, in the engine's schema. |
| **Policy** | Versioned release rules. Project overrides organisation overrides engine default. |
| **Report** | A rendered artefact of a run, in one format. |

Access is role-based (`viewer`, `maintainer`, `admin`) and every service call
resolves its tenant scope from the caller's principal — no endpoint accepts an
`organization_id`. A cross-tenant lookup returns 404, never 403.

Authentication is a protocol with a registry: API tokens and browser sessions
ship in v1.0, and a third provider trusts identity headers from an
authenticating reverse proxy — which is how an Entra ID deployment works today
without changing any application code. Adding a native OIDC provider is one
class and one registry entry; the `users` table already carries `subject` and
`issuer` columns for exactly that.

## Using the engine directly

The scanner engine is a standalone CLI, unchanged and independent of the server
— useful in CI, or for a one-off assessment:

```bash
markna init                    # writes markna.yaml, markna-policy.yaml, architecture.yaml
$EDITOR architecture.yaml      # describe the system as it actually is
markna assess --config markna.yaml
```

Or drive it entirely from flags:

```bash
# Architecture and code
markna assess \
  --project . \
  --arch docs/architecture.md \
  --arch-manifest architecture.yaml \
  --format json,markdown,sarif \
  --out markna-reports

# The deployed UAT environment (requires recorded authorisation — see below)
markna assess \
  --url https://uat.example.com \
  --layers environment \
  --authorized-by "J. Smith, Platform Lead" \
  --auth-reference CHG-4821 \
  --auth-expires 2026-09-30
```

Try it against the bundled insecure example:

```bash
markna assess \
  --project security-gate/examples/vulnerable-demo \
  --arch security-gate/examples/vulnerable-demo/docs/architecture.md \
  --arch-manifest security-gate/examples/vulnerable-demo/architecture.yaml \
  --out /tmp/markna-demo
```

## Findings

Every finding — from every scanner, in every layer — is normalised to the same
shape:

```json
{
  "id": "MK-COD-3F2A91B8C4",
  "source": "semgrep",
  "layer": "code",
  "severity": "critical",
  "title": "shell=True with a non-literal command allows OS command injection",
  "explanation": "...why this matters for this system...",
  "evidence": "29| subprocess.check_output(\"generate-report \" + name, shell=True)",
  "location": { "file": "app.py", "line": 29, "url": null, "component": null },
  "remediation": "Pass the command as an argument list and leave shell=False.",
  "blocking": true,
  "timestamp": "2026-08-21T13:44:02Z",
  "fingerprint": "3f2a91b8c4d5e6f7",
  "rule_id": "markna-py-subprocess-shell-true",
  "cwe": ["CWE-78"],
  "confidence": "high",
  "ai_generated": false
}
```

`id` is derived from `fingerprint`, which is derived from the scanner, rule,
location and normalised evidence — so the same issue keeps the same id between
runs, and a policy suppression written against it keeps working.

Evidence passes through a redactor before it reaches any report: credential
formats, `KEY=value` assignments and URL-embedded passwords come out as
`[REDACTED]`. A secrets scanner that printed the secret it found would just be
moving the secret somewhere new.

## Reports

`--format json,markdown,sarif,html` (default `json,markdown`):

- **json** — the complete record, and the only lossless format. Includes the
  scanner execution log and the coverage table.
- **markdown** — the human review document.
- **sarif** — SARIF 2.1.0, for GitHub code scanning and CI annotations.
- **html** — a self-contained page for a release record.

## Policy

`markna-policy.yaml` decides what blocks. Keep it in version control next to the
project it gates.

```yaml
name: markna-default-v1
block_on: [critical, high]
warn_on: [medium, low]
ai_findings_can_block: false

required_capabilities:
  architecture: [architecture-review]
  code: [sast, dependency-vulnerabilities, secrets, sbom]
  environment: [transport-security, http-security-headers]

coverage_gap_severity: high

severity_overrides:
  - match: { source: bandit, rule_id: B101 }
    severity: low
    reason: "assert usage in test helpers only"

suppressions:
  - match: { fingerprint: 0123456789abcdef }
    reason: "false positive: fixture value, confirmed by A. Reviewer"
    expires: 2026-12-31      # an expired suppression is ignored, so it comes back for review
```

`markna capabilities` lists the capability vocabulary that
`required_capabilities` accepts.

### The floor a policy cannot lower

A policy tunes the gate; it cannot switch it off. Some requirements are
structural and are restored whichever way the file is written:

| Floor | Effect |
| --- | --- |
| `architecture: [architecture-review]`, `code: [sast, secrets]`, `environment: [transport-security]` | Restored into `required_capabilities` if a policy drops them. |
| `critical` in `block_on` | Restored if a policy removes it. |
| Floor findings | Not suppressible and not re-rateable by `severity_overrides`. |
| Zero mandatory capabilities evaluated | `BLOCK`. An assessment that proves nothing cannot pass. |
| A configured layer that was not assessed | Surfaced as a finding, so the gap is visible in the report. |

An attempt to lower the floor is not silently ignored — it is recorded as a
`policy/floor-restored` finding, so the report shows both what was requested and
what the gate insisted on. This means the worst a mis-written or hostile policy
can do is make the gate noisier, never quieter.

## Architecture layer

Free-text documents cannot be checked mechanically, so MARKNA reads both:

**`architecture.yaml`** — a structured manifest (components, trust boundaries,
data flows, data stores, integrations, secrets, exposure, logging, threat
model). Fixed rules run over it: an internet-facing component with no
authentication is `critical`; a data flow crossing a trust boundary with no
encryption is `high`; a PII store with no encryption at rest is `high`; secrets
managed as `env-file` is `high`; every STRIDE category with no enumerated threat
is `low`. Every finding cites the manifest field it came from. `markna init`
writes a commented template.

**Design documents** (`--arch`, repeatable, Markdown/reST/plain text) — a
thirteen-topic coverage checklist (trust boundaries, authentication,
authorization, sensitive data, data flows, encryption, secrets, integrations,
network exposure, threat model, logging, backup, input validation) plus
stated-risk extraction, which lifts out phrases where the design already admits
a weakness — "no authentication", `http://`, "self-signed", "runs as root" —
with the file and line.

An honest `none` in the manifest produces a finding. That is the point: the gate
reviews what is true, and silence is not an answer.

## Code layer

MARKNA drives existing tools rather than reimplementing them. Each adapter
normalises output, maps severity, and preserves the tool's own rule id.

| Capability | Scanners (preference order) |
| --- | --- |
| SAST | Semgrep, Bandit |
| Dependency vulnerabilities | Trivy, OSV-Scanner, pip-audit, npm audit |
| Secrets | Gitleaks, detect-secrets, Trivy, bundled regex fallback |
| SBOM | Syft, cyclonedx-py |
| Insecure configuration | Trivy, Checkov |

**Semgrep offline.** When `semgrep.dev` is unreachable — air-gapped runners,
egress proxies — MARKNA falls back to the ruleset bundled in
`markna/rules/semgrep/`: 26 rules across Python and JavaScript/TypeScript for
injection, deserialisation, weak crypto, disabled TLS verification, debug mode,
JWT handling, insecure cookies and CORS, each carrying CWE and OWASP metadata.
When the registry *is* reachable, set `use_registry: true` and get the full
packs on top.

**The secrets fallback** only runs when no dedicated secrets scanner is
installed, and says so in an INFO finding — regex patterns without git-history
scanning are weak evidence, and the report should say which engine produced the
result.

Semgrep applies its own ignore list (test directories, vendored code), so
MARKNA records how many files were actually analysed as an INFO finding. A "no
findings" result means nothing without knowing what was looked at.

## Environment layer

This layer sends traffic to a running system, so it is gated:

- **Authorisation is mandatory.** `--authorized-by` (plus optional
  `--auth-reference` and `--auth-expires`) is recorded in the report. An expired
  authorisation refuses to run.
- **Scope is enforced on every request, including redirects.** Only the target
  host and any `--scope-host` you add are reachable; a redirect out of scope
  aborts the scanner rather than following it.
- **Private and loopback targets are refused** unless
  `--allow-private-targets` is set, so a typo cannot point the gate at an
  internal host by accident.
- **Requests are read-only.** GET, HEAD and OPTIONS only, no request bodies, no
  fuzzing, no credential guessing, rate-limited, and identified in the
  User-Agent so the team running the environment can see what hit their logs.
  Risky HTTP methods are detected from the advertised `Allow` header; they are
  never invoked.

The exposure scanner baselines against a path that cannot exist before probing,
so applications that answer 200 for everything do not produce a page of false
positives.

The ZAP baseline scan is **passive only** — it spiders and reports what its
passive rules observe, with no active attack rules. It needs Docker (or
`zap-baseline.py` on `PATH`); without it, MARKNA reports the DAST gap.

Exercise the whole layer safely against the bundled demo target:

```bash
python3 security-gate/examples/insecure-uat-server.py --port 8099 &
markna assess --url http://127.0.0.1:8099 --layers environment \
  --authorized-by "you" --allow-private-targets --out /tmp/markna-env
```

## AI advisory layer

Off by default. Enable with `--ai` (needs `pip install anthropic` and
`ANTHROPIC_API_KEY`, or an `ant auth login` profile):

```bash
markna assess --config markna.yaml --ai --ai-model claude-opus-5
```

It runs **after** the deterministic scanners and sees their results. What it
sends to the API: the architecture documents, the manifest, and the *titles and
locations* of deterministic findings. What it does not send: finding evidence
(which may contain secrets or customer data) and source code. Set
`include_evidence: true` under the `ai-advisory` scanner settings to opt in.

Its output includes an explicit list of questions the supplied material could
not answer — each one a gap in the evidence base, recorded as an INFO finding.

## CI

Exit codes are the contract:

| Code | Meaning |
| --- | --- |
| `0` | PASS, or WARN unless `--fail-on warn` |
| `1` | WARN with `--fail-on warn` |
| `2` | BLOCK |
| `3` | the assessment could not be run (configuration or setup error) |

`markna init` writes `markna-ci-example.yml`, a GitHub Actions workflow that
runs the architecture and code layers on every pull request and uploads the
SARIF to code scanning. Run the environment layer only where an authorised UAT
URL exists.

## What this is not

- **Not a penetration test.** The environment layer is read-only reconnaissance
  and passive DAST. It will not find business-logic flaws, authorisation bypass
  chains, or anything requiring authenticated interaction.
- **Not a replacement for review.** WARN means a human has to look. So does
  every AI advisory finding.
- **Not a proof of absence.** A PASS says the configured scanners ran and found
  nothing at or above the threshold. The coverage table in every report says
  exactly which classes of risk were examined; read it.

## Development

```bash
cd security-gate
pip install -e ".[dev]"
pytest                     # 405 tests; the scanner-integration tests skip
                           # without semgrep/bandit, everything else needs neither
```

The suite covers the finding model, the policy engine's verdict and coverage
logic, redaction, both architecture engines, the environment scanners (against a
local throwaway HTTP server), all four report formats, the CLI exit-code
contract, tenant isolation, the HTTP surfaces end to end, and the queue.

Security-acceptance regression suites, each written from the attacker's side:

| File | Property |
| --- | --- |
| `test_gate_floor.py` | No policy can produce an unconditional `PASS`. |
| `test_confinement.py` | A repository symlink never leads the gate out of the workspace. |
| `test_server_auth_hardening.py` | Multi-tenant login, token privilege, throttling, uniform failure. |
| `test_http_hardening.py` | Malformed `Content-Length` and body limits, over a real socket. |
| `test_execution_hardening.py` | Connect-time destination checks; scanner environment allow-list. |
| `test_scanner_integration.py` | Semgrep and Bandit really execute against the vulnerable fixture. |

`test_scanner_integration.py` skips when the binaries are absent. A skip is a
coverage gap, not a pass — run it with `-rs` in CI and treat skips as failures
on the machine that gates releases.

`tests/test_layering.py` enforces the architecture rather than describing it: it
parses the import graph and fails if a user-facing module reaches the scanner
engine, if the domain learns how it is stored, if a tenant-scoped repository
method loses its scope parameter, or if any module hard-codes an organisation or
project id.

Layout:

```
markna/                    the scanner engine — standalone, no server imports
├── models.py              Finding / ScannerRun / Assessment, ids and fingerprints
├── policy.py              verdict, coverage requirements, suppressions, overrides
├── runner.py              orchestration; isolates scanner failures
├── authorization.py       environment authorisation and scope enforcement
├── http.py                read-only, scope-checked HTTP client
├── redact.py              credential redaction for evidence
├── exec.py                bounded subprocess execution; allow-listed environment
├── confinement.py         workspace confinement: traversal, reads, reporting
├── scanners/              one adapter per tool; each declares its capabilities
├── rules/semgrep/         bundled offline SAST ruleset
├── ai/                    Anthropic reasoning layer (advisory)
└── report/                json / markdown / sarif / html

markna_server/             the web application
├── domain/                entities, ids, errors — no I/O
├── identity/              principals, roles, pluggable authentication, access control
├── storage/               repository protocols + SQLite implementation + schema
├── service.py             application services; the only place access is enforced
├── execution/             the queue worker and the engine adapter (separate process)
├── api/                   JSON API v1 and the WSGI core
├── web/                   server-rendered UI (no JavaScript, strict CSP)
├── config.py              configuration and workspace path confinement
├── app.py                 the WSGI application
└── cli.py                 provisioning, serve, worker
```

Adding a scanner: subclass `Scanner`, declare `layer` and `capabilities`,
implement `applicable`, `available` and `scan`, decorate with `@register`, and
add the module to the list in `scanners/__init__.py`. Return `Finding` objects —
never set `blocking`; that is the policy engine's decision.
