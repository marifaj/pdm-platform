# MARKNA Security Gate — architecture

Project 778. A server-hosted web application that runs an independent security
review of a software project before it reaches UAT or production.

**v1.0 scope: single organisation, internal.** There is no billing, no
self-service signup, no customer portal and no tenant-selection UI, and this
document does not propose adding any. What it does do is record the boundaries
that keep those options open, and the reasoning behind each one, so that a later
decision to productise is an extension rather than a rewrite. The last section
lists what productisation would actually involve; none of it is implemented.

---

## 1. Shape of the system

Two processes over one database:

```
                    ┌──────────────────────────────┐
   browser  ───────▶│  web process (WSGI)          │
   CI / API ───────▶│  markna_server.app           │
                    │    api/   web/   service/    │
                    └───────────────┬──────────────┘
                                    │ writes a queued run row
                                    ▼
                    ┌──────────────────────────────┐
                    │  database (SQLite in v1.0)   │
                    └───────────────┬──────────────┘
                                    │ claims the row
                                    ▼
                    ┌──────────────────────────────┐
                    │  worker process              │
                    │  markna_server.execution     │
                    │    └── markna (engine)       │──▶ scanners, UAT target
                    └──────────────────────────────┘
```

The web process never runs a scanner. It validates a request, writes a row and
returns 202. A worker claims the row, runs the engine, and writes back findings,
coverage, scanner-execution records and rendered reports.

Layering inside the server package, outermost first:

| Layer | Package | Depends on | Never depends on |
| --- | --- | --- | --- |
| HTTP | `api/`, `web/` | `service` | engine, `execution`, storage internals |
| Application | `service.py` | `domain`, `storage`, `identity` | engine, `execution`, HTTP |
| Domain | `domain/` | nothing | everything above |
| Storage | `storage/` | `domain`, `identity` | HTTP, `service` |
| Identity | `identity/` | `domain` | HTTP |
| Execution | `execution/` | `domain`, `storage`, **engine** | HTTP |

`tests/test_layering.py` enforces every row of that table by parsing the import
graph, so a violation fails the build rather than surviving review.

---

## 2. The six concepts

They are separate tables, separate entities and separate services. Conflating
any two is what makes an internal tool impossible to productise later.

### Organization
The tenant boundary. Every other record carries `organization_id`. v1.0
provisions exactly one, through `markna-server bootstrap`; the rule that there
is only one lives in that command and in the login page, not in the data model
(`tests/test_layering.py::TestNoHardcodedTenantOrProject` asserts this).

### Project
One assessed system: its source location, its architecture documents and
manifest, its environment targets, its policy. An organisation has many. Slugs
are unique *per organisation*, so two tenants can both have a project called
`platform`.

### AssessmentRun
One execution of the gate against one project, at one moment, under one policy
version. It is also the queue entry: `queued → running → succeeded | failed |
cancelled`.

### Finding
One observation belonging to one run, carrying the engine's schema unchanged —
id, source, layer, severity, title, explanation, evidence, location,
remediation, blocking, timestamp — plus the owning organisation and project. The
engine's fingerprint-derived id is preserved, so a report regenerated from
storage matches the one the engine produced and a policy suppression written
against it keeps working.

### Policy
The release rules, versioned. A policy with no `project_id` is the organisation
default; one with a `project_id` is that project's override. Resolution order is
**project → organisation → engine default**, implemented once in
`PolicyService.resolve_for_project`. Versions are immutable: an edit creates a
new version, and each run records the `policy_id` that judged it, so "why did
this pass in March?" is answerable.

A policy is bounded from below. `MANDATORY_CAPABILITIES` and
`MANDATORY_BLOCK_SEVERITIES` in `markna/policy.py` are unioned into whatever the
document asks for, floor findings are exempt from suppressions and severity
overrides, and a verdict computed with zero mandatory capabilities evaluated is
`BLOCK` rather than `PASS`. The reasoning is that policy authorship is a
maintainer-level privilege while switching off the gate is not, so the two have
to be separable — a maintainer can decide that a `medium` does not block a
release, and cannot decide that nothing does.

### Report
A rendered artefact of a run in one format (JSON, Markdown, SARIF, HTML). Stored
per run and per format; listings return metadata, only the download endpoint
returns content.

Supporting records: `EnvironmentTarget` (an authorised URL), `ScannerRunRecord`
and `CoverageRecord` (the evidence that a run was complete), `User`, `ApiToken`,
`Session`, and an append-only `AuditEvent` log.

---

## 3. Tenancy

Three mechanisms, none of which relies on a developer remembering to add a
`WHERE` clause:

1. **Every repository method takes a `TenantScope` first.** Not an optional
   filter — the first positional parameter. A method that cannot be expressed
   within a scope is a method that should not exist. A test parses
   `storage/repositories.py` and fails if any tenant-scoped method's signature
   does not start `(self, scope, …)`.

2. **Writes are checked too.** `_assert_scope` refuses to persist a record whose
   `organization_id` differs from the unit of work's scope, so a mismatched
   entity cannot be written at all — not just not read back.

3. **The scope comes from the principal, never from the request.** No endpoint
   accepts an `organization_id`; supplying one in a body is ignored, and
   supplying one that differs from the caller's raises `TenantIsolationError`.

`TenantIsolationError` renders as **404, not 403**: telling an attacker "that
exists but is not yours" is itself a disclosure.

**The one documented exception** is the worker. `claim_next_queued` and
`complete` read and write across tenants, because a worker is infrastructure and
has no principal — the run row is its authority. That surface is exactly two
methods, both named in `RunRepository` under a comment saying so, and a test
asserts the set has not grown.

---

## 4. Identity and access

**Authentication** is a protocol with a registry, not a hard-coded mechanism:

```python
class AuthenticationProvider(Protocol):
    name: str
    def authenticate(self, request: AuthRequest) -> Optional[Principal]: ...
    def challenge(self, request: AuthRequest) -> Optional[Challenge]: ...
```

Providers are tried in configured order; the first to recognise the caller wins.
Three ship in v1.0:

| Provider | For | Notes |
| --- | --- | --- |
| `api-token` | CI and machine callers | `Authorization: Bearer mkna_…`; stored as a SHA-256 hash, revocable |
| `session` | the browser UI | opaque server-side session id in an `HttpOnly; SameSite=Lax` cookie |
| `trusted-header` | identity asserted by an authenticating proxy | off unless configured, and configuration is rejected without an explicit organisation binding |

**Adding Entra ID.** Two supported routes, neither of which touches the services:

* *Today, no code:* front the application with Azure App Service Authentication,
  oauth2-proxy or an equivalent, and enable `trusted-header` pointed at the
  identity header it sets. The provider matches an **existing, active** user; it
  never auto-provisions, so removing the user in MARKNA is a real control
  independent of the directory. Only enable it where the application is
  unreachable except through that proxy — a header-trusting provider on an
  open port is an authentication bypass, which is why it is not a default.
* *Natively:* implement `AuthenticationProvider` for OIDC — authorisation-code
  redirect in `challenge()`, callback route, ID-token validation, then look the
  user up with `IdentityStore.find_user_by_subject(issuer, subject)` — and add
  it to `PROVIDER_FACTORIES`. The `users` table already carries `subject` and
  `issuer` columns, separate from the primary key, precisely so an existing
  account can be bound to a directory identity without rewriting the foreign
  keys that point at it.

**Authorisation** is one object, `AccessControl`, used by every service method.
Three roles map to permission sets; groups from a directory would map onto the
same three roles rather than introducing a parallel model.

| Role | Permissions |
| --- | --- |
| `viewer` | read organisation, projects, policies, runs, reports |
| `maintainer` | viewer + write projects, write policies, create and cancel runs |
| `admin` | maintainer + organisation administration, manage API tokens and users |

Roles are discrete labels, not a ladder: a maintainer holds `maintainer`, not
`{viewer, maintainer}`. Anywhere two grants are compared — issuing a token,
resolving a token's effective privilege — the comparison is on **permission
sets**, never on role names, so a maintainer can mint a read-only CI token
without holding the `viewer` label.

**API tokens** carry their own explicit roles, defaulting to the issuer's. The
issuing path rejects any token whose permissions exceed the issuer's, and the
provider re-checks that at authentication time against the live user, so
demoting a user immediately bounds every token they issued. Anyone who can hold
a session can mint a token no stronger than themselves; only an administrator
can see or revoke someone else's.

**Login is not scoped to a singleton organisation.** The caller supplies an
address; accounts are resolved across every tenant and the match determines the
scope. An address valid in two tenants is refused until the caller also names
the organisation slug — the ambiguity is only reachable by someone who has
already proved the password in both, so saying so discloses nothing. Failure is
uniform (a decoy verification runs when no account matches, so an unknown
address costs the same work) and bounded (failures are counted per address and
per client; past the threshold the attempt is refused before any verification).
The failure counters are deliberately **not** tenant-scoped: throttling has to
happen before the tenant is known, and keying it by tenant would let an attacker
sidestep the limit by varying the organisation.

---

## 5. Execution separation

Scanners shell out to third-party binaries, pull container images and can run
for minutes. None of that belongs in a request handler.

* The API/UI writes a `queued` run. `RunService.enqueue` validates and inserts;
  it does not import the engine.
* A worker claims the oldest queued run with a conditional `UPDATE … WHERE
  status='queued'`. Two workers racing produce one winner and one `None`, so
  scaling out is starting another process. No broker.
* `execution/adapter.py` is the **only** module that imports `markna.runner`. It
  translates a project, its policy version and its authorised target into a
  `RunConfig`, and the resulting `Assessment` back into findings, scanner
  records, coverage and reports.
* A scanner failure is data. A *run* failure — bad configuration, a deleted
  target — is recorded on the run row, so it is visible in the UI rather than
  only in the worker's log.

Verified two ways: an AST test that no user-facing module imports the runner,
and a subprocess test asserting that `import markna_server.app` does not pull
`markna.runner` into `sys.modules`.

The engine (`markna/`) is unchanged by all of this and still knows nothing about
organisations, projects or HTTP — a test asserts it never imports
`markna_server`.

---

## 6. Storage

SQLite, behind repository protocols. It is the right default for a
single-organisation deployment: one file to back up, no separate service, and
with WAL enabled it handles a web process and a worker process concurrently.

The schema is deliberately portable — no SQLite-specific types, no triggers,
JSON stored as `TEXT` — and every index leads with `organization_id`. Moving to
PostgreSQL is a second implementation of the protocols plus a configuration
change, which is the migration a multi-client deployment would need anyway.

Reports are stored as rows today, bounded by `max_report_bytes`. Moving them to
object storage is a change to `ReportRepository` alone.

---

## 7. Security controls in the application itself

A gate that checks other systems for these has to have them.

* **CSP** `default-src 'none'` — the UI ships no JavaScript and no external
  assets, so the strictest policy is also the accurate one. Plus `nosniff`,
  `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, `Permissions-Policy`,
  `Cache-Control: no-store`, and HSTS when the request arrived over HTTPS.
* **CSRF** — every state-changing form carries a session-bound token derived as
  `HMAC(secret_key, session_id)`. Derived rather than stored, so it survives a
  restart and works across several web processes.
* **Sessions** — opaque ids, server-side and revocable; nothing in the cookie.
* **Passwords** — PBKDF2-HMAC-SHA256, 240k iterations, 12-character minimum.
* **Path confinement** — a project's paths are administrator-supplied but name
  files the worker will open, so they are resolved against `workspace_root` and
  rejected if they escape it.
* **Environment authorisation** — a target cannot be stored without
  `authorized_by`, and adding one is audited with who authorised it. The engine
  independently refuses to send traffic without a valid authorisation, checks
  scope on every request including redirects, and only ever issues GET, HEAD and
  OPTIONS.
* **Uniform failure messages** — unknown user and wrong password produce the
  same text; cross-tenant lookups produce 404.
* **Workspace confinement** — the repository under assessment is hostile input.
  `markna/confinement.py` holds three independent controls: the walk never
  descends a symlinked directory or yields a file resolving outside the project
  root; every read MARKNA performs on a scanner-reported path is confined to
  that root; and any finding whose location escapes has its evidence stripped
  and replaced by an explicit escape finding. A third-party scanner may still
  follow a link — what it cannot do is make MARKNA quote the result into a
  report. Confinement is by resolved path, not string prefix.
* **Scanner environments are allow-listed** — a scanner subprocess receives
  `PATH`, `HOME`, locale, TLS trust, proxy settings and its own `TOOL_*`
  configuration. It does not receive the worker's API keys, database path or
  cloud credentials.
* **Destination enforcement at connect time** — the environment client resolves
  the host itself, vets each candidate address, and connects to an address that
  has already been approved. Checking a hostname and then letting the socket
  resolve it again leaves a window in which the answer can change; this closes
  it.
* **Request parsing inside the error boundary** — a negative or non-numeric
  `Content-Length` is a 400, not an exception escaping the WSGI callable, and
  the body limit is enforced on the read rather than trusting the header.
* **API CSRF** — session-authenticated writes to `/api/` must present a
  session-bound header a cross-origin form cannot set; JSON endpoints require
  `Content-Type: application/json`, which closes the simple-request shape.
  Bearer-token callers are exempt: a token is not an ambient credential.

---

## 8. Deployment

```bash
pip install ./security-gate
markna-server initdb
markna-server bootstrap --org-slug acme --org-name "Acme" \
  --admin-email security@acme.example --admin-password '…'

# web process (any WSGI server; the application is a plain WSGI callable)
gunicorn -w 4 -b 127.0.0.1:8080 markna_server.app:application

# worker process — one or more
markna-server worker
```

`markna-server serve` runs the standard library's development server; it is for
local work, not for production. Configuration comes from a YAML file
(`MARKNA_CONFIG`) with environment-variable overrides.

---

## 9. Future productisation considerations

**None of this is implemented, and none of it is proposed for v1.0.** It is
recorded so that the boundaries above have a stated purpose, and so a future
decision starts from a known position rather than a survey.

### Multi-tenant operation
The data model is already tenant-shaped. What would still be needed:

* a way to *select* an organisation — today a principal belongs to exactly one,
  and every endpoint infers the tenant from it. Users belonging to several
  organisations would need a membership table (`user_id, organization_id, roles`)
  in place of the columns on `users`, and an active-tenant concept in the
  session;
* organisation provisioning beyond `bootstrap`, with slug reservation;
* per-tenant limits (concurrent runs, retention, report size) — the worker
  currently drains one global FIFO queue, which is fair within one organisation
  and not across several;
* PostgreSQL, and a decision on isolation model (shared schema with the existing
  `organization_id` scoping, versus schema- or database-per-tenant);
* worker isolation. One worker process runs any tenant's scanners today. Across
  clients that becomes a hard requirement: per-tenant containers or dedicated
  worker pools, because scanning is code execution over customer material.

### Billing and metering
Nothing meters anything today. Runs, findings and report bytes are the obvious
units, and `AssessmentRun` already records timing and outcome — a metering
reader over existing tables, rather than new instrumentation. Plan enforcement
would sit in `RunService.enqueue`, which is already the single choke point for
starting work.

### Customer signup and self-service
Deliberately absent: there is no public registration route, no email
verification, no password reset. Adding them means an email transport, rate
limiting on the auth endpoints, and a re-look at the "no auto-provisioning" rule
in `TrustedHeaderProvider`. Until then, accounts are created by an administrator.

### Customer-facing portal
The current UI is an internal review tool: it shows scanner commands, worker
identifiers, coverage gaps and raw evidence. A customer-facing view would want a
different, narrower surface over the same services — which is why the services
return domain objects and the serialisers are a separate module.

### Data residency, retention and deletion
No retention policy, no deletion workflow, no export. Findings and reports
accumulate. `ON DELETE CASCADE` from `organizations` means tenant deletion is
technically one statement, but "delete my data" as a product commitment needs a
defined scope, an audit trail of the deletion, and a decision about the audit
log itself.

### Scale
SQLite and a polling worker are appropriate for one organisation. Multiple
clients would want PostgreSQL with `SELECT … FOR UPDATE SKIP LOCKED` (the claim
query maps onto it directly), object storage for reports, and probably a real
queue once fan-out or priority matters.

---

## 10. Deliberately not in v1.0

* Cloning repositories. A project names a path the operator has provisioned;
  `url`, `provider` and `default_branch` are recorded so adding a checkout step
  is a worker change, not a schema change.
* Scheduled runs and webhook triggers. `TriggerKind` already has `SCHEDULE` and
  `WEBHOOK` values, so the record shape is settled; nothing fires them.
* A policy editor in the UI. Policies are imported through the API or
  `markna-server policy import`.
* Finding triage workflow — accepting, assigning, or muting an individual
  finding across runs. Suppression is a policy decision today, which keeps it
  reviewable and versioned.
* Notifications of any kind.
