-- MARKNA Security Gate — relational schema.
--
-- Every table except `organizations` carries `organization_id`, every foreign
-- key that crosses a table carries it too, and every index leads with it. That
-- is what makes a tenant-scoped query the natural one to write and an unscoped
-- query the awkward one.
--
-- v1.0 runs on SQLite. The schema is deliberately portable: no SQLite-specific
-- types, no triggers, JSON stored as TEXT. Moving to PostgreSQL is a new
-- implementation of the repository protocols, not a redesign.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS organizations (
    id           TEXT PRIMARY KEY,
    slug         TEXT NOT NULL UNIQUE,
    name         TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    is_active    INTEGER NOT NULL DEFAULT 1,
    settings     TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS users (
    id              TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    email           TEXT NOT NULL,
    display_name    TEXT NOT NULL DEFAULT '',
    roles           TEXT NOT NULL DEFAULT 'viewer',
    -- Identity-provider subject/issuer. NULL for a locally managed account;
    -- populated when the account is bound to a directory.
    subject         TEXT,
    issuer          TEXT,
    password_hash   TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,
    last_login_at   TEXT,
    UNIQUE (organization_id, email)
);
CREATE INDEX IF NOT EXISTS idx_users_subject ON users(issuer, subject);

CREATE TABLE IF NOT EXISTS api_tokens (
    id              TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    -- The token's own roles. Always a subset of the issuer's roles at creation
    -- time, and intersected with the owner's current roles at authentication
    -- time, so a demotion or a role change takes effect immediately.
    roles           TEXT NOT NULL DEFAULT 'viewer',
    token_hash      TEXT NOT NULL UNIQUE,
    created_at      TEXT NOT NULL,
    expires_at      TEXT,
    last_used_at    TEXT,
    revoked_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_tokens_org ON api_tokens(organization_id, created_at DESC);

CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at      TEXT NOT NULL,
    expires_at      TEXT NOT NULL,
    user_agent      TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);

CREATE TABLE IF NOT EXISTS projects (
    id                     TEXT PRIMARY KEY,
    organization_id        TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    slug                   TEXT NOT NULL,
    name                   TEXT NOT NULL,
    description            TEXT NOT NULL DEFAULT '',
    source                 TEXT NOT NULL DEFAULT '{}',
    architecture_documents TEXT NOT NULL DEFAULT '[]',
    architecture_manifest  TEXT,
    default_policy_id      TEXT,
    created_at             TEXT NOT NULL,
    updated_at             TEXT NOT NULL,
    archived_at            TEXT,
    -- Slugs are unique per organisation, not globally: two tenants may both
    -- have a project called "platform".
    UNIQUE (organization_id, slug)
);

CREATE TABLE IF NOT EXISTS environment_targets (
    id                       TEXT PRIMARY KEY,
    organization_id          TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    project_id               TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name                     TEXT NOT NULL,
    url                      TEXT NOT NULL,
    kind                     TEXT NOT NULL DEFAULT 'uat',
    authorized_by            TEXT NOT NULL,
    authorization_reference  TEXT,
    authorization_expires    TEXT,
    scope_hosts              TEXT NOT NULL DEFAULT '[]',
    allow_private_targets    INTEGER NOT NULL DEFAULT 0,
    created_at               TEXT NOT NULL,
    UNIQUE (project_id, name)
);
CREATE INDEX IF NOT EXISTS idx_targets_org ON environment_targets(organization_id, project_id);

CREATE TABLE IF NOT EXISTS policies (
    id              TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    -- NULL project_id means "organisation default".
    project_id      TEXT REFERENCES projects(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    document        TEXT NOT NULL DEFAULT '{}',
    is_default      INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    created_by      TEXT
);
CREATE INDEX IF NOT EXISTS idx_policies_scope
    ON policies(organization_id, project_id, version DESC);

CREATE TABLE IF NOT EXISTS assessment_runs (
    id                    TEXT PRIMARY KEY,
    organization_id       TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    project_id            TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    policy_id             TEXT,
    policy_name           TEXT NOT NULL DEFAULT '',
    target_id             TEXT,
    status                TEXT NOT NULL DEFAULT 'queued',
    layers                TEXT NOT NULL DEFAULT '[]',
    trigger               TEXT NOT NULL DEFAULT '{}',
    verdict               TEXT,
    summary               TEXT NOT NULL DEFAULT '{}',
    verdict_reasons       TEXT NOT NULL DEFAULT '[]',
    engine_assessment_id  TEXT,
    git_commit            TEXT,
    git_branch            TEXT,
    environment_url       TEXT,
    error                 TEXT,
    worker_id             TEXT,
    created_at            TEXT NOT NULL,
    started_at            TEXT,
    finished_at           TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_project ON assessment_runs(organization_id, project_id, created_at DESC);
-- The worker's claim query. Ordered by creation so the queue is FIFO.
CREATE INDEX IF NOT EXISTS idx_runs_queue ON assessment_runs(status, created_at);

CREATE TABLE IF NOT EXISTS findings (
    id                 TEXT PRIMARY KEY,
    organization_id    TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    project_id         TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    run_id             TEXT NOT NULL REFERENCES assessment_runs(id) ON DELETE CASCADE,
    engine_id          TEXT NOT NULL,
    fingerprint        TEXT NOT NULL,
    source             TEXT NOT NULL,
    layer              TEXT NOT NULL,
    severity           TEXT NOT NULL,
    severity_rank      INTEGER NOT NULL DEFAULT 0,
    title              TEXT NOT NULL,
    explanation        TEXT NOT NULL DEFAULT '',
    evidence           TEXT NOT NULL DEFAULT '',
    location           TEXT NOT NULL DEFAULT '{}',
    remediation        TEXT NOT NULL DEFAULT '',
    blocking           INTEGER NOT NULL DEFAULT 0,
    detected_at        TEXT NOT NULL,
    rule_id            TEXT,
    refs               TEXT NOT NULL DEFAULT '[]',
    tags               TEXT NOT NULL DEFAULT '[]',
    cwe                TEXT NOT NULL DEFAULT '[]',
    confidence         TEXT NOT NULL DEFAULT 'high',
    ai_generated       INTEGER NOT NULL DEFAULT 0,
    suppressed         INTEGER NOT NULL DEFAULT 0,
    suppression_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_findings_run ON findings(organization_id, run_id, severity_rank DESC);
-- Supports "has this fingerprint been seen before in this project", which is
-- what a future trend or triage view needs.
CREATE INDEX IF NOT EXISTS idx_findings_fingerprint
    ON findings(organization_id, project_id, fingerprint);

CREATE TABLE IF NOT EXISTS scanner_runs (
    organization_id  TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    run_id           TEXT NOT NULL REFERENCES assessment_runs(id) ON DELETE CASCADE,
    name             TEXT NOT NULL,
    layer            TEXT NOT NULL,
    status           TEXT NOT NULL,
    capabilities     TEXT NOT NULL DEFAULT '[]',
    deterministic    INTEGER NOT NULL DEFAULT 1,
    tool             TEXT,
    tool_version     TEXT,
    command          TEXT,
    duration_seconds REAL NOT NULL DEFAULT 0,
    findings_count   INTEGER NOT NULL DEFAULT 0,
    message          TEXT,
    PRIMARY KEY (run_id, name)
);

CREATE TABLE IF NOT EXISTS coverage (
    organization_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    run_id          TEXT NOT NULL REFERENCES assessment_runs(id) ON DELETE CASCADE,
    layer           TEXT NOT NULL,
    capability      TEXT NOT NULL,
    required        INTEGER NOT NULL DEFAULT 1,
    satisfied       INTEGER NOT NULL DEFAULT 0,
    satisfied_by    TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (run_id, layer, capability)
);

CREATE TABLE IF NOT EXISTS reports (
    id              TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    project_id      TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    run_id          TEXT NOT NULL REFERENCES assessment_runs(id) ON DELETE CASCADE,
    format          TEXT NOT NULL,
    content_type    TEXT NOT NULL,
    size_bytes      INTEGER NOT NULL DEFAULT 0,
    content         TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    UNIQUE (run_id, format)
);
CREATE INDEX IF NOT EXISTS idx_reports_org ON reports(organization_id, run_id);

CREATE TABLE IF NOT EXISTS audit_events (
    id              TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    actor_id        TEXT,
    actor_label     TEXT NOT NULL DEFAULT 'system',
    action          TEXT NOT NULL,
    subject_type    TEXT NOT NULL DEFAULT '',
    subject_id      TEXT NOT NULL DEFAULT '',
    detail          TEXT NOT NULL DEFAULT '{}',
    at              TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_org ON audit_events(organization_id, at DESC);

-- Failed-authentication ledger, used to throttle credential guessing. Kept in
-- the database rather than in process memory so several web processes share one
-- view of an attack, and so a restart does not reset an attacker's budget.
CREATE TABLE IF NOT EXISTS auth_attempts (
    id         TEXT PRIMARY KEY,
    -- An opaque bucket key: the email attempted, or the client address.
    scope_key  TEXT NOT NULL,
    at         TEXT NOT NULL,
    successful INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_auth_attempts_key ON auth_attempts(scope_key, at DESC);

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
