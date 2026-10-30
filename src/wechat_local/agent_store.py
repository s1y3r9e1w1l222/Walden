"""Private SQLite store for the local-only P5 product surface.

The Agent Store is deliberately separate from the canonical EventStore.  It
contains the rebuildable I1 search projection and, from schema v2, only P5-I2
task state.  It never attaches or migrates an EventStore.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

from .common import ensure_private_parent


AGENT_STORE_APPLICATION_ID = 0x574C4131  # ASCII "WLA1"
AGENT_STORE_SCHEMA_VERSION = 4


class AgentStoreError(RuntimeError):
    """Raised when Agent Store identity or invariants cannot be preserved."""


# This frozen v1 schema is retained verbatim so the v1 -> v2 migration can be
# exercised against a real I1-shaped database.
SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS projection_profiles (
    profile_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    normalizer_name TEXT NOT NULL,
    normalizer_version TEXT NOT NULL,
    redaction_profile TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (contract_version, normalizer_name, normalizer_version, redaction_profile)
);
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    profile_id TEXT PRIMARY KEY REFERENCES projection_profiles(profile_id) ON DELETE CASCADE,
    last_materialization_id INTEGER NOT NULL DEFAULT 0 CHECK(last_materialization_id >= 0),
    projected_count INTEGER NOT NULL DEFAULT 0 CHECK(projected_count >= 0),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_search_projection (
    profile_id TEXT NOT NULL REFERENCES projection_profiles(profile_id) ON DELETE CASCADE,
    materialization_id INTEGER NOT NULL CHECK(materialization_id > 0),
    event_id TEXT NOT NULL, subject_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
    occurred_at TEXT NOT NULL, occurred_at_fallback INTEGER NOT NULL CHECK(occurred_at_fallback IN (0, 1)),
    observed_at TEXT NOT NULL, event_type TEXT NOT NULL, content_kind TEXT NOT NULL,
    citation_text TEXT NOT NULL, searchable_text TEXT NOT NULL,
    quality_status TEXT NOT NULL CHECK(quality_status IN ('normalized', 'degraded')),
    quality_codes_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    indexed_at TEXT NOT NULL, PRIMARY KEY (profile_id, materialization_id)
);
CREATE VIRTUAL TABLE IF NOT EXISTS event_search_fts USING fts5(
    profile_id UNINDEXED, materialization_id UNINDEXED, searchable_text, tokenize = 'unicode61'
);
CREATE INDEX IF NOT EXISTS event_search_projection_query_idx ON event_search_projection(profile_id, occurred_at DESC, materialization_id DESC);
CREATE INDEX IF NOT EXISTS event_search_projection_conversation_idx ON event_search_projection(profile_id, conversation_id, occurred_at DESC, materialization_id DESC);
CREATE INDEX IF NOT EXISTS event_search_projection_event_idx ON event_search_projection(profile_id, event_id, materialization_id DESC);
"""


SCHEMA_V2_ADDITIONS = """
CREATE TABLE policy_snapshots (
    policy_id TEXT PRIMARY KEY, policy_version TEXT NOT NULL, policy_json TEXT NOT NULL,
    policy_hash TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL
);
CREATE TABLE policy_evaluations (
    evaluation_id TEXT PRIMARY KEY, policy_id TEXT NOT NULL REFERENCES policy_snapshots(policy_id),
    policy_version TEXT NOT NULL, outcome TEXT NOT NULL CHECK(outcome IN ('allow','deny','degrade')),
    reason_codes_json TEXT NOT NULL, resolved_limits_json TEXT NOT NULL, scope_hash TEXT NOT NULL,
    provider_profile TEXT NOT NULL, task_id TEXT, created_at TEXT NOT NULL
);
CREATE TABLE agent_tasks (
    task_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK(kind IN ('answer','summarize','extract_tasks','draft_reply')),
    status TEXT NOT NULL CHECK(status IN ('pending','running','retryable','complete','failed','cancelled')),
    input_text TEXT NOT NULL CHECK(length(input_text) BETWEEN 1 AND 2000),
    scope_json TEXT NOT NULL, scope_hash TEXT NOT NULL,
    policy_id TEXT NOT NULL REFERENCES policy_snapshots(policy_id), policy_version TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version > 0), attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK(max_attempts BETWEEN 1 AND 10), available_at TEXT NOT NULL,
    active_run_id TEXT, lease_owner TEXT, lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK(lease_epoch >= 0),
    lease_expires_at TEXT, terminal_error_type TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    terminal_at TEXT
);
CREATE TABLE agent_task_event_refs (
    task_id TEXT NOT NULL REFERENCES agent_tasks(task_id) ON DELETE CASCADE, run_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0), event_id TEXT NOT NULL, subject_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL, occurred_at TEXT NOT NULL,
    quality_status TEXT NOT NULL CHECK(quality_status IN ('normalized','degraded')),
    citation_text TEXT NOT NULL, redaction_profile TEXT NOT NULL, retrieval_at TEXT NOT NULL,
    query_profile TEXT NOT NULL, PRIMARY KEY(task_id,run_id,ordinal), UNIQUE(task_id,run_id,event_id)
);
CREATE TABLE agent_runs (
    run_id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES agent_tasks(task_id),
    attempt INTEGER NOT NULL CHECK(attempt > 0),
    status TEXT NOT NULL CHECK(status IN ('running','succeeded','retryable','failed')),
    policy_version TEXT NOT NULL, provider_profile TEXT NOT NULL, prompt_version TEXT NOT NULL,
    error_type TEXT, input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(input_tokens >= 0),
    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(output_tokens >= 0),
    cost_microunits INTEGER NOT NULL DEFAULT 0 CHECK(cost_microunits >= 0),
    started_at TEXT NOT NULL, finished_at TEXT, UNIQUE(task_id,attempt)
);
CREATE TABLE decisions (
    decision_id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES agent_tasks(task_id),
    run_id TEXT NOT NULL REFERENCES agent_runs(run_id),
    decision_type TEXT NOT NULL CHECK(decision_type IN ('answer','no_action','needs_human')),
    summary TEXT NOT NULL CHECK(length(summary) <= 1000),
    confidence TEXT NOT NULL CHECK(confidence IN ('high','medium','low','unknown')),
    event_refs_json TEXT NOT NULL, policy_version TEXT NOT NULL, provider_profile TEXT NOT NULL,
    prompt_version TEXT NOT NULL, output_hash TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(run_id)
);
CREATE TABLE audit_traces (
    trace_id TEXT PRIMARY KEY, event_type TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('accepted','rejected','failed')), reason_code TEXT,
    actor_kind TEXT NOT NULL, related_ids_json TEXT NOT NULL, policy_version TEXT,
    provider_profile TEXT, prompt_version TEXT, created_at TEXT NOT NULL
);
CREATE TABLE command_idempotency (
    actor_id TEXT NOT NULL, endpoint TEXT NOT NULL, key_hash TEXT NOT NULL, request_hash TEXT NOT NULL,
    resource_id TEXT NOT NULL, safe_status TEXT NOT NULL, created_at TEXT NOT NULL, expires_at TEXT NOT NULL,
    PRIMARY KEY(actor_id,endpoint,key_hash)
);
CREATE INDEX agent_tasks_ready_idx ON agent_tasks(status,available_at);
CREATE INDEX agent_tasks_created_idx ON agent_tasks(created_at DESC,task_id DESC);
CREATE INDEX agent_runs_task_idx ON agent_runs(task_id,attempt);
CREATE INDEX audit_traces_event_idx ON audit_traces(event_type,created_at);
"""

SCHEMA_V3_ADDITIONS = """
CREATE TABLE action_intents (
    action_id TEXT NOT NULL, action_version INTEGER NOT NULL CHECK(action_version > 0),
    decision_id TEXT NOT NULL REFERENCES decisions(decision_id), action_type TEXT NOT NULL CHECK(action_type='draft_reply'),
    target_ref TEXT NOT NULL, payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('awaiting_approval','approved','rejected','expired','superseded','queued')),
    risk_level TEXT NOT NULL CHECK(risk_level='medium'), requires_approval INTEGER NOT NULL CHECK(requires_approval=1),
    created_at TEXT NOT NULL, PRIMARY KEY(action_id,action_version)
);
CREATE TABLE approvals (
    approval_id TEXT PRIMARY KEY, action_id TEXT NOT NULL, action_version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','approved','edited','rejected','expired','superseded')),
    version INTEGER NOT NULL CHECK(version>0), actor_id TEXT NOT NULL, expires_at TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    FOREIGN KEY(action_id,action_version) REFERENCES action_intents(action_id,action_version)
);
CREATE TABLE action_outbox (
    action_id TEXT NOT NULL, action_version INTEGER NOT NULL, execution_id TEXT NOT NULL UNIQUE,
    logical_key TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL, PRIMARY KEY(action_id,action_version),
    FOREIGN KEY(action_id,action_version) REFERENCES action_intents(action_id,action_version)
);
CREATE TABLE executions (
    execution_id TEXT PRIMARY KEY, action_id TEXT NOT NULL, action_version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('queued','running','retryable','succeeded','failed','cancelled')),
    version INTEGER NOT NULL CHECK(version>0), attempt_count INTEGER NOT NULL DEFAULT 0, lease_owner TEXT, lease_epoch INTEGER NOT NULL DEFAULT 0,
    lease_expires_at TEXT, available_at TEXT NOT NULL, max_attempts INTEGER NOT NULL DEFAULT 3 CHECK(max_attempts BETWEEN 1 AND 5),
    error_type TEXT, logical_key TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    FOREIGN KEY(action_id,action_version) REFERENCES action_intents(action_id,action_version)
);
CREATE TABLE execution_results (
    result_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(execution_id), attempt INTEGER NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome='dry_run'), created_at TEXT NOT NULL, UNIQUE(execution_id,attempt)
);
CREATE INDEX approvals_action_idx ON approvals(action_id,action_version,created_at DESC);
CREATE INDEX executions_ready_idx ON executions(status,available_at,created_at);
"""

SCHEMA_V4_ADDITIONS = """
CREATE TABLE automation_policies (
    policy_id TEXT NOT NULL, policy_version INTEGER NOT NULL CHECK(policy_version > 0),
    name TEXT NOT NULL CHECK(length(name) BETWEEN 1 AND 80),
    status TEXT NOT NULL CHECK(status IN ('disabled','enabled')),
    trigger_json TEXT NOT NULL, scope_json TEXT NOT NULL, template_json TEXT NOT NULL,
    rate_limit INTEGER NOT NULL CHECK(rate_limit BETWEEN 1 AND 20), cooldown_seconds INTEGER NOT NULL CHECK(cooldown_seconds BETWEEN 0 AND 86400),
    version INTEGER NOT NULL CHECK(version > 0), created_at TEXT NOT NULL, updated_at TEXT NOT NULL, disabled_at TEXT,
    PRIMARY KEY(policy_id, policy_version)
);
CREATE TABLE automation_policy_checkpoints (
    policy_id TEXT NOT NULL, policy_version INTEGER NOT NULL, profile_id TEXT NOT NULL REFERENCES projection_profiles(profile_id),
    last_materialization_id INTEGER NOT NULL DEFAULT 0 CHECK(last_materialization_id >= 0),
    version INTEGER NOT NULL CHECK(version > 0), lease_owner TEXT, lease_epoch INTEGER NOT NULL DEFAULT 0, lease_expires_at TEXT, updated_at TEXT NOT NULL,
    PRIMARY KEY(policy_id, policy_version), FOREIGN KEY(policy_id,policy_version) REFERENCES automation_policies(policy_id,policy_version)
);
CREATE TABLE automation_trigger_receipts (
    policy_id TEXT NOT NULL, policy_version INTEGER NOT NULL, event_id TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('created','rate_limited','cooldown','max_inflight','duplicate','policy_denied','ineligible')),
    task_id TEXT UNIQUE REFERENCES agent_tasks(task_id), trace_id TEXT REFERENCES audit_traces(trace_id), created_at TEXT NOT NULL,
    PRIMARY KEY(policy_id, policy_version, event_id), FOREIGN KEY(policy_id,policy_version) REFERENCES automation_policies(policy_id,policy_version)
);
CREATE INDEX automation_policies_current_idx ON automation_policies(policy_id,policy_version DESC);
CREATE INDEX automation_receipts_policy_idx ON automation_trigger_receipts(policy_id,policy_version,created_at DESC);
"""


class AgentStore:
    """Writable private store for P5 state, isolated from EventStore."""

    def __init__(self, path: Path, *, check_same_thread: bool = True):
        if path.is_symlink():
            raise AgentStoreError("Agent Store destination must not be a symlink")
        self.path = path.resolve()
        ensure_private_parent(self.path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, timeout=5.0, check_same_thread=check_same_thread)
        self._conn.row_factory = sqlite3.Row
        try:
            self._configure()
            self._open_or_initialize()
            os.chmod(self.path, 0o600)
        except Exception:
            self._conn.close()
            raise

    def _configure(self) -> None:
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self._conn.execute("PRAGMA synchronous = FULL")

    def _open_or_initialize(self) -> None:
        version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
        application_id = int(self._conn.execute("PRAGMA application_id").fetchone()[0])
        tables = self._conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'").fetchall()
        if version > AGENT_STORE_SCHEMA_VERSION:
            raise AgentStoreError("Unsupported future Agent Store schema")
        if version not in (0, 1, 2, 3, AGENT_STORE_SCHEMA_VERSION):
            raise AgentStoreError("Unsupported Agent Store schema")
        if version == AGENT_STORE_SCHEMA_VERSION:
            if application_id != AGENT_STORE_APPLICATION_ID:
                raise AgentStoreError("Database is not a P5 Agent Store")
            return
        if version == 3:
            if application_id != AGENT_STORE_APPLICATION_ID:
                raise AgentStoreError("Database is not a P5 Agent Store")
            self._migrate_v3_to_v4()
            return
        if version == 2:
            if application_id != AGENT_STORE_APPLICATION_ID:
                raise AgentStoreError("Database is not a P5 Agent Store")
            self._migrate_v2_to_v3()
            self._migrate_v3_to_v4()
            return
        if version == 1:
            if application_id != AGENT_STORE_APPLICATION_ID:
                raise AgentStoreError("Database is not a P5 Agent Store")
            self._migrate_v1_to_v2()
            self._migrate_v2_to_v3()
            self._migrate_v3_to_v4()
            return
        if application_id not in (0, AGENT_STORE_APPLICATION_ID):
            raise AgentStoreError("Unrelated SQLite database is not an Agent Store")
        if tables:
            raise AgentStoreError("Refusing to initialize a nonempty database as an Agent Store")
        self._initialize_v1()
        self._migrate_v1_to_v2()
        self._migrate_v2_to_v3()
        self._migrate_v3_to_v4()

    def _initialize_v1(self) -> None:
        self._conn.execute("PRAGMA journal_mode = WAL")
        try:
            self._conn.executescript("BEGIN IMMEDIATE;\n" + SCHEMA_V1 + f"\nPRAGMA application_id = {AGENT_STORE_APPLICATION_ID};\nPRAGMA user_version = 1;\nCOMMIT;")
        except Exception:
            if self._conn.in_transaction:
                self._conn.rollback()
            raise

    def _migrate_v1_to_v2(self) -> None:
        """Add I2 state atomically without changing I1 projection rows."""
        try:
            self._conn.executescript("BEGIN IMMEDIATE;\n" + SCHEMA_V2_ADDITIONS + "\nPRAGMA user_version = 2;\nCOMMIT;")
        except Exception:
            if self._conn.in_transaction:
                self._conn.rollback()
            raise

    def _migrate_v2_to_v3(self) -> None:
        """Add the local-only approval/dry-run records atomically."""
        try:
            self._conn.executescript("BEGIN IMMEDIATE;\n" + SCHEMA_V3_ADDITIONS + "\nPRAGMA user_version = 3;\nCOMMIT;")
        except Exception:
            if self._conn.in_transaction:
                self._conn.rollback()
            raise

    def _migrate_v3_to_v4(self) -> None:
        """Add default-deny local automation records atomically."""
        try:
            self._conn.executescript("BEGIN IMMEDIATE;\n" + SCHEMA_V4_ADDITIONS + "\nPRAGMA user_version = 4;\nCOMMIT;")
        except Exception:
            if self._conn.in_transaction:
                self._conn.rollback()
            raise

    def __enter__(self) -> "AgentStore":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()

    def integrity_check(self) -> str:
        return str(self._conn.execute("PRAGMA integrity_check").fetchone()[0])
