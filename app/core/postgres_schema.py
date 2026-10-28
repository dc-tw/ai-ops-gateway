"""Checksummed, append-only PostgreSQL history schema migrations."""

import hashlib

MIGRATION_1_SQL = """
CREATE TABLE gateway_applied_operations (
    source_id text NOT NULL,
    tenant_id text NOT NULL,
    source_ms numeric(20,0) NOT NULL,
    source_seq numeric(20,0) NOT NULL,
    envelope_hash text NOT NULL,
    archived_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_id, source_ms, source_seq)
);
CREATE TABLE gateway_records (
    source_id text NOT NULL,
    tenant_id text NOT NULL,
    kind text NOT NULL,
    resource_id text NOT NULL,
    data jsonb NOT NULL,
    search_text text NOT NULL,
    search_vector tsvector GENERATED ALWAYS AS
        (to_tsvector('simple'::regconfig, search_text)) STORED,
    source_ms numeric(20,0) NOT NULL,
    source_seq numeric(20,0) NOT NULL,
    archived_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_id, tenant_id, kind, resource_id)
);
CREATE INDEX gateway_records_search ON gateway_records USING gin(search_vector);
CREATE TABLE gateway_event_identities (
    source_id text NOT NULL,
    tenant_id text NOT NULL,
    kind text NOT NULL,
    resource_id text NOT NULL,
    event_id text NOT NULL,
    payload_hash text NOT NULL,
    PRIMARY KEY (source_id, tenant_id, kind, resource_id, event_id)
);
CREATE TABLE gateway_events (
    cursor bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id text NOT NULL,
    tenant_id text NOT NULL,
    kind text NOT NULL,
    resource_id text NOT NULL,
    event_id text NOT NULL,
    data jsonb NOT NULL,
    source_ms numeric(20,0) NOT NULL,
    source_seq numeric(20,0) NOT NULL,
    archived_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (source_id, tenant_id, kind, resource_id, event_id)
);
CREATE INDEX gateway_events_scope ON gateway_events
    (source_id, tenant_id, kind, resource_id, cursor);
CREATE TABLE gateway_tombstones (
    source_id text NOT NULL,
    tenant_id text NOT NULL,
    kind text NOT NULL,
    resource_id text NOT NULL,
    source_ms numeric(20,0) NOT NULL,
    source_seq numeric(20,0) NOT NULL,
    reason text NOT NULL,
    archived_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_id, tenant_id, kind, resource_id)
);
CREATE TABLE gateway_purge_fences (
    source_id text NOT NULL,
    tenant_id text NOT NULL,
    kind text NOT NULL,
    source_ms numeric(20,0) NOT NULL,
    source_seq numeric(20,0) NOT NULL,
    PRIMARY KEY (source_id, tenant_id, kind)
);
CREATE TABLE gateway_migration_reports (
    report_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id text NOT NULL,
    report jsonb NOT NULL,
    saved_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
"""

MIGRATION_2_SQL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
ALTER TABLE gateway_events ADD COLUMN search_text text NOT NULL DEFAULT '';
ALTER TABLE gateway_events ADD COLUMN search_vector tsvector GENERATED ALWAYS AS
    (to_tsvector('simple'::regconfig, search_text)) STORED;
CREATE INDEX gateway_events_search ON gateway_events USING gin(search_vector);
CREATE INDEX gateway_records_search_trigram ON gateway_records
    USING gin(search_text gin_trgm_ops);
CREATE INDEX gateway_events_search_trigram ON gateway_events
    USING gin(search_text gin_trgm_ops);
CREATE TABLE gateway_retention_operations (
    operation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id text NOT NULL,
    tenant_id text NOT NULL,
    kind text NOT NULL,
    before_cursor text NOT NULL,
    preview_sha256 text NOT NULL,
    records bigint NOT NULL,
    events bigint NOT NULL,
    target_resources bigint NOT NULL,
    executed_by text NOT NULL,
    executed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX gateway_retention_operations_scope ON gateway_retention_operations
    (source_id, tenant_id, operation_id);
"""

MIGRATIONS = {
    1: MIGRATION_1_SQL,
    2: MIGRATION_2_SQL,
}
MIGRATION_CHECKSUMS = {
    version: hashlib.sha256(sql.encode("utf-8")).hexdigest()
    for version, sql in MIGRATIONS.items()
}
SCHEMA_VERSION = max(MIGRATIONS)
SCHEMA_CHECKSUM = MIGRATION_CHECKSUMS[SCHEMA_VERSION]
# Backwards-compatible name for callers that display the bootstrap migration.
SCHEMA_SQL = MIGRATION_1_SQL
TABLE_NAMES = (
    "gateway_applied_operations",
    "gateway_records",
    "gateway_event_identities",
    "gateway_events",
    "gateway_tombstones",
    "gateway_purge_fences",
    "gateway_migration_reports",
    "gateway_retention_operations",
)
SCHEMA_RELATIONS = TABLE_NAMES + (
    "gateway_records_search",
    "gateway_events_scope",
    "gateway_events_search",
    "gateway_records_search_trigram",
    "gateway_events_search_trigram",
    "gateway_retention_operations_scope",
)
