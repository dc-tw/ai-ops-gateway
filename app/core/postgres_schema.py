"""Checksummed PostgreSQL history schema, independent of live Redis authority."""

import hashlib

SCHEMA_VERSION = 1
SCHEMA_SQL = """
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
SCHEMA_CHECKSUM = hashlib.sha256(SCHEMA_SQL.encode("utf-8")).hexdigest()
TABLE_NAMES = (
    "gateway_applied_operations",
    "gateway_records",
    "gateway_event_identities",
    "gateway_events",
    "gateway_tombstones",
    "gateway_purge_fences",
    "gateway_migration_reports",
)
