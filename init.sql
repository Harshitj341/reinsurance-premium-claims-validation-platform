-- =====================================================================
-- PRODUCTION L&H REINSURANCE PLATFORM SCHEMA
-- =====================================================================

-- 1. BRONZE LAYER & INGESTION
CREATE TABLE IF NOT EXISTS file_tracking (
    id SERIAL PRIMARY KEY,
    file_name TEXT,
    year TEXT,
    quarter TEXT,
    file_hash TEXT,
    uploaded_at TIMESTAMP,
    client_id TEXT
);

CREATE TABLE IF NOT EXISTS bronze_ingestion_log (
    run_id TEXT,
    file_name TEXT,
    file_hash TEXT,
    year TEXT,
    quarter TEXT,
    category TEXT,
    status TEXT,
    rows_read INTEGER,
    rows_written INTEGER,
    target_path TEXT,
    error_message TEXT,
    is_duplicate BOOLEAN,
    duplicate_count INTEGER,
    duplicate_files TEXT,
    client_id TEXT
);

CREATE TABLE IF NOT EXISTS persistent_hash_store (
    client_id VARCHAR,
    category VARCHAR,
    policy_number VARCHAR,
    benefit_id VARCHAR,
    valuation_date DATE,
    claim_id VARCHAR
);

-- 2. SCHEMA DRIFT & MAPPING
CREATE TABLE IF NOT EXISTS column_mapping (
    id SERIAL PRIMARY KEY,
    client_id TEXT NOT NULL,
    category TEXT NOT NULL,
    raw_col_name TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    is_critical BOOLEAN,
    is_pii BOOLEAN,
    mapping_version INTEGER NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_by TEXT,
    created_at TIMESTAMP,
    expected_datatype TEXT,
    nullable BOOLEAN,
    is_soa_metric BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_drift_log (
    run_id TEXT,
    file_name TEXT,
    file_hash TEXT,
    category TEXT,
    issue_type TEXT,
    column_name TEXT,
    detected_at TIMESTAMP,
    client_id TEXT
);

CREATE TABLE IF NOT EXISTS mapping_review_queue (
    id SERIAL PRIMARY KEY,
    client_id TEXT NOT NULL,
    category TEXT NOT NULL,
    raw_col_name TEXT NOT NULL,
    canonical_name TEXT,
    sample_values TEXT,
    is_critical BOOLEAN,
    is_pii BOOLEAN,
    status TEXT NOT NULL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMP,
    de_resolved BOOLEAN,
    de_resolved_by TEXT,
    de_resolved_at TIMESTAMP,
    created_at TIMESTAMP,
    file_name TEXT
);

-- 3. SILVER STAGING & DATA QUALITY
CREATE TABLE IF NOT EXISTS silver_run_queue (
    id BIGSERIAL PRIMARY KEY,
    file_hash VARCHAR NOT NULL,
    client_id VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    category VARCHAR,
    status VARCHAR,
    queued_at TIMESTAMP,
    processed_at TIMESTAMP,
    error_message TEXT,
    year VARCHAR,
    quarter VARCHAR,
    period VARCHAR,
    period_type VARCHAR
);

CREATE TABLE IF NOT EXISTS silver_staging_log (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    client_id VARCHAR NOT NULL,
    file_hash VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    year VARCHAR NOT NULL,
    quarter VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    rows_read INTEGER NOT NULL,
    rows_to_silver INTEGER NOT NULL,
    rows_quarantined INTEGER NOT NULL,
    target_path VARCHAR,
    quarantine_path VARCHAR,
    processed_at TIMESTAMPTZ,
    error_message TEXT,
    period TEXT,
    period_type TEXT
);

CREATE TABLE IF NOT EXISTS silver_quarantine_summary (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    client_id VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    dq_rule VARCHAR NOT NULL,
    row_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ
);

-- 4. TREATY CONFIGURATION & VALIDATION
CREATE TABLE IF NOT EXISTS treaty_benefit_config (
    uid SERIAL PRIMARY KEY,
    client_id TEXT NOT NULL,
    treaty_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    benefit_id TEXT NOT NULL,
    plan_code TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    age_min INTEGER NOT NULL,
    age_max INTEGER NOT NULL,
    retention_limit NUMERIC,
    quota_share NUMERIC,
    surplus_multiple NUMERIC,
    cal_limit NUMERIC,
    cal_level TEXT NOT NULL,
    effective_date DATE,
    is_inforce_block BOOLEAN NOT NULL,
    bdx_reporting_freq TEXT NOT NULL,
    policy_renewal_freq TEXT NOT NULL,
    addendum_no TEXT,
    created_at TIMESTAMP,
    created_by TEXT
);

CREATE TABLE IF NOT EXISTS rate_tables (
    id SERIAL PRIMARY KEY,
    client_id TEXT NOT NULL,
    treaty_id TEXT NOT NULL,
    benefit_id TEXT NOT NULL,
    age INTEGER NOT NULL,
    gender TEXT NOT NULL,
    smoker_status TEXT,
    rate NUMERIC NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP,
    created_by TEXT
);

CREATE TABLE IF NOT EXISTS validation_results (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    treaty_id TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    file_name TEXT NOT NULL,
    category TEXT NOT NULL,
    year TEXT NOT NULL,
    period TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    failed_count INTEGER,
    total_count INTEGER,
    message TEXT,
    checked_at TIMESTAMP
);

-- 5. GOLD LAYER & SOA RECONCILIATION
CREATE TABLE IF NOT EXISTS soa_entries (
    id SERIAL PRIMARY KEY,
    client_id TEXT NOT NULL,
    year TEXT NOT NULL,
    period TEXT NOT NULL,
    period_type TEXT NOT NULL,
    premium_soa NUMERIC NOT NULL,
    claims_soa NUMERIC NOT NULL,
    net_soa NUMERIC,
    status TEXT NOT NULL,
    entered_by TEXT NOT NULL,
    entered_at TIMESTAMP,
    approved_by TEXT,
    approved_at TIMESTAMP,
    rejection_reason TEXT,
    commission_soa NUMERIC,
    tax_soa NUMERIC,
    version INTEGER NOT NULL,
    additional_items JSONB
);

CREATE TABLE IF NOT EXISTS reconciliation_results (
    run_id TEXT,
    client_id TEXT,
    category TEXT,
    year TEXT,
    period TEXT,
    soa_amount DOUBLE PRECISION,
    file_amount DOUBLE PRECISION,
    variance DOUBLE PRECISION,
    status TEXT,
    checked_at TIMESTAMP
);