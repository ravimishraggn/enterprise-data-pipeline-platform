-- ============================================================
-- Financial DB initialization
-- Run automatically by PostgreSQL container on first start
-- ============================================================

-- ── CDC source tables (Debezium watches these) ────────────

CREATE TABLE IF NOT EXISTS transactions (
    id              BIGSERIAL PRIMARY KEY,
    transaction_id  UUID NOT NULL DEFAULT gen_random_uuid(),
    account_id      VARCHAR(20) NOT NULL,
    counterparty_id VARCHAR(20),
    amount          NUMERIC(18, 4) NOT NULL,
    currency        CHAR(3) NOT NULL DEFAULT 'USD',
    transaction_type VARCHAR(30) NOT NULL,   -- TRANSFER, PAYMENT, TRADE, DEPOSIT, WITHDRAWAL
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    risk_score      NUMERIC(5, 4),
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market_prices (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(10) NOT NULL,
    price       NUMERIC(18, 6) NOT NULL,
    bid         NUMERIC(18, 6),
    ask         NUMERIC(18, 6),
    volume      BIGINT,
    source      VARCHAR(50) NOT NULL DEFAULT 'synthetic',
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Feature store tables (pipeline writes enriched data here) ─

CREATE TABLE IF NOT EXISTS processed_transactions (
    id                  BIGSERIAL PRIMARY KEY,
    transaction_id      UUID NOT NULL,
    source_topic        VARCHAR(100),
    account_id          VARCHAR(20),
    amount              NUMERIC(18, 4),
    currency            CHAR(3),
    transaction_type    VARCHAR(30),
    risk_score          NUMERIC(5, 4),
    risk_label          VARCHAR(20),         -- LOW, MEDIUM, HIGH, CRITICAL
    enrichment_tags     TEXT[],
    pii_detected        BOOLEAN DEFAULT FALSE,
    processing_latency_ms INTEGER,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload         JSONB
);

-- ── Lineage tracking ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS lineage_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL DEFAULT gen_random_uuid(),
    pipeline_run_id UUID,
    entity_id       VARCHAR(100) NOT NULL,  -- transaction_id, document_id, etc.
    entity_type     VARCHAR(50) NOT NULL,
    source_system   VARCHAR(100),
    source_topic    VARCHAR(100),
    destination     VARCHAR(100),
    transformation  VARCHAR(200),
    schema_version  VARCHAR(20),
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB DEFAULT '{}'
);

-- ── PII audit log ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pii_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    source_topic    VARCHAR(100),
    message_key     VARCHAR(200),
    pii_types       TEXT[],           -- ['email', 'phone', 'ssn', ...]
    action_taken    VARCHAR(50),      -- REDACTED, FLAGGED, ALLOWED
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Governance: schema catalog ─────────────────────────────

CREATE TABLE IF NOT EXISTS schema_catalog (
    id              BIGSERIAL PRIMARY KEY,
    subject         VARCHAR(200) NOT NULL UNIQUE,
    schema_json     JSONB NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    compatibility   VARCHAR(30) DEFAULT 'BACKWARD',
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Indexes ────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_transactions_account    ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created    ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_status     ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_processed_txn_id        ON processed_transactions(transaction_id);
CREATE INDEX IF NOT EXISTS idx_lineage_entity          ON lineage_events(entity_id, entity_type);
CREATE INDEX IF NOT EXISTS idx_lineage_run             ON lineage_events(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_market_symbol           ON market_prices(symbol, captured_at DESC);

-- ── Logical replication publication (Debezium needs this) ──

SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_slot'
);

CREATE PUBLICATION financial_pub FOR TABLE transactions, market_prices;

-- ── Seed data ──────────────────────────────────────────────

INSERT INTO transactions (account_id, counterparty_id, amount, currency, transaction_type, status, risk_score)
VALUES
    ('ACC-001', 'ACC-002', 1500.00, 'USD', 'TRANSFER', 'COMPLETED', 0.12),
    ('ACC-003', NULL,      50000.00,'USD', 'DEPOSIT',  'COMPLETED', 0.05),
    ('ACC-001', 'ACC-099', 9999.99, 'USD', 'PAYMENT',  'PENDING',   0.78),
    ('ACC-004', 'ACC-002', 250.50,  'EUR', 'TRANSFER', 'COMPLETED', 0.08)
ON CONFLICT DO NOTHING;

INSERT INTO market_prices (symbol, price, bid, ask, volume)
VALUES
    ('AAPL',  182.50, 182.48, 182.52, 15234567),
    ('GOOGL', 141.20, 141.18, 141.22, 8901234),
    ('MSFT',  376.80, 376.78, 376.82, 12345678),
    ('BTC-USD', 43500.00, 43490.00, 43510.00, 5678)
ON CONFLICT DO NOTHING;
