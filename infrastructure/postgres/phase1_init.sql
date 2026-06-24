-- ============================================================
-- Phase 1: market_data schema
-- Created automatically on first container start
-- ============================================================

CREATE TABLE IF NOT EXISTS market_prices (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL,
    source_system   VARCHAR(100) NOT NULL,
    entity_id       VARCHAR(20) NOT NULL,      -- e.g. HDFC.NS
    event_type      VARCHAR(50) NOT NULL,
    price           NUMERIC(18, 4) NOT NULL,
    currency        CHAR(3) NOT NULL DEFAULT 'INR',
    schema_version  VARCHAR(10) NOT NULL,
    kafka_topic     VARCHAR(100),
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    event_timestamp TIMESTAMPTZ NOT NULL,
    stored_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_entity  ON market_prices(entity_id);
CREATE INDEX IF NOT EXISTS idx_market_ts      ON market_prices(event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_market_event   ON market_prices(event_id);
