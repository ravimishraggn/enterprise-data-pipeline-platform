# ADR-002: Debezium for Change Data Capture

**Status:** Accepted  
**Date:** 2024-01-15  
**Domain:** Data Ingestion / CDC

---

## Context

We need to capture every change to the `transactions` and `market_prices` tables in
PostgreSQL — not just the current state, but the full history of changes (inserts,
updates, deletes). The options are:

1. **Polling** — `SELECT * FROM transactions WHERE updated_at > :last_seen`
2. **Triggers** — DB triggers write changes to an outbox table, app reads outbox
3. **Debezium** — reads PostgreSQL's Write-Ahead Log (WAL) directly
4. **AWS DMS** — AWS Database Migration Service (managed CDC)

---

## Decision

We chose **Debezium with PostgreSQL logical replication** (pgoutput plugin).

---

## Rationale

### Polling: Why We Rejected It

```sql
-- Polling query (runs every N seconds):
SELECT * FROM transactions WHERE updated_at > '2024-01-15 14:00:00'
```

Problems:
- **Missed deletes**: If a row is hard-deleted, polling never sees it
- **Schema dependency**: Requires an `updated_at` column on every table
- **Database load**: Every poll hits the primary, even when nothing changed
- **Race conditions**: Records updated between polls can be missed if clock drift exists

### Outbox Pattern: Close But Not Quite

The outbox pattern is solid but adds application complexity:
```sql
-- Application writes to outbox:
INSERT INTO outbox (event_type, payload, published) VALUES ('TRANSACTION_CREATED', $1, false);
-- Separate process reads and marks published
```
Requires changes to every application writing to the database. Debezium needs no
application changes at all.

### Why Debezium Wins

Debezium reads PostgreSQL's **WAL (Write-Ahead Log)** — the same mechanism PostgreSQL
uses for replication to standby nodes. This means:

1. **Zero application changes** — the database doesn't know Debezium exists
2. **Captures everything** — INSERTs, UPDATEs, DELETEs, even schema changes (DDL)
3. **Before AND after images** — you see the row state before and after the change
4. **Consistent ordering** — WAL events are ordered by LSN (Log Sequence Number)
5. **No extra load** — reads the replication stream, not the primary query path

### Debezium Architecture

```
PostgreSQL Primary
    │
    │  WAL (logical replication protocol)
    ▼
Debezium Kafka Connect Task
    │  reads from replication slot: debezium_slot
    │  decodes using: pgoutput plugin
    ▼
Kafka Topic: cdc.public.transactions
    │
    ▼  (consumed by stream_processor)
processed.transactions
```

### Event Format

```json
{
  "op": "c",           // c=create, u=update, d=delete, r=read(snapshot)
  "ts_ms": 1705328400000,
  "before": null,      // null for inserts
  "after": {
    "id": 42,
    "transaction_id": "uuid-here",
    "amount": 9999.99,
    "status": "PENDING"
  },
  "source": {
    "db": "financial_db",
    "table": "transactions",
    "lsn": 12345678
  }
}
```

---

## Consequences

**Positive:**
- Full audit trail including deletes
- Sub-second latency from DB write to Kafka
- Works with existing schema, no triggers or outbox tables

**Negative:**
- PostgreSQL must have `wal_level=logical` (not default for all deployments)
- Replication slot holds WAL until consumed — monitor slot lag
- Debezium requires Kafka Connect cluster (we use the official Docker image)

---

## In This Project

See `connectors/cdc_connector/register_connector.py` to register the connector
after the stack is up. The PostgreSQL init script (`infrastructure/postgres/init.sql`)
creates the replication slot and publication automatically.
