# CDC Connector (Debezium) — Learning Notes

## What Is CDC?

**Change Data Capture (CDC)** tracks every INSERT, UPDATE, and DELETE in a database
and streams those changes as events to consumers. Think of it as a live transcript of
everything happening in your database.

```
INSERT → {op: "c", after: {id: 1, amount: 9999, ...}}
UPDATE → {op: "u", before: {status: "PENDING"}, after: {status: "COMPLETED"}}
DELETE → {op: "d", before: {id: 1, amount: 9999, ...}}
```

## How Debezium Works (PostgreSQL)

PostgreSQL has a feature called **Logical Replication** that exposes a stream of all
changes in a format that can be consumed by external systems. This is the same
mechanism used for database replication (primary → replica).

```
PostgreSQL WAL (Write-Ahead Log)
       ↓  (logical decoding via pgoutput plugin)
Debezium Kafka Connect Task
       ↓  (reads replication slot, decodes change events)
Kafka Topic: cdc.public.transactions
       ↓  (consumers read and react)
Your pipeline
```

### Key PostgreSQL Config

We set these in docker-compose.yml's postgres command:
```
wal_level=logical          # enable logical replication (default is "replica")
max_replication_slots=10   # max concurrent CDC consumers
max_wal_senders=10         # max WAL streaming connections
```

### Key Debezium Config

| Config | Value | Why |
|--------|-------|-----|
| `plugin.name` | `pgoutput` | Native PG 10+ decoder, no extension install needed |
| `publication.name` | `financial_pub` | Must match `CREATE PUBLICATION` in init.sql |
| `slot.name` | `debezium_slot` | Replication slot — PG buffers WAL until this consumer reads it |
| `snapshot.mode` | `initial` | Read all existing rows first, then stream new changes |

## The `op` Field

Every Debezium event has an `op` field indicating the change type:
- `"c"` → Create (INSERT)
- `"u"` → Update (UPDATE)
- `"d"` → Delete (DELETE)
- `"r"` → Read (initial snapshot)

The `before` and `after` fields contain the row state before and after the change.
For inserts, `before` is null. For deletes, `after` is null.

## Why Not Just Query the Database?

| Approach | Latency | Load on DB | Missed deletes | Back-pressure |
|----------|---------|------------|----------------|---------------|
| Polling (`SELECT * WHERE updated_at > ?`) | Poll interval | High (every poll) | Yes (if hard delete) | No |
| CDC (Debezium) | Near-zero | Very low | No | Yes (via Kafka offset) |

CDC is the right choice for financial data where:
- You need to know about **every change**, not just the latest state
- Deletes and corrections matter (regulatory audit trail)
- You can't add load to the production transaction database

## Important: Replication Slot Backpressure

A PostgreSQL replication slot holds WAL segments until the consumer (Debezium) reads
them. If Debezium is down for a long time, WAL accumulates and **your disk fills up**.

In production, always monitor:
- `pg_replication_slots` view — check `pg_wal_lsn_diff` / lag
- Set `max_slot_wal_keep_size` to limit WAL accumulation

## Try It Out

```bash
# Register the connector (Kafka Connect + Debezium must be running)
python connectors/cdc_connector/register_connector.py

# Insert a row and watch it appear in Kafka
docker exec -it postgres psql -U pipeline -d financial_db -c \
  "INSERT INTO transactions (account_id, amount, currency, transaction_type) \
   VALUES ('ACC-TEST', 9999.99, 'USD', 'PAYMENT');"

# Check Kafka UI → topic: cdc.public.transactions
```
