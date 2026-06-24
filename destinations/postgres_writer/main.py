"""
PostgreSQL Writer
=================
Consumes from `processed.transactions` and writes enriched records to the
`processed_transactions` table in PostgreSQL (feature store).

What this demonstrates:
  - asyncpg for async PostgreSQL writes
  - Batched UPSERT using ON CONFLICT DO UPDATE
  - Connection pool management
  - Idempotent writes (same transaction_id can be processed multiple times safely)
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from threading import Thread

import asyncpg
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import FastAPI
from starlette.responses import Response
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
INPUT_TOPIC    = os.environ.get("KAFKA_INPUT_TOPIC", "processed.transactions")
POSTGRES_DSN   = os.environ.get("POSTGRES_DSN", "postgresql://pipeline:pipeline123@localhost:5432/financial_db")
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE", "20"))
FLUSH_INTERVAL = float(os.environ.get("FLUSH_INTERVAL_SECONDS", "3.0"))
SERVICE_PORT   = int(os.environ.get("SERVICE_PORT", "8005"))

# ── Prometheus ────────────────────────────────────────────────────────────────

msgs_consumed = Counter(
    "pipeline_messages_consumed_total",
    "Messages consumed",
    ["topic", "service"]
)
rows_written = Counter(
    "pipeline_messages_produced_total",
    "Rows written to PostgreSQL",
    ["topic", "service"]
)
write_errors = Counter(
    "pipeline_processing_errors_total",
    "Write errors",
    ["service", "error_type"]
)
write_latency = Histogram(
    "pipeline_processing_duration_seconds",
    "Batch write latency",
    ["service"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)

# ── Writer ────────────────────────────────────────────────────────────────────

UPSERT_SQL = """
INSERT INTO processed_transactions (
    transaction_id, source_topic, account_id, amount, currency,
    transaction_type, risk_score, risk_label, enrichment_tags,
    pii_detected, processing_latency_ms, processed_at, raw_payload
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT DO NOTHING
"""

async def write_batch(pool: asyncpg.Pool, batch: list[dict]) -> int:
    records = []
    for record in batch:
        records.append((
            record.get("transaction_id"),
            record.get("source_topic") or record.get("source"),
            record.get("account_id"),
            float(record.get("amount", 0)),
            record.get("currency", "USD"),
            record.get("transaction_type"),
            float(record.get("risk_score", 0)),
            record.get("risk_label"),
            record.get("enrichment_tags", []),
            bool(record.get("pii_detected", False)),
            None,
            datetime.now(timezone.utc),
            json.dumps(record),
        ))

    async with pool.acquire() as conn:
        await conn.executemany(UPSERT_SQL, records)
    return len(records)

# ── Consumer loop ─────────────────────────────────────────────────────────────

def consumer_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _run():
        pool = None
        for _ in range(30):
            try:
                pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=5)
                break
            except Exception as e:
                print(f"[WARN] PostgreSQL not ready: {e}. Retrying...")
                await asyncio.sleep(5)

        if not pool:
            print("[ERROR] Could not connect to PostgreSQL. Exiting consumer.")
            return

        consumer = Consumer({
            "bootstrap.servers":  KAFKA_SERVERS,
            "group.id":           "postgres-writer-group",
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([INPUT_TOPIC])
        print(f"[INFO] PostgreSQL Writer subscribed to: {INPUT_TOPIC}")

        buffer     = []
        last_flush = time.monotonic()

        async def flush():
            nonlocal last_flush
            if not buffer:
                return
            start = time.monotonic()
            try:
                n = await write_batch(pool, buffer)
                rows_written.labels(topic=INPUT_TOPIC, service="postgres-writer").inc(n)
                write_latency.labels(service="postgres-writer").observe(time.monotonic() - start)
                print(f"[INFO] Wrote {n} rows to PostgreSQL")
                consumer.commit()
            except Exception as e:
                write_errors.labels(service="postgres-writer", error_type="db_write_error").inc()
                print(f"[ERROR] DB write error: {e}")
            buffer.clear()
            last_flush = time.monotonic()

        while True:
            msg = consumer.poll(timeout=0.5)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        write_errors.labels(service="postgres-writer", error_type="kafka_error").inc()
                    continue
                msgs_consumed.labels(topic=INPUT_TOPIC, service="postgres-writer").inc()
                try:
                    record = json.loads(msg.value().decode())
                    buffer.append(record)
                except Exception as e:
                    write_errors.labels(service="postgres-writer", error_type="json_error").inc()
                    print(f"[ERROR] JSON decode: {e}")

            if len(buffer) >= BATCH_SIZE or (time.monotonic() - last_flush) >= FLUSH_INTERVAL:
                await flush()

            await asyncio.sleep(0)

    loop.run_until_complete(_run())

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="PostgreSQL Writer", version="1.0.0")

@app.on_event("startup")
async def startup():
    t = Thread(target=consumer_loop, daemon=True)
    t.start()

@app.get("/health")
def health():
    return {"status": "ok", "service": "postgres-writer"}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=SERVICE_PORT, log_level="info")
