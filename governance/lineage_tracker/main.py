"""
Lineage Tracker
===============
Consumes from `audit.lineage` and persists lineage events to PostgreSQL,
then exposes a REST API for querying the data lineage graph.

Data lineage answers:
  - "Where did this record come from?"
  - "What transformations were applied?"
  - "Which downstream systems hold a copy of this data?"
  - "If this source data changes, what is impacted?"

What this demonstrates:
  - Data lineage as a first-class concern
  - Lineage graph storage in PostgreSQL (adjacency list model)
  - REST API for lineage queries (for a data catalog / governance UI)
  - OpenLineage-inspired event format
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from threading import Thread
from typing import Optional

import asyncpg
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
LINEAGE_TOPIC   = os.environ.get("KAFKA_INPUT_TOPIC", "audit.lineage")
POSTGRES_DSN    = os.environ.get("POSTGRES_DSN", "postgresql://pipeline:pipeline123@localhost:5432/financial_db")
SERVICE_PORT    = int(os.environ.get("SERVICE_PORT", "8010"))

# ── Prometheus ────────────────────────────────────────────────────────────────

lineage_events = Counter(
    "lineage_events_tracked_total",
    "Lineage events consumed and stored",
    ["entity_type"]
)
msgs_consumed = Counter(
    "pipeline_messages_consumed_total",
    "Messages consumed",
    ["topic", "service"]
)
store_errors = Counter(
    "pipeline_processing_errors_total",
    "Storage errors",
    ["service", "error_type"]
)
store_latency = Histogram(
    "pipeline_processing_duration_seconds",
    "Lineage event storage latency",
    ["service"]
)

# ── INSERT SQL ────────────────────────────────────────────────────────────────

INSERT_LINEAGE = """
INSERT INTO lineage_events (
    event_id, pipeline_run_id, entity_id, entity_type,
    source_system, source_topic, destination,
    transformation, schema_version, event_timestamp, metadata
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT DO NOTHING
"""

# ── Consumer loop ─────────────────────────────────────────────────────────────

def consumer_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _run():
        pool = None
        for _ in range(30):
            try:
                pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=1, max_size=3)
                break
            except Exception as e:
                print(f"[WARN] PostgreSQL not ready: {e}")
                await asyncio.sleep(5)

        if not pool:
            print("[ERROR] PostgreSQL unavailable. Lineage events will not be persisted.")
            return

        consumer = Consumer({
            "bootstrap.servers":  KAFKA_SERVERS,
            "group.id":           "lineage-tracker-group",
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": True,
        })
        consumer.subscribe([LINEAGE_TOPIC])
        print(f"[INFO] Lineage Tracker subscribed to: {LINEAGE_TOPIC}")

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                await asyncio.sleep(0)
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    store_errors.labels(service="lineage-tracker", error_type="kafka_error").inc()
                await asyncio.sleep(0)
                continue

            msgs_consumed.labels(topic=LINEAGE_TOPIC, service="lineage-tracker").inc()
            start = time.monotonic()

            try:
                event = json.loads(msg.value().decode())
                await pool.execute(INSERT_LINEAGE,
                    event.get("event_id"),
                    event.get("pipeline_run_id"),
                    event.get("entity_id"),
                    event.get("entity_type", "UNKNOWN"),
                    event.get("source_system"),
                    event.get("source_topic"),
                    event.get("destination"),
                    event.get("transformation"),
                    event.get("schema_version"),
                    datetime.fromisoformat(event["event_timestamp"]) if event.get("event_timestamp") else datetime.now(timezone.utc),
                    json.dumps(event.get("metadata", {})),
                )
                lineage_events.labels(entity_type=event.get("entity_type", "UNKNOWN")).inc()
                store_latency.labels(service="lineage-tracker").observe(time.monotonic() - start)

            except Exception as e:
                store_errors.labels(service="lineage-tracker", error_type="db_error").inc()
                print(f"[ERROR] Lineage store error: {e}")

            await asyncio.sleep(0)

    loop.run_until_complete(_run())

# ── FastAPI app + lineage query API ──────────────────────────────────────────

app = FastAPI(
    title="Lineage Tracker",
    description="Query data lineage for any entity in the pipeline",
    version="1.0.0",
)
_pool: asyncpg.Pool = None

@app.on_event("startup")
async def startup():
    global _pool
    for _ in range(20):
        try:
            _pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=1, max_size=3)
            break
        except Exception:
            await asyncio.sleep(3)
    t = Thread(target=consumer_loop, daemon=True)
    t.start()

@app.get("/health")
def health():
    return {"status": "ok", "service": "lineage-tracker"}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/lineage/{entity_id}")
async def get_lineage(entity_id: str):
    """Get the full lineage chain for an entity (e.g. a transaction_id)."""
    if not _pool:
        raise HTTPException(status_code=503, detail="Database not connected")

    rows = await _pool.fetch(
        """
        SELECT event_id, pipeline_run_id, entity_id, entity_type,
               source_system, source_topic, destination, transformation,
               schema_version, event_timestamp, metadata
        FROM lineage_events
        WHERE entity_id = $1
        ORDER BY event_timestamp ASC
        """,
        entity_id,
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No lineage found for entity: {entity_id}")

    return {
        "entity_id":    entity_id,
        "event_count":  len(rows),
        "lineage_chain": [dict(r) for r in rows],
    }

@app.get("/lineage/run/{pipeline_run_id}")
async def get_run_lineage(pipeline_run_id: str):
    """Get all lineage events for a single pipeline run."""
    if not _pool:
        raise HTTPException(status_code=503, detail="Database not connected")

    rows = await _pool.fetch(
        """
        SELECT * FROM lineage_events
        WHERE pipeline_run_id = $1
        ORDER BY event_timestamp ASC
        """,
        pipeline_run_id,
    )
    return {"pipeline_run_id": pipeline_run_id, "events": [dict(r) for r in rows]}

@app.get("/lineage/recent")
async def recent_lineage(limit: int = 20, entity_type: Optional[str] = None):
    """Get the most recent lineage events."""
    if not _pool:
        raise HTTPException(status_code=503, detail="Database not connected")

    if entity_type:
        rows = await _pool.fetch(
            "SELECT * FROM lineage_events WHERE entity_type=$1 ORDER BY event_timestamp DESC LIMIT $2",
            entity_type, limit
        )
    else:
        rows = await _pool.fetch(
            "SELECT * FROM lineage_events ORDER BY event_timestamp DESC LIMIT $1", limit
        )
    return {"events": [dict(r) for r in rows], "count": len(rows)}

@app.get("/lineage/stats")
async def lineage_stats():
    """Summary statistics about tracked lineage."""
    if not _pool:
        raise HTTPException(status_code=503, detail="Database not connected")

    total   = await _pool.fetchval("SELECT COUNT(*) FROM lineage_events")
    by_type = await _pool.fetch(
        "SELECT entity_type, COUNT(*) as count FROM lineage_events GROUP BY entity_type ORDER BY count DESC"
    )
    sources = await _pool.fetch(
        "SELECT source_system, COUNT(*) as count FROM lineage_events GROUP BY source_system ORDER BY count DESC LIMIT 10"
    )
    return {
        "total_events":      total,
        "by_entity_type":    [dict(r) for r in by_type],
        "top_source_systems": [dict(r) for r in sources],
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=SERVICE_PORT, log_level="info")
