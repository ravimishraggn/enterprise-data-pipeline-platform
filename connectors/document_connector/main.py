"""
Document Connector (MinIO / S3)
================================
Monitors a MinIO bucket for new financial documents (PDFs, JSON reports, etc.)
and publishes metadata + content to Kafka topic `raw.documents`.

What this demonstrates:
  - Object-store polling pattern (S3-compatible via MinIO)
  - Checkpointing with a marker file to avoid reprocessing
  - Large payload handling (content stored in MinIO, Kafka carries the reference)
  - Document metadata extraction
"""

import asyncio
import io
import json
import os
import time
import uuid
import random
from datetime import datetime, timezone

from confluent_kafka import Producer
from minio import Minio
from minio.error import S3Error
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
import uvicorn
from fastapi import FastAPI
from starlette.responses import Response

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC", "raw.documents")
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS     = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET     = os.environ.get("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET", "financial-documents")
POLL_INTERVAL    = int(os.environ.get("POLL_INTERVAL_SECONDS", "15"))
CHECKPOINT_FILE  = "/tmp/doc_connector_checkpoint.json"

# ── Prometheus Metrics ────────────────────────────────────────────────────────

docs_produced = Counter(
    "pipeline_messages_produced_total",
    "Total messages produced to Kafka",
    ["topic", "service"]
)
doc_errors = Counter(
    "pipeline_processing_errors_total",
    "Processing errors",
    ["service", "error_type"]
)
doc_latency = Histogram(
    "pipeline_processing_duration_seconds",
    "Document processing latency",
    ["service"]
)
docs_in_bucket = Gauge("document_connector_bucket_objects", "Objects in monitored bucket")

# ── Checkpoint (avoid reprocessing already-seen objects) ──────────────────────

def load_checkpoint() -> set[str]:
    try:
        with open(CHECKPOINT_FILE) as f:
            return set(json.load(f).get("processed", []))
    except FileNotFoundError:
        return set()

def save_checkpoint(seen: set[str]) -> None:
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"processed": list(seen), "updated_at": datetime.now(timezone.utc).isoformat()}, f)

# ── Kafka Producer ────────────────────────────────────────────────────────────

def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "acks":              "all",
        "retries":           3,
        "client.id":         "document-connector",
    })

# ── Document seeder (creates synthetic docs in MinIO for demo) ───────────────

SYNTHETIC_DOCUMENTS = [
    {
        "doc_type": "TRADE_CONFIRMATION",
        "content": "Trade Confirmation #TC-2024-001\nSymbol: AAPL\nShares: 500\nPrice: $182.50\nTotal: $91,250.00\nAccount: ACC-001\nDate: 2024-01-15",
        "tags": ["trade", "equity", "confirmation"],
    },
    {
        "doc_type": "RISK_REPORT",
        "content": "Daily Risk Report Q1 2024\nPortfolio VaR (95%): $2.3M\nTop Risk Factor: Interest Rate Duration\nStress Test Result: PASS\nExposure by Asset Class: Equities 45%, Fixed Income 35%, FX 20%",
        "tags": ["risk", "report", "daily"],
    },
    {
        "doc_type": "COMPLIANCE_MEMO",
        "content": "Compliance Notice: AML Screening Results\nDate: 2024-01-15\nScreening Batch: BATCH-2024-01-15-001\nRecords Screened: 15,234\nFlags Raised: 3\nStatus: Under Review",
        "tags": ["compliance", "aml", "screening"],
    },
    {
        "doc_type": "MARKET_RESEARCH",
        "content": "Market Research: Technology Sector Outlook Q1 2024\nRating: OVERWEIGHT\nKey Themes: AI Infrastructure, Cloud Migration\nTop Picks: MSFT, NVDA, GOOGL\nPrice Target MSFT: $420\nRisk: Regulatory headwinds in EU",
        "tags": ["research", "equity", "technology"],
    },
]

def seed_documents(client: Minio) -> int:
    """Upload synthetic documents to MinIO bucket if not already present."""
    seeded = 0
    for i, doc in enumerate(SYNTHETIC_DOCUMENTS):
        obj_name = f"synthetic/{doc['doc_type'].lower()}_{i:03d}.txt"
        try:
            client.stat_object(MINIO_BUCKET, obj_name)
        except S3Error:
            content = doc["content"].encode()
            client.put_object(
                MINIO_BUCKET,
                obj_name,
                io.BytesIO(content),
                length=len(content),
                content_type="text/plain",
                metadata={
                    "doc-type": doc["doc_type"],
                    "tags": ",".join(doc["tags"]),
                    "created-by": "document-connector-seed",
                },
            )
            seeded += 1
    return seeded

# ── Poll loop ─────────────────────────────────────────────────────────────────

async def poll_loop(producer: Producer, client: Minio):
    seen = load_checkpoint()
    print(f"[INFO] Document Connector started. Checkpoint: {len(seen)} already-processed objects")

    # Seed synthetic documents on first run
    try:
        n = seed_documents(client)
        if n > 0:
            print(f"[INFO] Seeded {n} synthetic documents into MinIO bucket '{MINIO_BUCKET}'")
    except Exception as e:
        print(f"[WARN] Could not seed documents: {e}")

    while True:
        start = time.monotonic()
        try:
            objects = list(client.list_objects(MINIO_BUCKET, recursive=True))
            docs_in_bucket.set(len(objects))

            new_docs = [o for o in objects if o.object_name not in seen]
            processed = 0

            for obj in new_docs:
                try:
                    # Read object content (for small docs; large docs → send reference only)
                    response  = client.get_object(MINIO_BUCKET, obj.object_name)
                    raw_bytes = response.read()
                    response.close()

                    # Build Kafka message: metadata + content inline (or reference for large files)
                    stat = client.stat_object(MINIO_BUCKET, obj.object_name)
                    record = {
                        "document_id":    str(uuid.uuid4()),
                        "source":         "document-connector",
                        "source_system":  "minio",
                        "bucket":         MINIO_BUCKET,
                        "object_name":    obj.object_name,
                        "object_size":    obj.size,
                        "content_type":   stat.content_type,
                        "doc_type":       (stat.metadata or {}).get("x-amz-meta-doc-type", "UNKNOWN"),
                        "tags":           (stat.metadata or {}).get("x-amz-meta-tags", ""),
                        "last_modified":  obj.last_modified.isoformat() if obj.last_modified else None,
                        "schema_version": "1.0",
                        "event_timestamp": datetime.now(timezone.utc).isoformat(),
                        # Inline small content; in production large files stay in MinIO
                        "content_preview": raw_bytes[:2000].decode("utf-8", errors="replace"),
                        "content_truncated": len(raw_bytes) > 2000,
                        "minio_reference": f"s3://{MINIO_BUCKET}/{obj.object_name}",
                    }

                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=record["document_id"].encode(),
                        value=json.dumps(record).encode(),
                    )
                    producer.flush()

                    seen.add(obj.object_name)
                    processed += 1
                    docs_produced.labels(topic=KAFKA_TOPIC, service="document-connector").inc()

                except Exception as e:
                    doc_errors.labels(service="document-connector", error_type="object_read_error").inc()
                    print(f"[ERROR] Failed to process {obj.object_name}: {e}")

            if processed > 0:
                save_checkpoint(seen)
                elapsed = time.monotonic() - start
                doc_latency.labels(service="document-connector").observe(elapsed)
                print(f"[INFO] Processed {processed} new documents in {elapsed:.3f}s")
            else:
                print(f"[INFO] No new documents found. Sleeping {POLL_INTERVAL}s ...")

        except Exception as e:
            doc_errors.labels(service="document-connector", error_type="poll_error").inc()
            print(f"[ERROR] Poll error: {e}")

        await asyncio.sleep(POLL_INTERVAL)

# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(title="Document Connector", version="1.0.0")
_minio_client: Minio = None
_producer: Producer = None

@app.on_event("startup")
async def startup():
    global _minio_client, _producer
    _minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    _producer     = make_producer()
    asyncio.create_task(poll_loop(_producer, _minio_client))

@app.get("/health")
def health():
    return {"status": "ok", "service": "document-connector", "bucket": MINIO_BUCKET}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/seed")
def seed():
    """Re-seed synthetic documents into MinIO (for testing)."""
    n = seed_documents(_minio_client)
    return {"seeded": n, "bucket": MINIO_BUCKET}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8004, log_level="info")
