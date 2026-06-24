"""
Document Processor
==================
Consumes from `raw.documents`, chunks the content into overlapping segments,
generates a simple TF-IDF-style embedding (real projects use sentence-transformers),
and publishes to `processed.documents` for ingestion into OpenSearch.

What this demonstrates:
  - Text chunking strategy (fixed-size with overlap)
  - Embedding pattern (simplified; see LEARNING.md for production approach)
  - Metadata preservation through the pipeline
  - Batch Kafka production for chunks of a single document
"""

import asyncio
import json
import math
import os
import re
import time
import uuid
from datetime import datetime, timezone
from threading import Thread

from confluent_kafka import Consumer, Producer, KafkaError
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import FastAPI
from starlette.responses import Response
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
INPUT_TOPIC     = os.environ.get("KAFKA_INPUT_TOPIC", "raw.documents")
OUTPUT_TOPIC    = os.environ.get("KAFKA_OUTPUT_TOPIC", "processed.documents")
CHUNK_SIZE      = int(os.environ.get("CHUNK_SIZE", "500"))      # characters
CHUNK_OVERLAP   = int(os.environ.get("CHUNK_OVERLAP", "100"))   # characters overlap

# ── Prometheus Metrics ────────────────────────────────────────────────────────

docs_processed = Counter(
    "pipeline_messages_consumed_total",
    "Documents consumed",
    ["topic", "service"]
)
chunks_produced = Counter(
    "pipeline_messages_produced_total",
    "Chunks produced to Kafka",
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

# ── Chunker ───────────────────────────────────────────────────────────────────

def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """
    Split text into overlapping chunks.
    Tries to break at sentence boundaries within the chunk window.
    """
    text   = text.strip()
    chunks = []
    start  = 0

    while start < len(text):
        end = start + chunk_size
        if end >= len(text):
            chunks.append(text[start:])
            break

        # Try to break at sentence boundary (. ! ?) within last 100 chars
        boundary = text.rfind(".", start, end)
        if boundary == -1 or boundary < start + chunk_size // 2:
            boundary = end

        chunks.append(text[start:boundary + 1].strip())
        start = max(start + 1, boundary + 1 - overlap)

    return [c for c in chunks if c]

# ── Simple embedding (TF-IDF inspired, deterministic) ────────────────────────
# In production: use sentence-transformers, OpenAI embeddings, or Bedrock.
# We build a 64-dim vector from character n-gram frequencies as a placeholder.

VOCAB_TOKENS = [
    "trade", "risk", "market", "price", "transaction", "account",
    "compliance", "alert", "fraud", "report", "portfolio", "equity",
    "bond", "derivative", "settlement", "clearance", "position",
    "exposure", "rating", "threshold", "limit", "volatility",
    "hedge", "option", "future", "swap", "currency", "rate",
    "credit", "default", "aml", "kyc", "regulatory", "audit",
    "transfer", "payment", "deposit", "withdrawal", "balance",
    "statement", "confirmation", "reconciliation", "booking",
    "execution", "order", "fill", "counterparty", "custodian",
    "fund", "capital", "margin", "collateral", "netting", "cva",
    "var", "stress", "scenario", "backtest", "model", "factor",
    "alpha", "beta", "gamma", "delta", "vega", "theta", "rho"
]

def simple_embed(text: str) -> list[float]:
    """
    Placeholder embedding: 64-dim TF-IDF-style vector over domain vocabulary.
    Replace with `SentenceTransformer('all-MiniLM-L6-v2').encode(text)` in production.
    """
    text_lower = text.lower()
    words      = re.findall(r"\b\w+\b", text_lower)
    word_count = max(len(words), 1)

    vector = []
    for token in VOCAB_TOKENS:
        tf = sum(1 for w in words if w == token) / word_count
        # IDF approximation — use log(100) as doc count proxy
        idf  = math.log(100 / (1 + text_lower.count(token)))
        vector.append(round(tf * idf, 6))

    # L2 normalize
    norm = math.sqrt(sum(v * v for v in vector)) or 1.0
    return [round(v / norm, 6) for v in vector]

# ── Document processing ───────────────────────────────────────────────────────

def process_document(raw: dict, producer: Producer) -> int:
    """Chunk + embed a document and produce each chunk to Kafka. Returns chunk count."""
    document_id  = raw.get("document_id", str(uuid.uuid4()))
    content      = raw.get("content_preview", "")
    doc_type     = raw.get("doc_type", "UNKNOWN")
    tags         = raw.get("tags", "")
    minio_ref    = raw.get("minio_reference", "")

    if not content.strip():
        return 0

    chunks = chunk_text(content)
    produced = 0

    for i, chunk in enumerate(chunks):
        embedding = simple_embed(chunk)
        chunk_record = {
            "chunk_id":          str(uuid.uuid4()),
            "document_id":       document_id,
            "chunk_index":       i,
            "total_chunks":      len(chunks),
            "content":           chunk,
            "embedding":         embedding,
            "embedding_dim":     len(embedding),
            "embedding_model":   "vocab-tfidf-v1-placeholder",
            "doc_type":          doc_type,
            "tags":              tags,
            "minio_reference":   minio_ref,
            "bucket":            raw.get("bucket"),
            "object_name":       raw.get("object_name"),
            "source":            "document-processor",
            "source_document_id": document_id,
            "schema_version":    "1.0",
            "processed_at":      datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(
            OUTPUT_TOPIC,
            key=chunk_record["chunk_id"].encode(),
            value=json.dumps(chunk_record).encode(),
        )
        produced += 1

    producer.flush()
    return produced

# ── Consumer loop ─────────────────────────────────────────────────────────────

def consumer_loop():
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_SERVERS,
        "group.id":           "document-processor-group",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "acks":              "all",
        "client.id":         "document-processor",
    })
    consumer.subscribe([INPUT_TOPIC])
    print(f"[INFO] Document Processor subscribed to: {INPUT_TOPIC}")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                doc_errors.labels(service="document-processor", error_type="kafka_error").inc()
            continue

        start = time.monotonic()
        try:
            raw   = json.loads(msg.value().decode("utf-8"))
            n     = process_document(raw, producer)
            docs_processed.labels(topic=INPUT_TOPIC, service="document-processor").inc()
            chunks_produced.labels(topic=OUTPUT_TOPIC, service="document-processor").inc(n)
            doc_latency.labels(service="document-processor").observe(time.monotonic() - start)
            print(f"[INFO] Processed document {raw.get('document_id')} → {n} chunks")
        except Exception as e:
            doc_errors.labels(service="document-processor", error_type="processing_error").inc()
            print(f"[ERROR] {e}")

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="Document Processor", version="1.0.0")

@app.on_event("startup")
async def startup():
    t = Thread(target=consumer_loop, daemon=True)
    t.start()

@app.get("/health")
def health():
    return {"status": "ok", "service": "document-processor"}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8005, log_level="info")
