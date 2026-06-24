"""
OpenSearch Writer
=================
Consumes from `processed.transactions` and `processed.documents` and indexes
records into OpenSearch. Handles index creation with appropriate mappings
(kNN vector field for document embeddings).

What this demonstrates:
  - OpenSearch index management (create if not exists, mapping with kNN)
  - Bulk indexing for throughput
  - Document routing (transactions vs document chunks → separate indices)
  - kNN vector search setup for RAG / semantic search
"""

import json
import os
import time
from datetime import datetime, timezone
from threading import Thread

import httpx
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import FastAPI
from starlette.responses import Response
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
INPUT_TOPICS   = os.environ.get("KAFKA_INPUT_TOPICS", "processed.transactions,processed.documents").split(",")
OS_HOST        = os.environ.get("OPENSEARCH_HOST", "http://localhost:9200")
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE", "50"))
FLUSH_INTERVAL = float(os.environ.get("FLUSH_INTERVAL_SECONDS", "2.0"))
SERVICE_PORT   = int(os.environ.get("SERVICE_PORT", "8004"))

# ── Prometheus Metrics ────────────────────────────────────────────────────────

msgs_consumed = Counter(
    "pipeline_messages_consumed_total",
    "Messages consumed",
    ["topic", "service"]
)
docs_indexed = Counter(
    "pipeline_messages_produced_total",
    "Documents indexed to OpenSearch",
    ["topic", "service"]
)
index_errors = Counter(
    "pipeline_processing_errors_total",
    "Indexing errors",
    ["service", "error_type"]
)
index_latency = Histogram(
    "pipeline_processing_duration_seconds",
    "Bulk index latency",
    ["service"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# ── Index Definitions ─────────────────────────────────────────────────────────

TRANSACTION_INDEX_MAPPING = {
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            "transaction_id":   {"type": "keyword"},
            "account_id":       {"type": "keyword"},
            "counterparty_id":  {"type": "keyword"},
            "amount":           {"type": "double"},
            "currency":         {"type": "keyword"},
            "transaction_type": {"type": "keyword"},
            "status":           {"type": "keyword"},
            "risk_score":       {"type": "float"},
            "risk_label":       {"type": "keyword"},
            "enrichment_tags":  {"type": "keyword"},
            "pii_detected":     {"type": "boolean"},
            "source":           {"type": "keyword"},
            "source_topic":     {"type": "keyword"},
            "processed_at":     {"type": "date"},
            "pipeline_run_id":  {"type": "keyword"},
        }
    }
}

DOCUMENT_INDEX_MAPPING = {
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,
        "knn": True,                  # Enable k-NN plugin
    },
    "mappings": {
        "properties": {
            "chunk_id":         {"type": "keyword"},
            "document_id":      {"type": "keyword"},
            "chunk_index":      {"type": "integer"},
            "total_chunks":     {"type": "integer"},
            "content":          {"type": "text", "analyzer": "standard"},
            "doc_type":         {"type": "keyword"},
            "tags":             {"type": "keyword"},
            "minio_reference":  {"type": "keyword"},
            "processed_at":     {"type": "date"},
            # kNN vector field — 64-dim matches our simple_embed() output
            "embedding": {
                "type":          "knn_vector",
                "dimension":     64,
                "method": {
                    "name":       "hnsw",
                    "space_type": "cosinesimil",
                    "engine":     "lucene",
                }
            }
        }
    }
}

# ── OpenSearch Client ─────────────────────────────────────────────────────────

class OpenSearchClient:
    def __init__(self, host: str):
        self.host    = host
        self.session = httpx.Client(timeout=30.0)

    def ensure_index(self, index: str, mapping: dict) -> None:
        r = self.session.head(f"{self.host}/{index}")
        if r.status_code == 404:
            r = self.session.put(f"{self.host}/{index}", json=mapping)
            r.raise_for_status()
            print(f"[INFO] Created index: {index}")

    def bulk_index(self, index: str, docs: list[dict]) -> dict:
        """Use OpenSearch Bulk API for efficient batch indexing."""
        lines = []
        for doc in docs:
            doc_id = doc.get("chunk_id") or doc.get("transaction_id") or doc.get("pipeline_run_id")
            lines.append(json.dumps({"index": {"_index": index, "_id": doc_id}}))
            lines.append(json.dumps(doc))
        body = "\n".join(lines) + "\n"

        r = self.session.post(
            f"{self.host}/_bulk",
            content=body.encode(),
            headers={"Content-Type": "application/x-ndjson"},
        )
        r.raise_for_status()
        result = r.json()

        # Count errors in bulk response
        errors = sum(1 for item in result.get("items", []) if "error" in item.get("index", {}))
        return {"took": result.get("took"), "errors": errors, "total": len(docs)}

    def search(self, index: str, query: dict) -> dict:
        r = self.session.post(f"{self.host}/{index}/_search", json=query)
        r.raise_for_status()
        return r.json()

# ── Consumer loop ─────────────────────────────────────────────────────────────

def consumer_loop():
    os_client = OpenSearchClient(OS_HOST)

    # Wait for OpenSearch to be ready
    for _ in range(30):
        try:
            r = httpx.get(f"{OS_HOST}/_cluster/health", timeout=5)
            if r.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(5)

    os_client.ensure_index("financial-transactions", TRANSACTION_INDEX_MAPPING)
    os_client.ensure_index("financial-documents",    DOCUMENT_INDEX_MAPPING)

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_SERVERS,
        "group.id":           "opensearch-writer-group",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(INPUT_TOPICS)
    print(f"[INFO] OpenSearch Writer subscribed to: {INPUT_TOPICS}")

    txn_buffer = []
    doc_buffer = []
    last_flush = time.monotonic()

    def flush_buffers():
        nonlocal last_flush
        if txn_buffer:
            start = time.monotonic()
            try:
                result = os_client.bulk_index("financial-transactions", txn_buffer)
                docs_indexed.labels(topic="financial-transactions", service="opensearch-writer").inc(result["total"])
                index_latency.labels(service="opensearch-writer").observe(time.monotonic() - start)
                print(f"[INFO] Indexed {result['total']} transactions (errors: {result['errors']})")
            except Exception as e:
                index_errors.labels(service="opensearch-writer", error_type="bulk_index_error").inc()
                print(f"[ERROR] Transaction bulk index failed: {e}")
            txn_buffer.clear()

        if doc_buffer:
            start = time.monotonic()
            try:
                result = os_client.bulk_index("financial-documents", doc_buffer)
                docs_indexed.labels(topic="financial-documents", service="opensearch-writer").inc(result["total"])
                index_latency.labels(service="opensearch-writer").observe(time.monotonic() - start)
                print(f"[INFO] Indexed {result['total']} document chunks (errors: {result['errors']})")
            except Exception as e:
                index_errors.labels(service="opensearch-writer", error_type="bulk_index_error").inc()
                print(f"[ERROR] Document bulk index failed: {e}")
            doc_buffer.clear()

        consumer.commit()
        last_flush = time.monotonic()

    while True:
        msg = consumer.poll(timeout=0.5)

        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    index_errors.labels(service="opensearch-writer", error_type="kafka_error").inc()
                continue

            topic = msg.topic()
            msgs_consumed.labels(topic=topic, service="opensearch-writer").inc()

            try:
                doc = json.loads(msg.value().decode("utf-8"))
                if topic == "processed.documents":
                    doc_buffer.append(doc)
                else:
                    txn_buffer.append(doc)
            except Exception as e:
                index_errors.labels(service="opensearch-writer", error_type="deserialize_error").inc()
                print(f"[ERROR] Deserialize error: {e}")

        # Flush on batch size OR time interval
        should_flush = (
            len(txn_buffer) >= BATCH_SIZE
            or len(doc_buffer) >= BATCH_SIZE
            or (time.monotonic() - last_flush) >= FLUSH_INTERVAL
        )
        if should_flush and (txn_buffer or doc_buffer):
            flush_buffers()

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="OpenSearch Writer", version="1.0.0")

@app.on_event("startup")
async def startup():
    t = Thread(target=consumer_loop, daemon=True)
    t.start()

@app.get("/health")
def health():
    return {"status": "ok", "service": "opensearch-writer", "opensearch_host": OS_HOST}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=SERVICE_PORT, log_level="info")
