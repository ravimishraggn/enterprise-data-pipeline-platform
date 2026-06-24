"""
MinIO Sink (Raw Event Archive)
==============================
Consumes raw events from multiple Kafka topics and archives them to MinIO
as newline-delimited JSON files (NDJSON), partitioned by date and hour.

Storage layout:
  raw-events-archive/
    raw.transactions/
      year=2024/month=01/day=15/hour=14/
        batch_20240115_143012_abc123.ndjson
    raw.webhooks/
      ...

What this demonstrates:
  - Time-partitioned data lake pattern (Hive-style partitioning)
  - NDJSON format for analytical tools (Spark, Athena, DuckDB can read this directly)
  - Micro-batch accumulation before flushing (avoid millions of tiny files)
  - Object key design for efficient partition pruning in queries
"""

import io
import json
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from threading import Thread

from confluent_kafka import Consumer, KafkaError
from minio import Minio
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi import FastAPI
from starlette.responses import Response
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
INPUT_TOPICS     = os.environ.get("KAFKA_INPUT_TOPICS", "raw.transactions,raw.webhooks,raw.documents").split(",")
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS     = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET     = os.environ.get("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET", "raw-events-archive")
FLUSH_SIZE       = int(os.environ.get("FLUSH_SIZE", "100"))          # records per file
FLUSH_INTERVAL   = float(os.environ.get("FLUSH_INTERVAL_SECONDS", "30.0"))  # seconds

# ── Prometheus ────────────────────────────────────────────────────────────────

msgs_consumed = Counter(
    "pipeline_messages_consumed_total",
    "Messages consumed",
    ["topic", "service"]
)
files_written = Counter(
    "pipeline_messages_produced_total",
    "Files written to MinIO",
    ["topic", "service"]
)
write_errors = Counter(
    "pipeline_processing_errors_total",
    "Write errors",
    ["service", "error_type"]
)
write_latency = Histogram(
    "pipeline_processing_duration_seconds",
    "MinIO write latency",
    ["service"]
)
buffer_size = Gauge(
    "minio_sink_buffer_size",
    "Current buffer size (records pending flush)",
    ["topic"]
)

# ── Object key builder ────────────────────────────────────────────────────────

def make_object_key(topic: str, batch_id: str) -> str:
    """
    Hive-style partitioned key:
    raw.transactions/year=2024/month=01/day=15/hour=14/batch_xxx.ndjson
    """
    now   = datetime.now(timezone.utc)
    topic_safe = topic.replace(".", "_").replace("-", "_")
    return (
        f"{topic_safe}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"batch_{now.strftime('%Y%m%d_%H%M%S')}_{batch_id[:8]}.ndjson"
    )

# ── MinIO writer ──────────────────────────────────────────────────────────────

def flush_to_minio(client: Minio, topic: str, records: list[dict]) -> str:
    """Serialize records as NDJSON and upload to MinIO. Returns the object key."""
    batch_id   = str(uuid.uuid4())
    object_key = make_object_key(topic, batch_id)

    ndjson     = "\n".join(json.dumps(r) for r in records) + "\n"
    raw_bytes  = ndjson.encode("utf-8")

    client.put_object(
        MINIO_BUCKET,
        object_key,
        io.BytesIO(raw_bytes),
        length=len(raw_bytes),
        content_type="application/x-ndjson",
        metadata={
            "source-topic":   topic,
            "record-count":   str(len(records)),
            "batch-id":       batch_id,
            "archived-at":    datetime.now(timezone.utc).isoformat(),
        },
    )
    return object_key

# ── Consumer loop ─────────────────────────────────────────────────────────────

def consumer_loop():
    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

    # Wait for MinIO
    for _ in range(30):
        try:
            minio_client.list_buckets()
            break
        except Exception:
            time.sleep(3)

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_SERVERS,
        "group.id":           "minio-sink-group",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(INPUT_TOPICS)
    print(f"[INFO] MinIO Sink subscribed to: {INPUT_TOPICS}")

    # Per-topic buffer: {topic: [records]}
    buffers    = defaultdict(list)
    last_flush = defaultdict(float)

    def flush_topic(topic: str) -> None:
        records = buffers[topic]
        if not records:
            return
        start = time.monotonic()
        try:
            key = flush_to_minio(minio_client, topic, records)
            files_written.labels(topic=topic, service="minio-sink").inc()
            write_latency.labels(service="minio-sink").observe(time.monotonic() - start)
            print(f"[INFO] Archived {len(records)} records → s3://{MINIO_BUCKET}/{key}")
            consumer.commit()
        except Exception as e:
            write_errors.labels(service="minio-sink", error_type="minio_write_error").inc()
            print(f"[ERROR] MinIO write failed for topic {topic}: {e}")
        buffers[topic].clear()
        last_flush[topic] = time.monotonic()
        buffer_size.labels(topic=topic).set(0)

    while True:
        msg = consumer.poll(timeout=0.5)

        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    write_errors.labels(service="minio-sink", error_type="kafka_error").inc()
                continue

            topic = msg.topic()
            msgs_consumed.labels(topic=topic, service="minio-sink").inc()

            try:
                record = json.loads(msg.value().decode())
                record["_archived_at"] = datetime.now(timezone.utc).isoformat()
                record["_source_topic"] = topic
                buffers[topic].append(record)
                buffer_size.labels(topic=topic).set(len(buffers[topic]))
            except Exception as e:
                write_errors.labels(service="minio-sink", error_type="deserialize_error").inc()
                print(f"[ERROR] Deserialize: {e}")

        now = time.monotonic()
        for topic in INPUT_TOPICS:
            size_threshold = len(buffers[topic]) >= FLUSH_SIZE
            time_threshold = (now - last_flush.get(topic, 0)) >= FLUSH_INTERVAL and buffers[topic]
            if size_threshold or time_threshold:
                flush_topic(topic)

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="MinIO Sink", version="1.0.0")

@app.on_event("startup")
async def startup():
    t = Thread(target=consumer_loop, daemon=True)
    t.start()

@app.get("/health")
def health():
    return {"status": "ok", "service": "minio-sink", "bucket": MINIO_BUCKET}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8006, log_level="info")
