"""
Stream Processor
================
Consumes from multiple Kafka topics, applies validation + enrichment + routing,
and produces processed events to downstream topics.

Pipeline per message:
  1. Deserialize and normalize (handle REST, CDC, and webhook formats)
  2. Validate schema and business rules
  3. PII scan (detect and tag)
  4. Risk enrichment (compute risk label from score)
  5. Route to output topic or DLQ
  6. Emit lineage event
  7. Cache in Redis (latest state per account)

Topics consumed:
  raw.transactions       → from REST API connector
  raw.cdc.transactions   → from Debezium CDC
  raw.webhooks           → from webhook receiver
  cdc.public.transactions → Debezium topic (alternate naming)

Topics produced:
  processed.transactions → validated + enriched records
  dlq.transactions       → records that failed validation
  audit.lineage          → lineage tracking events
  audit.pii              → PII detection alerts
"""

import asyncio
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from threading import Thread

import redis as redis_lib
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from fastapi import FastAPI
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
INPUT_TOPICS   = os.environ.get("KAFKA_INPUT_TOPICS", "raw.transactions,raw.cdc.transactions,raw.webhooks").split(",")
OUTPUT_TOPIC   = os.environ.get("KAFKA_OUTPUT_TOPIC", "processed.transactions")
DLQ_TOPIC      = os.environ.get("KAFKA_DLQ_TOPIC", "dlq.transactions")
LINEAGE_TOPIC  = os.environ.get("KAFKA_LINEAGE_TOPIC", "audit.lineage")
PII_TOPIC      = os.environ.get("KAFKA_PII_TOPIC", "audit.pii")
REDIS_HOST     = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.environ.get("REDIS_PORT", "6379"))
SERVICE_PORT   = int(os.environ.get("SERVICE_PORT", "8003"))

# ── Prometheus Metrics ────────────────────────────────────────────────────────

msgs_consumed = Counter(
    "pipeline_messages_consumed_total",
    "Messages consumed from Kafka",
    ["topic", "service"]
)
msgs_produced = Counter(
    "pipeline_messages_produced_total",
    "Messages produced to Kafka",
    ["topic", "service"]
)
processing_errors = Counter(
    "pipeline_processing_errors_total",
    "Processing errors",
    ["service", "error_type"]
)
dlq_messages = Counter(
    "pipeline_dlq_messages_total",
    "Messages sent to DLQ",
    ["service", "reason"]
)
processing_latency = Histogram(
    "pipeline_processing_duration_seconds",
    "Per-message processing latency",
    ["service"],
    buckets=[0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
)
pii_detections = Counter(
    "pii_detections_total",
    "PII detections in stream processor",
    ["pii_type"]
)
risk_score_hist = Histogram(
    "pipeline_transaction_risk_score",
    "Transaction risk score distribution",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
)
active_accounts = Gauge(
    "stream_processor_active_accounts",
    "Unique accounts seen in Redis cache"
)

# ── PII Patterns ──────────────────────────────────────────────────────────────

PII_PATTERNS = {
    "email":       re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    "phone":       re.compile(r"\b(\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
    "ssn":         re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d{4}[-\s]?){3}\d{4}\b"),
    "iban":        re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}\b"),
}

def scan_for_pii(text: str) -> list[str]:
    """Return list of PII types found in text."""
    found = []
    for pii_type, pattern in PII_PATTERNS.items():
        if pattern.search(text):
            found.append(pii_type)
    return found

def redact_pii(text: str, pii_types: list[str]) -> str:
    """Replace PII values with [REDACTED-TYPE] placeholders."""
    for pii_type in pii_types:
        text = PII_PATTERNS[pii_type].sub(f"[REDACTED-{pii_type.upper()}]", text)
    return text

# ── Normalizer: unify events from all 3 sources ──────────────────────────────

def normalize_event(raw: dict, source_topic: str) -> dict | None:
    """
    Translate source-specific schemas into a unified internal schema.
    Returns None if the event type isn't relevant for this processor.
    """
    event_type = raw.get("event_type", "")

    # CDC event format from Debezium: {op, before, after, source, ts_ms}
    if "op" in raw and "after" in raw:
        after = raw.get("after") or {}
        return {
            "transaction_id":   str(after.get("transaction_id") or uuid.uuid4()),
            "account_id":       str(after.get("account_id", "UNKNOWN")),
            "counterparty_id":  after.get("counterparty_id"),
            "amount":           float(after.get("amount", 0)),
            "currency":         after.get("currency", "USD"),
            "transaction_type": after.get("transaction_type", "UNKNOWN"),
            "status":           after.get("status", "UNKNOWN"),
            "risk_score":       float(after.get("risk_score") or 0),
            "source":           "cdc",
            "source_topic":     source_topic,
            "cdc_op":           raw.get("op"),
            "schema_version":   "1.0",
            "original_ts":      raw.get("ts_ms"),
        }

    # REST API transaction format
    if raw.get("source") == "rest_api_connector" and "transaction_id" in raw:
        return {
            "transaction_id":   raw["transaction_id"],
            "account_id":       raw.get("account_id", "UNKNOWN"),
            "counterparty_id":  raw.get("counterparty_id"),
            "amount":           float(raw.get("amount", 0)),
            "currency":         raw.get("currency", "USD"),
            "transaction_type": raw.get("transaction_type", "UNKNOWN"),
            "status":           raw.get("status", "PENDING"),
            "risk_score":       float(raw.get("risk_score", 0)),
            "symbol":           raw.get("symbol"),
            "source":           "rest_api",
            "source_topic":     source_topic,
            "schema_version":   "1.0",
        }

    # Market price ticks → not processed as transactions
    if raw.get("event_type") == "MARKET_PRICE":
        return None

    # Webhook risk alert format
    if event_type == "RISK_ALERT":
        return {
            "transaction_id":   raw.get("transaction_id") or str(uuid.uuid4()),
            "account_id":       raw.get("account_id", "UNKNOWN"),
            "amount":           0.0,
            "currency":         "USD",
            "transaction_type": "RISK_ALERT",
            "status":           "FLAGGED",
            "risk_score":       float(raw.get("risk_score", 0.8)),
            "alert_type":       raw.get("alert_type"),
            "alert_severity":   raw.get("severity"),
            "description":      raw.get("description"),
            "source":           "webhook",
            "source_topic":     source_topic,
            "schema_version":   "1.0",
        }

    return None

# ── Validation ────────────────────────────────────────────────────────────────

class ValidationError(Exception):
    pass

def validate(event: dict) -> None:
    """Apply business rule validation. Raises ValidationError on failure."""
    if not event.get("account_id") or event["account_id"] == "UNKNOWN":
        raise ValidationError("Missing account_id")
    if event["amount"] < 0:
        raise ValidationError(f"Negative amount: {event['amount']}")
    if event["amount"] > 10_000_000:
        raise ValidationError(f"Amount exceeds sanity limit: {event['amount']}")
    if not (0.0 <= event.get("risk_score", 0) <= 1.0):
        raise ValidationError(f"Invalid risk_score: {event['risk_score']}")

# ── Enrichment ────────────────────────────────────────────────────────────────

RISK_LABELS = [
    (0.0,  0.3, "LOW"),
    (0.3,  0.6, "MEDIUM"),
    (0.6,  0.85,"HIGH"),
    (0.85, 1.01,"CRITICAL"),
]

def enrich(event: dict, redis_client: redis_lib.Redis) -> dict:
    """Add computed fields and pull context from Redis."""
    risk_score = event.get("risk_score", 0)
    risk_label = next((label for lo, hi, label in RISK_LABELS if lo <= risk_score < hi), "UNKNOWN")

    enrichment_tags = []
    if event["amount"] > 50000:
        enrichment_tags.append("large_transaction")
    if risk_label in ("HIGH", "CRITICAL"):
        enrichment_tags.append("high_risk")
    if event.get("transaction_type") == "RISK_ALERT":
        enrichment_tags.append("alert_flagged")
    if event.get("cdc_op") == "d":
        enrichment_tags.append("deleted")

    # Pull recent transaction count from Redis for velocity check
    account_id = event["account_id"]
    acct_key   = f"acct:txn_count:{account_id}"
    try:
        txn_count = redis_client.incr(acct_key)
        redis_client.expire(acct_key, 3600)
        if txn_count > 20:
            enrichment_tags.append("velocity_breach")
    except Exception:
        txn_count = None

    event.update({
        "risk_label":             risk_label,
        "enrichment_tags":        enrichment_tags,
        "account_txn_count_1h":   txn_count,
        "processed_at":           datetime.now(timezone.utc).isoformat(),
        "processing_service":     "stream-processor",
        "pipeline_run_id":        str(uuid.uuid4()),
    })
    return event

# ── Processing pipeline ───────────────────────────────────────────────────────

def process_message(
    raw: dict,
    source_topic: str,
    producer: Producer,
    redis_client: redis_lib.Redis,
) -> str:
    """
    Full processing pipeline for one message.
    Returns: "processed" | "dlq" | "skipped"
    """
    start = time.monotonic()
    pipeline_run_id = str(uuid.uuid4())

    # Step 1: Normalize
    event = normalize_event(raw, source_topic)
    if event is None:
        return "skipped"

    # Step 2: Validate
    try:
        validate(event)
    except ValidationError as e:
        dlq_messages.labels(service="stream-processor", reason="validation_failed").inc()
        dlq_record = {
            "original_message":  raw,
            "error":             str(e),
            "source_topic":      source_topic,
            "failed_at":         datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(DLQ_TOPIC, key=pipeline_run_id.encode(), value=json.dumps(dlq_record).encode())
        return "dlq"

    # Step 3: PII scan
    full_text  = json.dumps(event)
    pii_found  = scan_for_pii(full_text)
    if pii_found:
        for pii_type in pii_found:
            pii_detections.labels(pii_type=pii_type).inc()
        # Emit PII alert
        pii_alert = {
            "source_topic":   source_topic,
            "message_key":    event.get("transaction_id"),
            "pii_types":      pii_found,
            "action_taken":   "FLAGGED",
            "detected_at":    datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(PII_TOPIC, key=pipeline_run_id.encode(), value=json.dumps(pii_alert).encode())
        event["pii_detected"] = True
        event["pii_types"]    = pii_found
    else:
        event["pii_detected"] = False

    # Step 4: Enrich
    event = enrich(event, redis_client)

    # Step 5: Emit lineage event
    lineage_event = {
        "event_id":       str(uuid.uuid4()),
        "pipeline_run_id": pipeline_run_id,
        "entity_id":       event.get("transaction_id"),
        "entity_type":     "TRANSACTION",
        "source_system":   event.get("source"),
        "source_topic":    source_topic,
        "destination":     OUTPUT_TOPIC,
        "transformation":  "validate+enrich",
        "schema_version":  "1.0",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    producer.produce(LINEAGE_TOPIC, key=pipeline_run_id.encode(), value=json.dumps(lineage_event).encode())

    # Step 6: Produce to output
    producer.produce(
        OUTPUT_TOPIC,
        key=event.get("transaction_id", pipeline_run_id).encode(),
        value=json.dumps(event).encode(),
    )

    elapsed = time.monotonic() - start
    processing_latency.labels(service="stream-processor").observe(elapsed)
    risk_score_hist.observe(event.get("risk_score", 0))
    msgs_produced.labels(topic=OUTPUT_TOPIC, service="stream-processor").inc()

    return "processed"

# ── Consumer loop (runs in a background thread) ───────────────────────────────

def consumer_loop():
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_SERVERS,
        "group.id":           "stream-processor-group",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "acks":              "all",
        "retries":           3,
        "client.id":         "stream-processor",
    })
    redis_client = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Also subscribe to Debezium's direct topic naming
    all_topics = INPUT_TOPICS + ["cdc.public.transactions", "cdc.public.market_prices"]
    consumer.subscribe(all_topics)
    print(f"[INFO] Stream Processor subscribed to: {all_topics}")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                processing_errors.labels(service="stream-processor", error_type="kafka_consume_error").inc()
                print(f"[ERROR] Consumer error: {msg.error()}")
            continue

        topic = msg.topic()
        msgs_consumed.labels(topic=topic, service="stream-processor").inc()

        try:
            raw    = json.loads(msg.value().decode("utf-8"))
            result = process_message(raw, topic, producer, redis_client)
            if result == "processed":
                producer.flush()
        except json.JSONDecodeError as e:
            processing_errors.labels(service="stream-processor", error_type="json_decode_error").inc()
            print(f"[ERROR] JSON decode error on topic {topic}: {e}")
        except Exception as e:
            processing_errors.labels(service="stream-processor", error_type="unexpected_error").inc()
            print(f"[ERROR] Unexpected error on topic {topic}: {e}")

# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(title="Stream Processor", version="1.0.0")

@app.on_event("startup")
async def startup():
    t = Thread(target=consumer_loop, daemon=True)
    t.start()

@app.get("/health")
def health():
    return {
        "status":        "ok",
        "service":       "stream-processor",
        "input_topics":  INPUT_TOPICS,
        "output_topic":  OUTPUT_TOPIC,
    }

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=SERVICE_PORT, log_level="info")
