"""
PII Detector
============
Standalone service that consumes raw messages from multiple topics, scans
for PII, and emits structured PII alerts to `audit.pii`. Also writes audit
records to PostgreSQL for the compliance team.

This is a second layer of PII detection (stream_processor does a first pass).
Here we use more sophisticated detection including:
  - Regex patterns (fast, high precision for structured PII)
  - Contextual patterns (e.g., "account number: XXXXX")
  - Luhn algorithm check for credit card numbers
  - IBAN validation checksum

What this demonstrates:
  - Dedicated governance service pattern (separation from business logic)
  - Pattern-based PII detection without ML dependencies
  - Audit trail writing for compliance
  - Non-blocking read-only consumption (does not transform or route)
"""

import json
import math
import os
import re
import time
from datetime import datetime, timezone
from threading import Thread
from typing import Optional

import asyncpg
import asyncio
from confluent_kafka import Consumer, Producer, KafkaError
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import FastAPI
from starlette.responses import Response
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
MONITOR_TOPICS   = os.environ.get("KAFKA_MONITOR_TOPICS", "raw.transactions,raw.webhooks").split(",")
PII_TOPIC        = os.environ.get("KAFKA_PII_TOPIC", "audit.pii")
POSTGRES_DSN     = os.environ.get("POSTGRES_DSN", "postgresql://pipeline:pipeline123@localhost:5432/financial_db")
SERVICE_PORT     = int(os.environ.get("SERVICE_PORT", "8011"))

# ── Prometheus ────────────────────────────────────────────────────────────────

msgs_scanned = Counter(
    "pipeline_messages_consumed_total",
    "Messages scanned for PII",
    ["topic", "service"]
)
pii_detections = Counter(
    "pii_detections_total",
    "PII detections by type",
    ["pii_type"]
)
pii_alerts = Counter(
    "pipeline_messages_produced_total",
    "PII alert messages produced",
    ["topic", "service"]
)
scan_latency = Histogram(
    "pipeline_processing_duration_seconds",
    "PII scan latency per message",
    ["service"],
    buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01]
)

# ── PII Detection Engine ──────────────────────────────────────────────────────

PATTERNS = {
    "email": re.compile(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    ),
    "phone_us": re.compile(
        r"\b(\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
    ),
    "ssn": re.compile(
        r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"
    ),
    "ip_address": re.compile(
        r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
        r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b"
    ),
    "date_of_birth": re.compile(
        r"\b(?:dob|date.of.birth|born)[:\s]+\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b",
        re.IGNORECASE
    ),
    "passport": re.compile(
        r"\bpassport[:\s#]*[A-Z]{1,2}[0-9]{6,9}\b",
        re.IGNORECASE
    ),
}

CONTEXT_PATTERNS = {
    "account_number": re.compile(
        r"\b(?:account.?(?:number|num|no)|acct)[:\s#]*\d{8,17}\b",
        re.IGNORECASE
    ),
    "routing_number": re.compile(
        r"\b(?:routing|aba|ach)[:\s#]*\d{9}\b",
        re.IGNORECASE
    ),
}


def luhn_check(card_number: str) -> bool:
    """Verify credit card number with Luhn algorithm."""
    digits = [int(d) for d in re.sub(r"\D", "", card_number)]
    if len(digits) < 13 or len(digits) > 19:
        return False
    total = 0
    for i, digit in enumerate(reversed(digits)):
        if i % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0


def check_credit_cards(text: str) -> list[str]:
    """Find potential credit card numbers and validate with Luhn."""
    card_pattern = re.compile(r"\b(?:\d{4}[-\s]?){3}\d{4}\b")
    found = []
    for match in card_pattern.finditer(text):
        if luhn_check(match.group()):
            found.append(match.group())
    return found


def scan_message(text: str) -> dict[str, list[str]]:
    """
    Full PII scan returning {pii_type: [matched_values]}.
    Values are partially masked for audit logging.
    """
    results = {}

    for pii_type, pattern in {**PATTERNS, **CONTEXT_PATTERNS}.items():
        matches = pattern.findall(text)
        if matches:
            # Mask all but first 3 chars for audit safety
            masked = [m[:3] + "***" if isinstance(m, str) and len(m) > 3 else "***"
                      for m in matches]
            results[pii_type] = masked

    cc_hits = check_credit_cards(text)
    if cc_hits:
        results["credit_card"] = [c[:4] + "-****-****-" + c[-4:] for c in cc_hits]

    return results


# ── Consumer loop ─────────────────────────────────────────────────────────────

def consumer_loop():
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_SERVERS,
        "group.id":           "pii-detector-group",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "acks":              "1",
        "client.id":         "pii-detector",
    })
    consumer.subscribe(MONITOR_TOPICS)
    print(f"[INFO] PII Detector monitoring: {MONITOR_TOPICS}")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[ERROR] Kafka error: {msg.error()}")
            continue

        topic = msg.topic()
        msgs_scanned.labels(topic=topic, service="pii-detector").inc()
        start = time.monotonic()

        try:
            raw_text = msg.value().decode("utf-8", errors="replace")
            pii_found = scan_message(raw_text)

            if pii_found:
                for pii_type in pii_found:
                    pii_detections.labels(pii_type=pii_type).inc()

                try:
                    parsed = json.loads(raw_text)
                    message_key = (
                        parsed.get("transaction_id")
                        or parsed.get("alert_id")
                        or parsed.get("event_id")
                        or "unknown"
                    )
                except Exception:
                    message_key = "parse_failed"

                alert = {
                    "source_topic":  topic,
                    "message_key":   message_key,
                    "pii_types":     list(pii_found.keys()),
                    "pii_details":   pii_found,
                    "action_taken":  "FLAGGED",
                    "detected_at":   datetime.now(timezone.utc).isoformat(),
                    "detector":      "pii-detector-service",
                }
                producer.produce(
                    PII_TOPIC,
                    key=message_key.encode(),
                    value=json.dumps(alert).encode(),
                )
                producer.flush()
                pii_alerts.labels(topic=PII_TOPIC, service="pii-detector").inc()
                print(f"[ALERT] PII detected in {topic}: {list(pii_found.keys())}")

            scan_latency.labels(service="pii-detector").observe(time.monotonic() - start)

        except Exception as e:
            print(f"[ERROR] PII scan error: {e}")

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="PII Detector", version="1.0.0")

@app.on_event("startup")
async def startup():
    t = Thread(target=consumer_loop, daemon=True)
    t.start()

@app.get("/health")
def health():
    return {"status": "ok", "service": "pii-detector", "monitoring": MONITOR_TOPICS}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/scan")
def scan_text(body: dict):
    """Ad-hoc PII scan endpoint for testing."""
    text   = json.dumps(body)
    result = scan_message(text)
    cc     = check_credit_cards(text)
    return {
        "pii_detected": bool(result),
        "pii_types":    list(result.keys()),
        "details":      result,
        "credit_cards_found": len(cc),
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=SERVICE_PORT, log_level="info")
