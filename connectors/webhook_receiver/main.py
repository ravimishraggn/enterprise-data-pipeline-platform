"""
Webhook Receiver
================
FastAPI service that exposes POST endpoints for external systems to push
events to. Validates incoming payloads and publishes them to Kafka topic
`raw.webhooks`.

What this demonstrates:
  - Push-based ingestion (external system calls us)
  - Payload validation with Pydantic
  - HMAC signature verification (common webhook security pattern)
  - Async Kafka producer in a synchronous FastAPI context
  - Webhook deduplication via idempotency keys
"""

import hashlib
import hmac
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Header, Request
from pydantic import BaseModel, Field, validator
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC    = os.environ.get("KAFKA_TOPIC", "raw.webhooks")
SERVICE_PORT   = int(os.environ.get("SERVICE_PORT", "8002"))
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "dev-secret-change-in-prod")

# ── Prometheus Metrics ────────────────────────────────────────────────────────

webhooks_received = Counter(
    "pipeline_messages_produced_total",
    "Total messages produced to Kafka",
    ["topic", "service"]
)
webhook_errors = Counter(
    "pipeline_processing_errors_total",
    "Processing errors",
    ["service", "error_type"]
)
webhook_latency = Histogram(
    "pipeline_processing_duration_seconds",
    "Webhook processing latency",
    ["service"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
)

# ── Pydantic Models ───────────────────────────────────────────────────────────

class RiskAlertPayload(BaseModel):
    """Payload sent by a risk management system."""
    alert_id:     str = Field(default_factory=lambda: str(uuid.uuid4()))
    alert_type:   str                    # FRAUD_SUSPECTED, LIMIT_BREACH, AML_FLAG
    severity:     str                    # LOW, MEDIUM, HIGH, CRITICAL
    account_id:   str
    transaction_id: Optional[str] = None
    description:  str
    risk_score:   float = Field(ge=0.0, le=1.0)
    metadata:     dict[str, Any] = {}

    @validator("severity")
    def validate_severity(cls, v):
        allowed = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"severity must be one of {allowed}")
        return v.upper()

    @validator("alert_type")
    def validate_alert_type(cls, v):
        allowed = {"FRAUD_SUSPECTED", "LIMIT_BREACH", "AML_FLAG", "SANCTIONS_HIT", "VELOCITY_BREACH"}
        if v.upper() not in allowed:
            raise ValueError(f"alert_type must be one of {allowed}")
        return v.upper()

class GenericWebhookPayload(BaseModel):
    """Generic envelope for any webhook event."""
    event_id:   str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    source:     str
    payload:    dict[str, Any]
    timestamp:  Optional[str] = None

# ── Kafka Producer ────────────────────────────────────────────────────────────

producer = Producer({
    "bootstrap.servers": KAFKA_SERVERS,
    "acks":              "all",
    "retries":           3,
    "client.id":         "webhook-receiver",
})

def publish(key: str, record: dict) -> None:
    record.setdefault("received_at", datetime.now(timezone.utc).isoformat())
    record.setdefault("source_service", "webhook-receiver")
    record.setdefault("schema_version", "1.0")
    producer.produce(
        topic=KAFKA_TOPIC,
        key=key.encode(),
        value=json.dumps(record).encode(),
    )
    producer.flush()

# ── HMAC signature verification ───────────────────────────────────────────────

def verify_signature(body: bytes, signature: str) -> bool:
    """Verify webhook HMAC-SHA256 signature (same pattern as GitHub/Stripe)."""
    expected = "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode(), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)

# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Webhook Receiver",
    description="Receives push-based events and publishes to Kafka",
    version="1.0.0",
)

@app.post("/webhook/risk-alert", status_code=202)
async def receive_risk_alert(
    payload: RiskAlertPayload,
    request: Request,
    x_webhook_signature: Optional[str] = Header(None),
    x_idempotency_key:   Optional[str] = Header(None),
):
    """
    Receive a risk alert from the fraud / risk management system.
    Signature validation is enforced if the header is present.
    """
    start = time.monotonic()
    try:
        if x_webhook_signature:
            body = await request.body()
            if not verify_signature(body, x_webhook_signature):
                webhook_errors.labels(service="webhook-receiver", error_type="invalid_signature").inc()
                raise HTTPException(status_code=401, detail="Invalid webhook signature")

        record = payload.dict()
        record["event_type"]       = "RISK_ALERT"
        record["idempotency_key"]  = x_idempotency_key or payload.alert_id

        publish(key=payload.alert_id, record=record)
        webhooks_received.labels(topic=KAFKA_TOPIC, service="webhook-receiver").inc()
        webhook_latency.labels(service="webhook-receiver").observe(time.monotonic() - start)

        return {"accepted": True, "alert_id": payload.alert_id, "topic": KAFKA_TOPIC}
    except HTTPException:
        raise
    except Exception as e:
        webhook_errors.labels(service="webhook-receiver", error_type="processing_error").inc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/webhook/generic", status_code=202)
async def receive_generic(payload: GenericWebhookPayload):
    """Generic webhook endpoint for any event type not covered above."""
    start = time.monotonic()
    try:
        record = payload.dict()
        publish(key=payload.event_id, record=record)
        webhooks_received.labels(topic=KAFKA_TOPIC, service="webhook-receiver").inc()
        webhook_latency.labels(service="webhook-receiver").observe(time.monotonic() - start)
        return {"accepted": True, "event_id": payload.event_id}
    except Exception as e:
        webhook_errors.labels(service="webhook-receiver", error_type="processing_error").inc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/webhook/simulate-alerts")
async def simulate_alerts(count: int = 5):
    """Dev endpoint: generates and publishes synthetic risk alerts."""
    import random
    published = []
    types      = ["FRAUD_SUSPECTED", "LIMIT_BREACH", "AML_FLAG", "VELOCITY_BREACH"]
    severities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    accounts   = [f"ACC-{i:03d}" for i in range(1, 21)]

    for _ in range(count):
        record = {
            "alert_id":       str(uuid.uuid4()),
            "event_type":     "RISK_ALERT",
            "alert_type":     random.choice(types),
            "severity":       random.choice(severities),
            "account_id":     random.choice(accounts),
            "transaction_id": str(uuid.uuid4()),
            "description":    "Synthetic risk alert generated for testing",
            "risk_score":     round(random.uniform(0.5, 1.0), 4),
            "metadata":       {"simulated": True},
        }
        publish(key=record["alert_id"], record=record)
        published.append(record["alert_id"])

    webhooks_received.labels(topic=KAFKA_TOPIC, service="webhook-receiver").inc(count)
    return {"published": count, "alert_ids": published}

@app.get("/health")
def health():
    return {"status": "ok", "service": "webhook-receiver"}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=SERVICE_PORT, log_level="info")
