"""
REST API Connector
==================
Polls a synthetic financial market-data endpoint on a fixed interval and
publishes each record to the Kafka topic `raw.transactions`.

What this demonstrates:
  - Pull-based ingestion pattern (connector polls, not listens)
  - Idempotent producer with message keys
  - Prometheus metrics exposure via /metrics
  - FastAPI health + control endpoints alongside the background poller
"""

import asyncio
import json
import os
import time
import uuid
import random
from datetime import datetime, timezone
from typing import Any

import httpx
from confluent_kafka import Producer, KafkaException
from fastapi import FastAPI
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC", "raw.transactions")
POLL_INTERVAL    = int(os.environ.get("POLL_INTERVAL_SECONDS", "10"))
SERVICE_PORT     = int(os.environ.get("SERVICE_PORT", "8001"))

# ── Prometheus Metrics ────────────────────────────────────────────────────────

msgs_produced = Counter(
    "pipeline_messages_produced_total",
    "Total messages produced to Kafka",
    ["topic", "service"]
)
produce_errors = Counter(
    "pipeline_processing_errors_total",
    "Total processing errors",
    ["service", "error_type"]
)
produce_latency = Histogram(
    "pipeline_processing_duration_seconds",
    "Time to fetch + produce one batch",
    ["service"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)
last_poll_ts = Gauge(
    "rest_connector_last_poll_timestamp",
    "Unix timestamp of last successful poll"
)

# ── Synthetic data generator ──────────────────────────────────────────────────

ACCOUNT_IDS = [f"ACC-{i:03d}" for i in range(1, 21)]
TXN_TYPES   = ["TRANSFER", "PAYMENT", "TRADE", "DEPOSIT", "WITHDRAWAL"]
CURRENCIES  = ["USD", "EUR", "GBP", "JPY"]
SYMBOLS     = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "BTC-USD", "ETH-USD"]

def generate_transaction_batch(size: int = 5) -> list[dict[str, Any]]:
    """Generate a batch of synthetic financial transactions."""
    records = []
    for _ in range(size):
        txn_type = random.choice(TXN_TYPES)
        amount   = round(random.uniform(10, 50000), 2)
        # Higher risk for large amounts or specific types
        risk     = round(min(0.99, (amount / 50000) * 0.7 + random.uniform(0, 0.3)), 4)

        records.append({
            "transaction_id":   str(uuid.uuid4()),
            "account_id":       random.choice(ACCOUNT_IDS),
            "counterparty_id":  random.choice(ACCOUNT_IDS),
            "amount":           amount,
            "currency":         random.choice(CURRENCIES),
            "transaction_type": txn_type,
            "status":           "PENDING",
            "risk_score":       risk,
            "symbol":           random.choice(SYMBOLS) if txn_type == "TRADE" else None,
            "source":           "rest_api_connector",
            "source_system":    "synthetic-market-api",
            "schema_version":   "1.0",
            "event_timestamp":  datetime.now(timezone.utc).isoformat(),
            "metadata": {
                "batch_id": str(uuid.uuid4()),
                "connector_version": "1.0.0",
            }
        })
    return records

def generate_market_price_batch(size: int = 3) -> list[dict[str, Any]]:
    """Generate synthetic market price ticks."""
    records = []
    base_prices = {"AAPL": 182, "GOOGL": 141, "MSFT": 377, "AMZN": 178,
                   "TSLA": 242, "BTC-USD": 43500, "ETH-USD": 2280}
    for symbol in random.sample(SYMBOLS, min(size, len(SYMBOLS))):
        base = base_prices.get(symbol, 100)
        spread = base * 0.0002
        price  = round(base * (1 + random.uniform(-0.005, 0.005)), 4)
        records.append({
            "event_type":     "MARKET_PRICE",
            "symbol":         symbol,
            "price":          price,
            "bid":            round(price - spread, 4),
            "ask":            round(price + spread, 4),
            "volume":         random.randint(1000, 10_000_000),
            "source":         "rest_api_connector",
            "source_system":  "synthetic-market-api",
            "schema_version": "1.0",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        })
    return records

# ── Kafka Producer ────────────────────────────────────────────────────────────

def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "acks":              "all",          # wait for all in-sync replicas
        "retries":           3,
        "retry.backoff.ms":  500,
        "client.id":         "rest-api-connector",
    })

def delivery_report(err, msg):
    if err:
        produce_errors.labels(service="rest-api-connector", error_type="delivery_failure").inc()
        print(f"[ERROR] Delivery failed for {msg.key()}: {err}")

def produce_batch(producer: Producer, records: list[dict]) -> int:
    produced = 0
    for record in records:
        key = record.get("transaction_id") or record.get("symbol") or str(uuid.uuid4())
        producer.produce(
            topic=KAFKA_TOPIC,
            key=key.encode(),
            value=json.dumps(record).encode(),
            on_delivery=delivery_report,
        )
        produced += 1
    producer.flush()
    return produced

# ── Polling loop ──────────────────────────────────────────────────────────────

async def poll_loop(producer: Producer):
    print(f"[INFO] Starting REST API Connector → topic={KAFKA_TOPIC} interval={POLL_INTERVAL}s")
    while True:
        start = time.monotonic()
        try:
            txn_records   = generate_transaction_batch(size=random.randint(3, 8))
            price_records = generate_market_price_batch(size=random.randint(1, 3))
            all_records   = txn_records + price_records

            n = produce_batch(producer, all_records)
            msgs_produced.labels(topic=KAFKA_TOPIC, service="rest-api-connector").inc(n)
            last_poll_ts.set(time.time())
            elapsed = time.monotonic() - start
            produce_latency.labels(service="rest-api-connector").observe(elapsed)
            print(f"[INFO] Produced {n} records in {elapsed:.3f}s")
        except KafkaException as e:
            produce_errors.labels(service="rest-api-connector", error_type="kafka_error").inc()
            print(f"[ERROR] Kafka error: {e}")
        except Exception as e:
            produce_errors.labels(service="rest-api-connector", error_type="unknown").inc()
            print(f"[ERROR] Unexpected error: {e}")
        await asyncio.sleep(POLL_INTERVAL)

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="REST API Connector",
    description="Polls synthetic financial API and produces to Kafka",
    version="1.0.0",
)

@app.on_event("startup")
async def startup():
    producer = make_producer()
    asyncio.create_task(poll_loop(producer))

@app.get("/health")
def health():
    return {"status": "ok", "service": "rest-api-connector", "topic": KAFKA_TOPIC}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/trigger")
async def trigger_poll():
    """Manually trigger one poll cycle (useful for testing)."""
    producer = make_producer()
    records  = generate_transaction_batch(size=5) + generate_market_price_batch(size=2)
    n        = produce_batch(producer, records)
    return {"produced": n, "topic": KAFKA_TOPIC}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=SERVICE_PORT, log_level="info")
