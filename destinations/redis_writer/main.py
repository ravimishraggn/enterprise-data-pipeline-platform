"""
Redis Writer
============
Consumes from `processed.transactions` and caches the most recent transaction
state per account in Redis. Implements a sliding window of last-N transactions
for real-time lookups.

Data structures:
  acct:latest:{account_id}          → HASH  — latest transaction details
  acct:history:{account_id}         → LIST  — last 10 transaction IDs (LPUSH + LTRIM)
  acct:risk:{account_id}            → STRING — latest risk score (float)
  txn:{transaction_id}              → STRING — full transaction JSON, TTL=1h
  leaderboard:risk                  → ZSET  — accounts sorted by risk score

What this demonstrates:
  - Multiple Redis data structure patterns (Hash, List, String, ZSet)
  - TTL-based cache eviction
  - Real-time leaderboard / ranking pattern
  - Pub/Sub notification for high-risk transactions
"""

import json
import os
import time
from datetime import datetime, timezone
from threading import Thread

import redis as redis_lib
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import FastAPI
from starlette.responses import Response
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
INPUT_TOPIC   = os.environ.get("KAFKA_INPUT_TOPIC", "processed.transactions")
REDIS_HOST    = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT    = int(os.environ.get("REDIS_PORT", "6379"))
CACHE_TTL     = int(os.environ.get("CACHE_TTL_SECONDS", "3600"))
HISTORY_LIMIT = int(os.environ.get("ACCOUNT_HISTORY_LIMIT", "10"))
SERVICE_PORT  = int(os.environ.get("SERVICE_PORT", "8007"))

# ── Prometheus ────────────────────────────────────────────────────────────────

msgs_consumed = Counter(
    "pipeline_messages_consumed_total",
    "Messages consumed",
    ["topic", "service"]
)
cache_writes = Counter(
    "pipeline_messages_produced_total",
    "Redis cache writes",
    ["topic", "service"]
)
write_errors = Counter(
    "pipeline_processing_errors_total",
    "Write errors",
    ["service", "error_type"]
)
write_latency = Histogram(
    "pipeline_processing_duration_seconds",
    "Redis write latency per message",
    ["service"],
    buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]
)

# ── Redis writer ──────────────────────────────────────────────────────────────

def write_to_redis(client: redis_lib.Redis, record: dict) -> None:
    """Write one processed transaction to Redis using a pipeline (atomic batch)."""
    txn_id     = record.get("transaction_id", "unknown")
    account_id = record.get("account_id", "unknown")
    risk_score = float(record.get("risk_score", 0))
    risk_label = record.get("risk_label", "UNKNOWN")

    with client.pipeline() as pipe:
        # 1. Full transaction cache (TTL)
        pipe.setex(f"txn:{txn_id}", CACHE_TTL, json.dumps(record))

        # 2. Account latest state (Hash — field-level access)
        pipe.hset(f"acct:latest:{account_id}", mapping={
            "transaction_id":   txn_id,
            "amount":           str(record.get("amount", 0)),
            "currency":         record.get("currency", "USD"),
            "transaction_type": record.get("transaction_type", ""),
            "risk_score":       str(risk_score),
            "risk_label":       risk_label,
            "updated_at":       datetime.now(timezone.utc).isoformat(),
        })
        pipe.expire(f"acct:latest:{account_id}", CACHE_TTL * 24)

        # 3. Account transaction history (List — last N IDs)
        pipe.lpush(f"acct:history:{account_id}", txn_id)
        pipe.ltrim(f"acct:history:{account_id}", 0, HISTORY_LIMIT - 1)
        pipe.expire(f"acct:history:{account_id}", CACHE_TTL * 24)

        # 4. Latest risk score (simple string for fast reads)
        pipe.setex(f"acct:risk:{account_id}", CACHE_TTL * 24, str(risk_score))

        # 5. Risk leaderboard (Sorted Set — enables "top N risky accounts" query)
        pipe.zadd("leaderboard:risk", {account_id: risk_score})
        pipe.zremrangebyrank("leaderboard:risk", 0, -1001)  # keep top 1000

        pipe.execute()

    # Publish high-risk alerts to Redis pub/sub channel
    if risk_label in ("HIGH", "CRITICAL"):
        alert = {
            "transaction_id": txn_id,
            "account_id":     account_id,
            "risk_score":     risk_score,
            "risk_label":     risk_label,
            "amount":         record.get("amount"),
            "alerted_at":     datetime.now(timezone.utc).isoformat(),
        }
        client.publish("alerts:high-risk", json.dumps(alert))

# ── Consumer loop ─────────────────────────────────────────────────────────────

def consumer_loop():
    # Wait for Redis
    redis_client = None
    for _ in range(20):
        try:
            redis_client = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            redis_client.ping()
            break
        except Exception as e:
            print(f"[WARN] Redis not ready: {e}. Retrying...")
            time.sleep(3)

    if not redis_client:
        print("[ERROR] Could not connect to Redis.")
        return

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_SERVERS,
        "group.id":           "redis-writer-group",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([INPUT_TOPIC])
    print(f"[INFO] Redis Writer subscribed to: {INPUT_TOPIC}")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                write_errors.labels(service="redis-writer", error_type="kafka_error").inc()
            continue

        msgs_consumed.labels(topic=INPUT_TOPIC, service="redis-writer").inc()
        start = time.monotonic()

        try:
            record = json.loads(msg.value().decode())
            write_to_redis(redis_client, record)
            cache_writes.labels(topic=INPUT_TOPIC, service="redis-writer").inc()
            write_latency.labels(service="redis-writer").observe(time.monotonic() - start)
        except Exception as e:
            write_errors.labels(service="redis-writer", error_type="write_error").inc()
            print(f"[ERROR] Redis write error: {e}")

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="Redis Writer", version="1.0.0")
_redis: redis_lib.Redis = None

@app.on_event("startup")
async def startup():
    global _redis
    _redis = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    t = Thread(target=consumer_loop, daemon=True)
    t.start()

@app.get("/health")
def health():
    return {"status": "ok", "service": "redis-writer"}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/account/{account_id}")
def get_account(account_id: str):
    """Live lookup: get cached account state from Redis."""
    latest  = _redis.hgetall(f"acct:latest:{account_id}")
    history = _redis.lrange(f"acct:history:{account_id}", 0, -1)
    risk    = _redis.get(f"acct:risk:{account_id}")
    return {
        "account_id": account_id,
        "latest":     latest or None,
        "history":    history,
        "risk_score": risk,
    }

@app.get("/leaderboard/risk")
def risk_leaderboard(top: int = 10):
    """Top N accounts by risk score (descending)."""
    entries = _redis.zrevrange("leaderboard:risk", 0, top - 1, withscores=True)
    return [{"account_id": acct, "risk_score": score} for acct, score in entries]

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=SERVICE_PORT, log_level="info")
