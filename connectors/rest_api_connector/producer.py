"""
Market Data Producer
====================
Generates synthetic Indian market price events every 5 seconds and
publishes them to the Kafka topic `raw-market-data`.

Run:
  pip install -r requirements.txt
  python producer.py

Environment variables (optional):
  KAFKA_BROKERS   – default: localhost:29092
  TOPIC           – default: raw-market-data
  INTERVAL        – default: 5 (seconds between batches)
"""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Configuration ──────────────────────────────────────────────────────────────

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:29092")
TOPIC         = os.environ.get("TOPIC", "raw-market-data")
INTERVAL      = float(os.environ.get("INTERVAL", "5"))

# ── Synthetic market universe ──────────────────────────────────────────────────
# NSE-listed symbols with realistic base prices in INR

SYMBOLS = {
    "HDFC.NS":    {"base": 1650,   "name": "HDFC Bank"},
    "RELIANCE.NS":{"base": 2900,   "name": "Reliance Industries"},
    "TCS.NS":     {"base": 3700,   "name": "Tata Consultancy Services"},
    "INFY.NS":    {"base": 1550,   "name": "Infosys"},
    "ICICIBANK.NS":{"base": 950,   "name": "ICICI Bank"},
    "WIPRO.NS":   {"base": 450,    "name": "Wipro"},
    "HCLTECH.NS": {"base": 1250,   "name": "HCL Technologies"},
    "BAJFINANCE.NS":{"base": 7100, "name": "Bajaj Finance"},
}

# ── Producer setup ─────────────────────────────────────────────────────────────

def make_producer() -> KafkaProducer:
    """
    Create a KafkaProducer with production-safe defaults.

    acks='all'
        The broker waits for ALL in-sync replicas to write the message
        before returning success. This prevents data loss if the leader
        crashes immediately after receiving the message.
        Trade-off: slightly higher latency vs. acks=1 (leader only) or
        acks=0 (fire and forget).

    retries=3
        If the broker returns a retriable error (network blip, leader
        election in progress), the producer retries up to 3 times
        automatically before raising an exception.

    value_serializer
        Kafka messages are bytes. We convert our Python dict to a
        UTF-8 encoded JSON string so Kafdrop and consumers can display
        and decode the message as human-readable text.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        acks="all",
        retries=3,
        retry_backoff_ms=200,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Increase request timeout for robustness
        request_timeout_ms=30_000,
        # Reduce linger to keep demo responsive
        linger_ms=10,
    )


def build_event(symbol: str) -> dict:
    """
    Build one synthetic price-update event for the given symbol.

    Partition key = entity_id (the symbol string).

    Why partition by entity_id?
    All price updates for HDFC.NS always land on the SAME partition.
    This guarantees that a consumer reading that partition sees all
    HDFC.NS events in the exact order they were produced. Without
    keying, events could spread across partitions and arrive out of
    order at a consumer.
    """
    meta    = SYMBOLS[symbol]
    drift   = random.uniform(-0.008, 0.008)    # ±0.8% random tick
    price   = round(meta["base"] * (1 + drift), 2)

    return {
        "event_id":      str(uuid.uuid4()),
        "source_system": "market-data-vendor",
        "entity_id":     symbol,               # ← partition key
        "event_type":    "price_update",
        "price":         price,
        "currency":      "INR",
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "schema_version":"v1.0",
        # extra fields for learning — won't break consumers that ignore them
        "company_name":  meta["name"],
        "base_price":    meta["base"],
        "drift_pct":     round(drift * 100, 4),
    }


# ── Delivery callback ──────────────────────────────────────────────────────────

def on_success(record_metadata):
    print(
        f"  ✓ delivered → topic={record_metadata.topic}"
        f"  partition={record_metadata.partition}"
        f"  offset={record_metadata.offset}"
    )


def on_error(exc):
    print(f"  ✗ delivery failed: {exc}", file=sys.stderr)


# ── Main loop ──────────────────────────────────────────────────────────────────

def run():
    print(f"Connecting to Kafka at {KAFKA_BROKERS} ...")

    producer = None
    for attempt in range(1, 6):
        try:
            producer = make_producer()
            # Force a metadata fetch to confirm connectivity
            producer.partitions_for(TOPIC)
            print(f"Connected. Producing to topic '{TOPIC}' every {INTERVAL}s.\n")
            break
        except Exception as e:
            print(f"  Attempt {attempt}/5 failed: {e}")
            time.sleep(5)

    if producer is None:
        print("Could not connect to Kafka. Is 'make start' running?")
        sys.exit(1)

    # Graceful shutdown on Ctrl-C
    running = True
    def _handle_sig(sig, frame):
        nonlocal running
        running = False
        print("\nShutting down producer ...")
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    cycle = 0
    while running:
        cycle += 1
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"[{now_str}] Cycle {cycle} — producing {len(SYMBOLS)} events:")

        for symbol in SYMBOLS:
            event = build_event(symbol)
            print(
                f"  → {symbol:<18} price={event['price']:>10,.2f} INR"
                f"  drift={event['drift_pct']:+.3f}%"
            )
            producer.send(
                TOPIC,
                key=event["entity_id"],    # partition routing key
                value=event,
            ).add_callback(on_success).add_errback(on_error)

        producer.flush()
        print()
        time.sleep(INTERVAL)

    producer.close()
    print("Producer stopped.")


if __name__ == "__main__":
    run()
