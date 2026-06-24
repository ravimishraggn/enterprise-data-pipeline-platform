"""
Consumer Group A — Analytics Consumer
======================================
Group ID: analytics-group

Reads every event from `raw-market-data` and prints it to the terminal
with the Kafka metadata (topic, partition, offset, consumer group lag).

This consumer ONLY reads and prints. It does not write to any database,
does not transform data. It demonstrates the pure "read and react" pattern.

Run:
  pip install kafka-python
  python consumers/consumer_group_a.py

Open a second terminal and run consumer_group_b.py simultaneously.
Both groups receive every message independently — that is the key point.
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

# ── Configuration ──────────────────────────────────────────────────────────────

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:29092")
TOPIC         = os.environ.get("TOPIC", "raw-market-data")
GROUP_ID      = "analytics-group"

# ── Consumer setup ─────────────────────────────────────────────────────────────

def make_consumer() -> KafkaConsumer:
    """
    KafkaConsumer configuration explained:

    group_id
        All consumers sharing the same group_id form a consumer group.
        Kafka distributes topic partitions among group members. If this group
        has 1 consumer and the topic has 3 partitions, this consumer handles
        ALL 3. If you run 3 instances, each handles 1.

    auto_offset_reset='earliest'
        When this group_id has no stored offset (first run, or after reset),
        start reading from the EARLIEST available message. This means you'll
        see any messages produced before you started.
        Alternative: 'latest' — only see messages produced AFTER you connect.

    enable_auto_commit=True
        After every poll(), the consumer automatically commits the offset
        of the last processed message. This tells Kafka "I have processed
        up to this point; if I restart, start from here."
        Trade-off: auto-commit is at-least-once delivery (not exactly-once).
        If the consumer crashes between processing and commit, it re-reads
        those messages on restart.

    auto_commit_interval_ms=1000
        Commit offsets every 1 second (not after every single message).
        This batches the commits for efficiency.
    """
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        consumer_timeout_ms=-1,     # block forever (no timeout)
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )


# ── Formatter ──────────────────────────────────────────────────────────────────

COLORS = {
    "HDFC.NS":      "\033[94m",   # blue
    "RELIANCE.NS":  "\033[92m",   # green
    "TCS.NS":       "\033[93m",   # yellow
    "INFY.NS":      "\033[95m",   # magenta
    "ICICIBANK.NS": "\033[96m",   # cyan
    "WIPRO.NS":     "\033[91m",   # red
    "HCLTECH.NS":   "\033[97m",   # white
    "BAJFINANCE.NS":"\033[33m",   # orange
}
RESET = "\033[0m"


def print_event(msg) -> None:
    """Pretty-print one Kafka message with metadata."""
    event      = msg.value
    symbol     = event.get("entity_id", "UNKNOWN")
    price      = event.get("price", 0)
    drift      = event.get("drift_pct", 0)
    ts         = event.get("timestamp", "")
    received   = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    color      = COLORS.get(symbol, "")

    arrow      = "↑" if drift >= 0 else "↓"
    drift_str  = f"{arrow} {abs(drift):.3f}%"

    print(
        f"{color}"
        f"[{received}] "
        f"{symbol:<18} "
        f"₹{price:>10,.2f}  "
        f"{drift_str:<12}"
        f"  │ partition={msg.partition}"
        f"  offset={msg.offset}"
        f"  key={msg.key}"
        f"{RESET}"
    )
    # Show what auto-commit is doing (every ~1s in background)
    # The commit happens automatically via enable_auto_commit=True


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    print(f"{'='*70}")
    print(f"  Consumer Group A  (group_id: {GROUP_ID})")
    print(f"  Topic: {TOPIC}  |  Broker: {KAFKA_BROKERS}")
    print(f"{'='*70}")
    print("  This group receives ALL messages independently of Group B.")
    print("  Offsets are committed automatically every 1 second.")
    print("  Press Ctrl-C to stop.\n")

    consumer = None
    for attempt in range(1, 6):
        try:
            consumer = make_consumer()
            print(f"  Connected. Waiting for messages ...\n")
            break
        except Exception as e:
            print(f"  Attempt {attempt}/5 failed: {e}")
            time.sleep(5)

    if consumer is None:
        print("Could not connect to Kafka. Is 'make start' running?")
        sys.exit(1)

    running = True
    def _shutdown(sig, frame):
        nonlocal running
        running = False
        print("\n\nShutting down consumer group A ...")
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    count = 0
    try:
        for msg in consumer:
            if not running:
                break
            count += 1
            print_event(msg)
            # Auto-commit happens in the background; no explicit commit needed.
            # To see exactly when commits happen, you can set enable_auto_commit=False
            # and call consumer.commit() manually after processing each batch.
    except Exception as e:
        print(f"\nConsumer error: {e}", file=sys.stderr)
    finally:
        consumer.close()
        print(f"\nConsumed {count} messages total. Offsets committed. Goodbye.")


if __name__ == "__main__":
    run()
