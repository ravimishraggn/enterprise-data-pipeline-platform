"""
Consumer Group B — Storage Consumer
=====================================
Group ID: storage-group

Reads every event from `raw-market-data` and writes each record to
PostgreSQL. Demonstrates that two consumer groups are completely
independent: Group B's offset is tracked separately from Group A's.

Stopping Group B has zero impact on Group A, and vice versa.

Run:
  pip install kafka-python psycopg2-binary
  python consumers/consumer_group_b.py

Verify data landed:
  docker exec -it postgres psql -U pipeline -d market_data -c \
    "SELECT entity_id, COUNT(*), MIN(price), MAX(price) FROM market_prices GROUP BY entity_id ORDER BY entity_id;"
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── Configuration ──────────────────────────────────────────────────────────────

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:29092")
TOPIC         = os.environ.get("TOPIC", "raw-market-data")
GROUP_ID      = "storage-group"

POSTGRES_DSN  = os.environ.get(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=market_data user=pipeline password=pipeline123"
)

# ── PostgreSQL writer ──────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO market_prices (
    event_id, source_system, entity_id, event_type,
    price, currency, schema_version,
    kafka_topic, kafka_partition, kafka_offset,
    event_timestamp
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s
)
ON CONFLICT DO NOTHING
"""


def connect_postgres(retries: int = 10) -> psycopg2.extensions.connection:
    for i in range(1, retries + 1):
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            conn.autocommit = False
            print(f"  PostgreSQL connected.")
            return conn
        except Exception as e:
            print(f"  PG attempt {i}/{retries}: {e}")
            time.sleep(5)
    print("Could not connect to PostgreSQL.")
    sys.exit(1)


def write_event(cursor, msg) -> None:
    """Insert one Kafka message into the market_prices table."""
    event = msg.value
    cursor.execute(INSERT_SQL, (
        event.get("event_id"),
        event.get("source_system"),
        event.get("entity_id"),
        event.get("event_type"),
        float(event.get("price", 0)),
        event.get("currency", "INR"),
        event.get("schema_version", "v1.0"),
        msg.topic,
        msg.partition,
        msg.offset,
        event.get("timestamp"),
    ))


# ── Consumer setup ─────────────────────────────────────────────────────────────

def make_consumer() -> KafkaConsumer:
    """
    Group B uses manual offset commit (enable_auto_commit=False).

    Why manual commit here?
        We commit the Kafka offset ONLY AFTER successfully writing to
        PostgreSQL. This gives us at-least-once delivery semantics with
        a slightly stronger guarantee: if the DB write fails, we don't
        commit the offset, so we'll retry the message on restart.

        With auto-commit (Group A's approach), the offset could be
        committed even if our downstream processing failed. The message
        would be "lost" from Kafka's perspective even though it was never
        stored in the DB.

    The trade-off: if we commit offset but then crash before the DB
    write completes, we'll skip that message on restart (we've moved
    our position forward but the data isn't in the DB). This is why
    true exactly-once requires transactional APIs on both the DB and
    Kafka, which is beyond Phase 1 scope.
    """
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,          # we commit manually after DB write
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        consumer_timeout_ms=-1,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    print(f"{'='*70}")
    print(f"  Consumer Group B  (group_id: {GROUP_ID})")
    print(f"  Topic: {TOPIC}  |  Broker: {KAFKA_BROKERS}")
    print(f"  Writing to PostgreSQL (market_prices table)")
    print(f"{'='*70}")
    print("  Offsets committed MANUALLY after each successful DB write.")
    print("  Group B's position is tracked independently from Group A.")
    print("  Press Ctrl-C to stop.\n")

    pg_conn   = connect_postgres()
    pg_cursor = pg_conn.cursor()

    consumer = None
    for attempt in range(1, 6):
        try:
            consumer = make_consumer()
            print(f"  Kafka connected. Waiting for messages ...\n")
            break
        except Exception as e:
            print(f"  Attempt {attempt}/5: {e}")
            time.sleep(5)

    if consumer is None:
        print("Could not connect to Kafka.")
        sys.exit(1)

    running = True
    def _shutdown(sig, frame):
        nonlocal running
        running = False
        print("\n\nShutting down consumer group B ...")
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    count = 0
    batch_size = 10    # commit to DB + Kafka every N messages
    batch = []

    try:
        for msg in consumer:
            if not running:
                break

            batch.append(msg)
            symbol  = msg.value.get("entity_id", "?")
            price   = msg.value.get("price", 0)
            print(
                f"  [received] {symbol:<18} ₹{price:>10,.2f}"
                f"  partition={msg.partition}  offset={msg.offset}"
                f"  (batch {len(batch)}/{batch_size})"
            )

            if len(batch) >= batch_size:
                # Write batch to PostgreSQL
                for m in batch:
                    write_event(pg_cursor, m)
                pg_conn.commit()

                # Only after DB commit succeeds, commit Kafka offsets
                consumer.commit()
                count += len(batch)
                print(
                    f"\n  ✓ Committed {len(batch)} rows to PostgreSQL"
                    f" + Kafka offset. Total stored: {count}\n"
                )
                batch.clear()

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        pg_conn.rollback()
    finally:
        # Flush remaining batch
        if batch:
            try:
                for m in batch:
                    write_event(pg_cursor, m)
                pg_conn.commit()
                consumer.commit()
                count += len(batch)
                print(f"  ✓ Flushed final {len(batch)} rows.")
            except Exception as e:
                print(f"  Final flush error: {e}")

        consumer.close()
        pg_cursor.close()
        pg_conn.close()
        print(f"\nStored {count} events total. Goodbye.")


if __name__ == "__main__":
    run()
