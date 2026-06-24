# REST API / Market Data Producer

## What This Does

`producer.py` simulates a financial market data feed. Every 5 seconds it generates
synthetic price-update events for 8 NSE-listed stocks and publishes them to the
Kafka topic `raw-market-data`.

## Run It

```bash
# Terminal 1: start the stack
make start

# Terminal 2: run the producer
make produce
# Output:
# [14:23:01] Cycle 1 — producing 8 events:
#   → HDFC.NS           price=1,651.32 INR  drift=+0.080%
#   → RELIANCE.NS       price=2,895.40 INR  drift=-0.159%
#   ...
#   ✓ delivered → topic=raw-market-data  partition=1  offset=0
```

---

## Key Concept: What Is a Kafka Producer?

A producer is any application that **writes messages to Kafka**. The flow is:

```
Your App (producer.py)
    │
    │  KafkaProducer.send(topic, key=entity_id, value=event_dict)
    ▼
Kafka Broker
    │  routes to partition based on hash(key)
    ▼
Partition 0 │ Partition 1 │ Partition 2
(all HDFC.NS│(all TCS.NS  │(all INFY.NS
 events)    │ events)     │ events)
```

The producer never talks to consumers directly. It only writes to the broker.
Consumers independently read at their own pace.

---

## Key Concept: What Does `acks="all"` Mean and Why Does It Matter?

When you call `producer.send(...)`, the broker can acknowledge receipt in three ways:

| `acks` | Who acknowledges | Risk | Latency |
|--------|-----------------|------|---------|
| `0`    | Nobody (fire-and-forget) | Data loss if broker crashes | Fastest |
| `1`    | Only the partition leader | Loss if leader crashes before replicating | Medium |
| `"all"`| Leader + ALL in-sync replicas | No loss even if leader crashes | Slightly higher |

For financial data — where losing a trade event is unacceptable — `acks="all"` is
the correct setting. The latency difference is typically < 5ms and completely
invisible in practice.

---

## Key Concept: Why Partition by `entity_id`?

Kafka partitions are the unit of parallelism AND ordering. Within a single partition,
messages are stored in a strict, immutable sequence. Across partitions, there is no
ordering guarantee.

```
Without key (round-robin):
  Event 1 for HDFC.NS → Partition 0
  Event 2 for HDFC.NS → Partition 1   ← could be read BEFORE Event 1
  Event 3 for HDFC.NS → Partition 2   ← could be read BEFORE Event 1 or 2

With key = entity_id:
  ALL HDFC.NS events → Partition 1    ← always in production order
  ALL TCS.NS events  → Partition 0    ← always in production order
```

This matters for downstream consumers that need to compute **running price trends**
or **detect anomalies** — they require events for a symbol in order.

The mapping is stable: `partition = hash(key) % num_partitions`. As long as you
don't change the partition count, HDFC.NS always goes to the same partition.

---

## Key Concept: REST Polling vs Webhooks

This producer simulates a REST API polling pattern: wake up, call the "API" (here
we generate synthetic data), push to Kafka, sleep, repeat.

| Pattern | Who initiates | Latency | Complexity | Use when |
|---------|--------------|---------|------------|----------|
| **REST polling** | YOU call the API | Poll interval (e.g. 5s) | Low | API doesn't support push; you control the cadence |
| **Webhook (push)** | THEY call your endpoint | Near-zero | Medium | Source can send data the instant something changes |
| **CDC (Debezium)** | Database WAL reader | Sub-second | Higher | You need every DB change including deletes |

For a market data feed that publishes prices every tick, polling is fine. For a
fraud alert system where every millisecond matters, you'd use webhooks or CDC.

---

## Event Schema (v1.0)

```json
{
  "event_id":      "uuid4 — globally unique per event",
  "source_system": "market-data-vendor",
  "entity_id":     "HDFC.NS",
  "event_type":    "price_update",
  "price":         1650.50,
  "currency":      "INR",
  "timestamp":     "2024-01-15T08:30:00.123456+00:00",
  "schema_version":"v1.0"
}
```

`schema_version` is critical for evolution: when you need to add or rename fields,
bump it to `v1.1`. Consumers can check the version and apply the correct parsing
logic. This prevents breaking downstream consumers when the schema changes.
