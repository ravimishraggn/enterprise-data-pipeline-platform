# Kafka Fundamentals — Deep Learning Notes

---

## What Is a Kafka Topic?

A **topic** is a named, persistent, append-only log. Think of it like a file that
can only be written to at the end, and many different programs can read from it at
their own position in the file — without affecting each other.

```
raw-market-data topic
─────────────────────────────────────────────────────────────────►  time
[offset 0] [offset 1] [offset 2] [offset 3] [offset 4] ... [offset N]
 HDFC 1650  TCS 3701   HDFC 1648  RELIANCE    TCS 3699    ...  newest
```

Key properties:
- **Immutable**: once written, a message cannot be changed or deleted (only expires after retention period)
- **Ordered**: within a partition, messages have a monotonically increasing offset
- **Persistent**: stored on disk; consumers reading slowly does not block producers
- **Replayable**: a new consumer can start at offset 0 and re-read the entire history

This is the biggest conceptual difference between Kafka and a traditional queue like
SQS or RabbitMQ: **reading does not consume the message**.

---

## What Is a Partition and Why Does It Exist?

A single topic is split into **N partitions**. Each partition is an independent
ordered log. Partitions exist for two reasons:

### Reason 1: Parallelism (write throughput)

A single Kafka broker can handle ~100K-500K messages/second. With 1 partition,
you're limited to what one thread can write. With 3 partitions across 3 brokers,
you have 3× the write throughput.

```
                    ┌─ Partition 0 ──────────────────────────────┐
raw-market-data ───┼─ Partition 1 ──────────────────────────────┤
                    └─ Partition 2 ──────────────────────────────┘
```

### Reason 2: Consumer parallelism (read throughput)

Each partition can be read by exactly ONE consumer within a consumer group. So a
topic with 3 partitions can be consumed in parallel by up to 3 consumers in the
same group.

### How partition routing works

```python
# key provided → deterministic partition:
partition = hash(key) % num_partitions
# "HDFC.NS" always maps to partition 1 (for a 3-partition topic)

# no key → round-robin across partitions
```

### Ordering guarantee

**Within a partition**: messages are in strict production order.
**Across partitions**: no ordering guarantee.

This is why we key by `entity_id` (the stock symbol). All HDFC.NS events land on
the same partition, so any consumer reading that partition sees them in order.

---

## Partition Count Decision Formula

There is no universal rule, but a common starting heuristic:

```
target_partitions = max(
    max_expected_throughput_msg_per_sec / throughput_per_consumer_msg_per_sec,
    max_parallel_consumers_you_need
)
```

**Example for our market data use case:**

| Factor | Value |
|--------|-------|
| Expected peak volume | 8 symbols × 10 ticks/sec = 80 msg/s |
| Consumer throughput | ~5,000 msg/s (Python consumer is IO-bound) |
| Max parallel analytics consumers | 3 |

```
target = max(80/5000, 3) = max(0.016, 3) = 3 partitions  ← dominated by parallelism need
```

**Practical rules:**
- Start with 3-6 partitions for development
- More partitions = more parallel consumers, but also more overhead (leader election, metadata)
- You can increase partitions later, but it changes key → partition mapping (breaks ordering by key temporarily)
- Never decrease partitions (not supported)
- Rule of thumb for production: 10-30 partitions for high-throughput financial topics

---

## What Are Consumer Groups and Why Do They Matter?

A **consumer group** is a set of consumers that cooperate to consume a topic.
The group_id string is the name of the group. Kafka tracks one offset per partition
per group.

```
Kafka Broker
│
│  raw-market-data: Partition 0, Partition 1, Partition 2
│
│  Consumer Group: analytics-group
│    └─ offsets: {P0: 412, P1: 407, P2: 415}
│
│  Consumer Group: storage-group
│    └─ offsets: {P0: 350, P1: 348, P2: 352}
│
│  Consumer Group: ml-training-group (hypothetical)
│    └─ offsets: {P0: 0, P1: 0, P2: 0}  ← just started, re-reading from the beginning
```

The same Kafka topic can serve **unlimited independent consumer groups** with zero
interference. This is what makes Kafka the right choice when you need to pipe data
to multiple downstream systems simultaneously.

### Consumer group rebalancing

When a consumer joins or leaves a group, Kafka **rebalances** partition assignment:

```
Before (2 consumers, 3 partitions):
  Consumer-1: P0, P1
  Consumer-2: P2

After adding Consumer-3 (rebalance triggered):
  Consumer-1: P0
  Consumer-2: P1
  Consumer-3: P2

After Consumer-2 crashes (rebalance triggered):
  Consumer-1: P0, P1
  Consumer-3: P2
```

During rebalancing, consumption pauses briefly. This is why you see a short lag
spike in monitoring when deploying new consumer versions.

---

## Kafka vs SQS/SNS — Full Comparison

| Feature | Apache Kafka | Amazon SQS/SNS |
|---------|-------------|----------------|
| **Message persistence** | Configurable retention (hours to years) | SQS: 4-14 days. SNS: no storage |
| **Replayability** | Yes — seek to any offset, re-read history | No — once read, message is deleted |
| **Multiple consumers** | Unlimited independent consumer groups | Fan-out requires SNS → multiple SQS queues (complex) |
| **Ordering** | Per-partition strict ordering | SQS FIFO: per-group, not per-key |
| **Throughput** | Millions msg/s per broker | SQS standard: ~120K/s. FIFO: 3K/s |
| **Latency** | Sub-10ms p99 in-datacenter | ~1-30ms (higher variance) |
| **Schema enforcement** | Via Schema Registry (Avro/Protobuf) | No (JSON blobs) |
| **Consumer lag visibility** | Built-in (offset arithmetic) | Via `ApproximateNumberOfMessages` (approximate) |
| **Back-pressure** | Natural (slow consumers just lag) | Visibility timeout, DLQ needed |
| **Operational burden** | High (you manage brokers, disks, ZK) | Zero (fully managed) |
| **Cost model** | Fixed cluster cost | Pay per message |
| **CDC integration** | Native (Debezium) | Via Lambda bridge |
| **Vendor lock-in** | None (open source, runs anywhere) | AWS only |

---

## When NOT to Use Kafka

Kafka is powerful but has real costs. These are cases where SQS or a simpler
solution is clearly the right choice:

### 1. Simple task queues

If you just need "send email on signup" or "resize image on upload" — SQS is
dramatically simpler. You don't need replay, you don't need multiple consumers,
and you don't need ordering.

```
Verdict: SQS ✓ — one message in, one worker out, done.
```

### 2. Low-volume, low-frequency events

< 1,000 messages/day. The operational overhead of running Kafka (broker, ZK,
monitoring, topic management) vastly outweighs the benefit.

```
Verdict: SQS or even PostgreSQL LISTEN/NOTIFY ✓
```

### 3. Request-response patterns

Kafka is fire-and-forget (asynchronous). If your producer needs an immediate
response from a consumer, Kafka is the wrong tool.

```
Verdict: REST API or gRPC ✓
```

### 4. Small teams with no Kafka expertise

Kafka's operational burden is real. Partition tuning, replication factor, ISR
management, consumer lag alerting — all need attention. If your team is 2 people,
Confluent Cloud or AWS MSK (managed Kafka) reduces this significantly. But raw
Kafka on-premises requires dedicated expertise.

```
Verdict: Confluent Cloud / AWS MSK if you need Kafka semantics,
         SQS if you don't ✓
```

### 5. Messages that must be deleted on read

GDPR right-to-be-forgotten is harder with Kafka. If you store PII in Kafka
messages, you either encrypt-per-user (complex) or compact-and-delete (slow).
With SQS, deletion is immediate.

```
Verdict: SQS or application-level encryption + compaction strategy ✓
```

---

## The Rule of Thumb

Use Kafka when you need **any** of:
- Replay (re-process historical data)
- Multiple independent consumers of the same stream
- Sub-second latency at high throughput
- Stream joins, windowing, stateful processing
- CDC integration

Use SQS when you need:
- Simple, managed, task-queue delivery with minimal ops
- AWS-native integration (Lambda triggers, S3 events, etc.)
- Short-lived messages with no replay requirement
