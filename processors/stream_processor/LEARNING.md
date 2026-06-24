# Stream Processor — Learning Notes

## What Is Stream Processing?

Stream processing means applying transformations to data **as it flows through**,
rather than batching it up and processing later. The key difference:

| Batch Processing | Stream Processing |
|-----------------|-------------------|
| Process 1M records every hour | Process each record within milliseconds of arrival |
| High throughput, high latency | Lower throughput, very low latency |
| Simpler, easier to debug | More complex, requires fault-tolerance design |
| Good for reports, historical analysis | Good for fraud detection, real-time alerts |

In financial services, stream processing is critical for fraud detection, risk
management, and real-time position tracking.

## The Processing Pipeline (per message)

```
Message arrives from Kafka
        │
        ▼
   1. Normalize    ← unify different source schemas into one internal schema
        │
        ▼
   2. Validate     ← reject bad data early; route to DLQ
        │
        ▼
   3. PII Scan     ← detect email, phone, SSN, credit card numbers
        │
        ▼
   4. Enrich       ← add risk labels, velocity check via Redis
        │
        ▼
   5. Lineage      ← emit an audit event recording this transformation
        │
        ▼
   6. Produce      ← write to processed.transactions
```

## Schema Normalization

Three different systems produce data in three different formats:
- REST API connector → `{transaction_id, amount, risk_score, ...}`
- Debezium CDC → `{op, before, after, ts_ms, source}`
- Webhook receiver → `{event_type, alert_id, alert_type, risk_score, ...}`

The `normalize_event()` function translates all three into a **unified internal
schema**. This is the fan-in pattern — many shapes in, one shape out.

## Dead Letter Queue (DLQ)

When a message fails validation, we don't drop it or crash. We route it to
`dlq.transactions` with the original payload and error reason attached.

Benefits:
- No data loss
- Ops team can inspect failures and replay after fixing the root cause
- Failure rate is measurable via `pipeline_dlq_messages_total` metric

## PII Detection

We scan the full JSON payload with regex patterns for common PII types. This is a
simplified approach — production systems use tools like:
- **Microsoft Presidio** (open source, ML-based)
- **AWS Macie** (cloud, ML-based)
- **Google DLP** (cloud, ML-based)

When PII is detected, we:
1. Tag the record (`pii_detected: true`)
2. Emit an alert to `audit.pii` for the governance team
3. Optionally redact the PII (shown in `redact_pii()` function)

## Redis for Velocity Checks

The enrichment step queries Redis to count how many transactions an account has
made in the last hour. If the count exceeds a threshold (20 in this POC), we tag
the transaction with `velocity_breach`.

Redis is ideal for this because:
- Sub-millisecond latency (same process loop, not a DB query)
- Automatic TTL expiry (`EXPIRE 3600`) — no cleanup job needed
- Atomic increment (`INCR`) — safe under concurrent processing

## Kafka Consumer Groups

Our consumer uses `group.id: "stream-processor-group"`. This means:
- If you run 3 instances of stream-processor, Kafka distributes partitions among them
- Each partition is consumed by exactly one instance at a time
- This is how you horizontally scale stream processing

For our 3-partition topics: 3 processor instances = each handles 1 partition.

## Why Not Apache Flink?

See [ADR-003-flink-stream-processing.md](../../adrs/ADR-003-flink-stream-processing.md)
for the full analysis. Short answer: Flink adds fault-tolerance, windowing, stateful
computation, and exactly-once semantics — but requires a JVM cluster to run. For this
learning project, a Python consumer is 90% of the value at 10% of the complexity.
