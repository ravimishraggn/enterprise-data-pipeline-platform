# ADR-001: Apache Kafka as the Unified Message Bus

**Status:** Accepted  
**Date:** 2024-01-15  
**Supersedes:** N/A  
**Authors:** Data Platform Team

---

## Context

We are building a financial data pipeline that must:

1. Ingest market price events from multiple vendors (REST APIs, CDC, webhooks)
2. Route those events to multiple downstream consumers simultaneously:
   - Real-time analytics dashboard
   - Feature store (PostgreSQL)
   - ML training data lake (MinIO/S3)
   - Fraud detection service
3. Survive individual consumer failures without losing data
4. Allow new consumers to be added without modifying producers
5. Support replaying historical data for backtesting and recovery

The candidate message buses evaluated:
- **Apache Kafka** (self-managed or Confluent Cloud)
- **Amazon SQS + SNS** (AWS-managed)
- **RabbitMQ** (self-managed)
- **Redis Streams** (lightweight, self-managed)

---

## Decision

We chose **Apache Kafka** as the central message bus.

---

## Reasons

### 1. Replayability (the decisive factor)

In financial services, the ability to replay historical data is not a nice-to-have — it is a regulatory requirement and an operational necessity.

**Scenarios that require replay:**

| Scenario | Requires |
|----------|----------|
| New analytics consumer needs to backfill last 30 days | Full replay from offset 0 |
| ML model retrained on 6-month dataset | Full replay with time filter |
| Bug in fraud detection processed wrong — reprocess affected hours | Partial replay with offset range |
| Audit: prove what data was received and when | Immutable log with timestamps |
| Disaster recovery: destination DB corrupted | Replay to rebuild state |

SQS cannot do any of these. A message consumed by SQS is deleted immediately.
Kafka retains messages for a configurable period (our default: 7 days; we archive
to S3 via MinIO Sink before expiry for longer retention).

### 2. Independent Consumer Groups (the architectural multiplier)

With SQS fan-out, each new consumer requires:
- A new SNS subscription
- A new SQS queue
- Configuration of SNS → SQS binding
- Separate dead-letter queue per consumer

With Kafka:
```python
KafkaConsumer(topic, group_id="new-consumer-group")
```
That is the entire change. The new consumer gets its own offset, starts from the
beginning of the available log, and has zero impact on existing consumers.

For a platform that will eventually have 10+ consumers (analytics, ML, risk, audit,
compliance, dashboards, feature stores...), this matters enormously.

### 3. Back-Pressure Visibility

SQS offers `ApproximateNumberOfMessages` — a count, not a trend, and approximate.

Kafka exposes **consumer lag**: the difference between the latest offset and the
consumer's committed offset, per partition. This is exact, real-time, and available
per consumer group. We can alert when `analytics-group` is 10,000 messages behind
and page the on-call engineer before the lag causes a stale dashboard.

### 4. Ordering and Partitioning by Business Key

We need all price events for a given stock symbol (entity_id) to be processed in
strict production order. Kafka guarantees this within a partition when messages are
keyed by entity_id.

SQS FIFO provides ordering only per `MessageGroupId`, with a maximum throughput of
3,000 messages/second for the entire queue — insufficient for our peak volume of
~500,000 price ticks/second at market open.

### 5. CDC Integration

Debezium (the industry-standard CDC tool) writes directly to Kafka. No bridge or
Lambda intermediary is needed. Our CDC connector for PostgreSQL is a single
configuration file.

---

## Consequences

### Positive

- Unlimited independent consumers of the same stream
- Full replay capability for backfill, recovery, and audit
- Exact consumer lag metrics per group
- Native Debezium integration for CDC
- No data loss with `acks=all` + replication factor ≥ 2

### Negative (accepted trade-offs)

- **Operational complexity**: Kafka requires Zookeeper (or KRaft), broker management,
  disk monitoring, and topic configuration. SQS requires zero ops.
  *Mitigation*: Use Confluent Cloud or AWS MSK in production; local Docker for dev.

- **Learning curve**: Consumer group rebalancing, offset management, partition
  tuning, and ISR configuration require expertise that SQS does not.
  *Mitigation*: This ADR and the LEARNING.md files in this repo.

- **Cost**: A 3-broker Kafka cluster (production minimum) costs more than SQS
  at low message volumes (<1M/day).
  *Mitigation*: At our volume (>10M/day), Kafka is significantly cheaper per message.

---

## When We Would Choose SQS Instead

If any of these were true, we would use SQS:

1. **Single consumer per event type**: no fan-out needed
2. **No replay requirement**: events are consumed and discarded
3. **AWS-only**: team is small, operational burden must be zero
4. **Volume < 1M msg/day**: cost differential favors SQS
5. **Simple task queue**: image resizing, email sending, report generation

For a simple notification service or background job queue, we would use SQS without
hesitation. The additional complexity of Kafka is only justified when you need the
full feature set.

---

## Implementation Notes

- **Local development**: Single-broker Kafka + Zookeeper via Docker Compose
- **Production**: Confluent Cloud (Kafka-as-a-service) or AWS MSK
- **Topic naming**: `raw-market-data`, `processed-transactions`, `audit-lineage`
  (dash-separated; avoid dots for compatibility with some monitoring tools)
- **Default retention**: 7 days in Kafka; indefinite in MinIO archive
- **Replication factor**: 1 for dev (single broker); 3 for production
- **Consumer group naming convention**: `{team}-{purpose}-group`
  e.g. `analytics-group`, `storage-group`, `ml-training-group`
