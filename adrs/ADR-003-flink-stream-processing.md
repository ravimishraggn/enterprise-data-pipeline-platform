# ADR-003: Apache Flink for Production Stream Processing

**Status:** Accepted (Flink for production; Python consumer for local POC)  
**Date:** 2024-01-15  
**Domain:** Stream Processing

---

## Context

The stream processor needs to consume from Kafka, apply transformations (validate,
enrich, route), and produce to output topics. Options:

1. **Python Kafka consumer** ← what we use locally
2. **Apache Flink** ← production recommendation
3. **Apache Spark Structured Streaming**
4. **AWS Kinesis Data Analytics (Flink-managed)**

---

## Decision

**Python consumer for local POC** (this project).  
**Apache Flink for production** (this ADR documents why and how).

---

## What Flink Adds Over a Python Consumer

### 1. Exactly-Once Semantics

Our Python consumer uses `enable.auto.commit: True`. This means:
- Message is committed (marked as processed) even if the downstream produce fails
- A crash between processing and committing can cause duplicates (at-least-once)

Flink achieves **exactly-once** via **distributed snapshots (Chandy-Lamport algorithm)**:
```
Every N seconds, Flink inserts a "checkpoint barrier" into the Kafka stream.
When all operators have processed the barrier:
  → Take a snapshot of all operator state
  → Commit Kafka offsets
If a failure occurs:
  → Restore from last snapshot
  → Replay from that Kafka offset
  → Result: each record processed exactly once
```

### 2. Stateful Windowed Computations

Our Python processor does a simple Redis INCR for velocity checks. Flink provides
first-class windowing:

```java
// Tumbling 1-hour window: count transactions per account
stream
  .keyBy(Transaction::getAccountId)
  .window(TumblingEventTimeWindows.of(Time.hours(1)))
  .aggregate(new CountAggregate())
  .addSink(riskSink);
```

Supported window types:
- **Tumbling**: Fixed, non-overlapping windows (every 1h, count from scratch)
- **Sliding**: Overlapping windows (last 1h, updated every 5 min)
- **Session**: Gap-based (group events until 30min of silence)

### 3. Event Time vs Processing Time

Our Python processor uses wall-clock time (when Kafka delivers the message). But:
```
Market closes at 16:00
Network blip delays some messages
Message from 15:59:59 arrives at 16:00:05

With processing time:  message is bucketed in the 16:00-16:05 window (WRONG)
With event time:       message is bucketed in the 15:00-16:00 window (CORRECT)
```

Flink handles **late arrivals** with configurable watermarks:
```java
.assignTimestampsAndWatermarks(
    WatermarkStrategy
        .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(30))
        .withTimestampAssigner(t -> t.getEventTimestamp())
)
```

### 4. Horizontal Scaling

Our Python processor: one thread, one consumer, sequential processing.
Flink: runs as a distributed cluster, parallelism per operator:

```
                   ┌─ Validate (parallelism=4) ─┐
Kafka Source ──── ├─ Validate (parallelism=4) ─┤ ──── OpenSearch Sink
(partitions=12)   ├─ Validate (parallelism=4) ─┤
                   └─ Validate (parallelism=4) ─┘
```

### 5. Backpressure Handling

Flink automatically applies backpressure: if the sink is slow, upstream stages slow
down gracefully without data loss or OOM. Python's `producer.flush()` provides no
such coordination.

---

## When to NOT Use Flink

- **Low throughput** (< 10K msg/s): Python consumer is simpler and sufficient
- **Simple transformations**: If you're just filtering + forwarding, Flink's overhead isn't worth it
- **Prototyping**: Flink requires a JVM cluster; Python runs anywhere

---

## Production Flink Setup (Reference Architecture)

```
AWS:  Kinesis Data Analytics for Apache Flink (managed, auto-scaling)
GCP:  Dataflow (Beam API, Flink runner)
Azure: Azure Stream Analytics or AKS-hosted Flink
On-prem: Flink on Kubernetes (operator available)
```

**Local testing** (if you want to try Flink with this project):
```yaml
# Add to docker-compose.yml:
flink-jobmanager:
  image: flink:1.18-java17
  command: jobmanager
  ports: ["8081:8081"]
  environment:
    FLINK_PROPERTIES: "jobmanager.rpc.address: flink-jobmanager"

flink-taskmanager:
  image: flink:1.18-java17
  command: taskmanager
  depends_on: [flink-jobmanager]
  environment:
    FLINK_PROPERTIES: |
      jobmanager.rpc.address: flink-jobmanager
      taskmanager.numberOfTaskSlots: 4
```

---

## In This Project

The `processors/stream_processor/` is a Python implementation that demonstrates
the same logic (validate → enrich → route) without the operational overhead of a
Flink cluster. The code is structured so that porting to Flink would involve:

1. Replacing `Consumer.poll()` with `FlinkKafkaConsumer`
2. Replacing Redis velocity check with Flink's `ValueState`
3. Replacing our manual windowing with `TumblingEventTimeWindows`
