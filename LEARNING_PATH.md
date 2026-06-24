# Learning Path — Enterprise Data Pipeline Platform

This guide tells you what to read and explore in what order, from foundational
concepts to advanced production patterns. Each step builds on the previous one.

---

## Phase 1: Foundations (Start Here)

### Step 1: Understand the overall picture
📄 Read `README.md`
- Study the ASCII architecture diagram
- Understand what each layer does
- Look at the Kafka topic table

### Step 2: Why Kafka? (Not SQS, not RabbitMQ)
📄 Read `adrs/ADR-001-kafka-vs-sqs.md`
- The replayability argument
- When Kafka is overkill
- Key concepts: consumer groups, offsets, partitions

### Step 3: Start the stack and watch it work
```bash
docker-compose up -d
# Wait ~3 minutes, then:
open http://localhost:8080      # Kafka UI
```
- Watch topics being created
- See the REST connector producing messages every 10 seconds
- Browse the message content in Kafka UI

---

## Phase 2: Ingestion Patterns

### Step 4: Pull-based ingestion (REST API Connector)
📄 Read `connectors/rest_api_connector/LEARNING.md`
🔍 Read `connectors/rest_api_connector/main.py`

Key concepts to understand:
- `Producer` configuration: `acks="all"`, `retries`, `client.id`
- Message keys and partition routing
- Delivery callback pattern
- FastAPI + background task pattern

Try it:
```bash
curl -X POST http://localhost:8001/trigger
# Watch the message arrive in Kafka UI → raw.transactions
```

### Step 5: Push-based ingestion (Webhooks)
📄 Read `connectors/webhook_receiver/LEARNING.md`
🔍 Read `connectors/webhook_receiver/main.py`

Key concepts:
- HMAC-SHA256 signature verification
- Why `hmac.compare_digest` instead of `==`
- `HTTP 202 Accepted` semantics
- Idempotency keys for webhook deduplication
- Pydantic validation at the boundary

Try it:
```bash
curl -X POST http://localhost:8002/webhook/simulate-alerts?count=10
# Inspect raw.webhooks in Kafka UI
```

### Step 6: CDC (Change Data Capture)
📄 Read `adrs/ADR-002-debezium-cdc.md`
📄 Read `connectors/cdc_connector/LEARNING.md`
🔍 Read `connectors/cdc_connector/register_connector.py`

Key concepts:
- PostgreSQL WAL and logical replication
- Debezium connector configuration
- The `op` field: c/u/d/r
- Replication slot lag monitoring

Try it:
```bash
python connectors/cdc_connector/register_connector.py

# Insert a row and watch it appear in Kafka:
docker exec -it postgres psql -U pipeline -d financial_db -c \
  "INSERT INTO transactions (account_id, amount, currency, transaction_type) \
   VALUES ('ACC-CDC-TEST', 9999.99, 'USD', 'PAYMENT');"

# Check cdc.public.transactions in Kafka UI
```

### Step 7: Document Ingestion (MinIO / S3)
📄 Read `connectors/document_connector/LEARNING.md`
🔍 Read `connectors/document_connector/main.py`

Key concepts:
- Poll + checkpoint pattern for object stores
- The big file problem and reference vs inline content
- Hive-style partitioned object keys
- MinIO as local S3

Try it:
```bash
open http://localhost:9001         # MinIO Console (admin/password123)
# Navigate to financial-documents bucket
# Upload a .txt file
# Watch it appear in raw.documents in Kafka UI
```

---

## Phase 3: Stream Processing

### Step 8: The Stream Processor (Core of the Pipeline)
📄 Read `processors/stream_processor/LEARNING.md`
🔍 Read `processors/stream_processor/main.py`

Key concepts:
- Schema normalization (3 source formats → 1 internal format)
- Validation-first pattern + Dead Letter Queue
- PII scanning with regex
- Enrichment (risk label, velocity check via Redis)
- Lineage event emission
- Kafka consumer group mechanics

Try it:
```bash
# Watch the full flow:
curl -X POST http://localhost:8001/trigger
# Then check:
# Kafka UI → processed.transactions
# Kafka UI → audit.lineage
# http://localhost:8010/lineage/recent
```

### Step 9: Why Flink for Production?
📄 Read `adrs/ADR-003-flink-stream-processing.md`

Key concepts:
- Exactly-once vs at-least-once semantics
- Distributed snapshots (Chandy-Lamport)
- Event time vs processing time
- Windowing (tumbling, sliding, session)
- Horizontal scaling via operator parallelism

No code to run — this is conceptual. Come back to this after understanding the
Python processor, then re-read it with "how would I port this to Flink?" in mind.

### Step 10: Document Processor (Chunking + Embedding)
🔍 Read `processors/document_processor/main.py`

Key concepts:
- Text chunking with overlap (why overlap prevents context loss at boundaries)
- TF-IDF embedding as a placeholder (vs sentence-transformers in production)
- L2 normalization of vectors
- One document → many Kafka messages (fan-out pattern)

---

## Phase 4: Destinations

### Step 11: OpenSearch (Search + Vector Store)
📄 Read `adrs/ADR-004-opensearch-ilm.md`
🔍 Read `destinations/opensearch_writer/main.py`

Key concepts:
- Bulk API vs single document indexing
- kNN vector field mapping (HNSW, cosinesimil)
- Index Lifecycle Management (ILM) for time-series data
- Manual commit offset management for batched consumers

Try it:
```bash
# After some data flows through:
curl -X POST http://localhost:9200/financial-transactions/_search \
  -H "Content-Type: application/json" \
  -d '{"query": {"match_all": {}}, "size": 5}'

# Open OpenSearch Dashboards:
open http://localhost:5601
# Discover → Create index pattern → financial-transactions
```

### Step 12: PostgreSQL (Feature Store)
🔍 Read `destinations/postgres_writer/main.py`
🔍 Read `infrastructure/postgres/init.sql`

Key concepts:
- `ON CONFLICT DO NOTHING` for idempotent upserts
- asyncpg connection pool
- The `processed_transactions` table as a feature store
- Why you need both OpenSearch AND PostgreSQL (search vs transactional queries)

Try it:
```bash
docker exec -it postgres psql -U pipeline -d financial_db -c \
  "SELECT risk_label, COUNT(*), AVG(risk_score) FROM processed_transactions GROUP BY risk_label;"
```

### Step 13: MinIO Sink (Data Lake Archive)
🔍 Read `destinations/minio_sink/main.py`

Key concepts:
- Hive-style partitioning: `year=X/month=X/day=X/hour=X/`
- NDJSON format for analytical tool compatibility
- Why partition on time: partition pruning in Athena/Spark/DuckDB
- Micro-batching (avoid the small file problem)

Try it:
```bash
open http://localhost:9001  # MinIO Console
# Browse: raw-events-archive → raw_transactions → year=... → hour=...
# Download a .ndjson file and open it
```

### Step 14: Redis (Real-Time Cache)
🔍 Read `destinations/redis_writer/main.py`

Key concepts:
- Redis data structure selection: Hash vs String vs List vs ZSet
- Pipeline API for atomic multi-command operations
- TTL-based eviction (no cleanup job needed)
- Sorted Set for risk leaderboard
- Pub/Sub for real-time high-risk alerts

Try it:
```bash
# After some transactions flow through:
curl http://localhost:8007/account/ACC-001
curl http://localhost:8007/leaderboard/risk?top=5

# Connect directly to Redis:
docker exec -it redis redis-cli
> HGETALL acct:latest:ACC-001
> LRANGE acct:history:ACC-001 0 9
> ZREVRANGE leaderboard:risk 0 9 WITHSCORES
```

---

## Phase 5: Governance

### Step 15: PII Detection
📄 Read `governance/pii_detector/LEARNING.md` (see pii_detector/main.py)
🔍 Read `governance/pii_detector/main.py`

Key concepts:
- Regex-based PII detection (fast, high precision for structured PII)
- Luhn algorithm for credit card validation
- HMAC timing attacks and why `compare_digest` matters
- Two-tier detection: stream_processor (first pass) + pii_detector (dedicated service)

Try it:
```bash
# Test the ad-hoc scan endpoint:
curl -X POST http://localhost:8011/scan \
  -H "Content-Type: application/json" \
  -d '{"description": "Contact john.doe@example.com SSN: 123-45-6789"}'
```

### Step 16: Data Lineage
🔍 Read `governance/lineage_tracker/main.py`

Key concepts:
- OpenLineage-inspired event format
- Lineage graph as an adjacency list in PostgreSQL
- "Where did this data come from?" query
- Audit trail for regulatory compliance (MiFID II, FINRA)

Try it:
```bash
# Get recent lineage events:
curl http://localhost:8010/lineage/recent?limit=10

# Get stats:
curl http://localhost:8010/lineage/stats

# Lookup lineage for a specific transaction:
# (grab a transaction_id from Kafka UI first)
curl http://localhost:8010/lineage/<transaction_id>
```

### Step 17: Schema Registry (Conceptual)
📄 Read `governance/schema_registry/LEARNING.md`
📄 Read `adrs/ADR-005-schema-registry.md`

Key concepts:
- Schema compatibility modes (BACKWARD, FORWARD, FULL, NONE)
- Avro vs Protobuf vs JSON for Kafka messages
- Schema evolution workflow in CI/CD
- Why this is critical for financial data governance

---

## Phase 6: Observability

### Step 18: Metrics with Prometheus + Grafana
🔍 Read `observability/prometheus/prometheus.yml`
🔍 Read `observability/grafana/dashboards/pipeline_overview.json`

Key concepts:
- Prometheus scrape model (pull, not push)
- Counter vs Gauge vs Histogram metric types
- PromQL basics: `rate()`, `increase()`, `histogram_quantile()`
- Grafana dashboard provisioning via YAML

Try it:
```bash
open http://localhost:9090  # Prometheus
# Query: rate(pipeline_messages_produced_total[1m])
# Query: histogram_quantile(0.99, rate(pipeline_processing_duration_seconds_bucket[5m]))

open http://localhost:3000  # Grafana (admin/admin)
# Dashboard: Pipeline Overview
```

---

## Phase 7: End-to-End Testing

### Step 19: Integration Tests
🔍 Read `tests/integration/test_pipeline.py`

Key concepts:
- Testing event-driven systems (eventual consistency)
- Kafka consumer in tests (read messages to verify production)
- Test isolation with unique consumer group IDs
- Health-first testing pattern (test infrastructure before business logic)

Run the tests:
```bash
cd tests/integration
pip install -r requirements.txt
pytest test_pipeline.py -v --timeout=60
```

---

## Phase 8: Production Thinking

### Step 20: Kafka Connect vs Custom Services
📄 Read `adrs/ADR-006-kafka-connect-vs-custom.md`

By now you understand what our custom services do internally. Re-read this ADR
with that context: which parts would you replace with Kafka Connect connectors
in production, and why?

### Step 21: Production Gaps Checklist

Things this POC intentionally omits (production would need these):

| Gap | Production Solution |
|-----|---------------------|
| Single Kafka broker | 3+ brokers, rack awareness |
| No TLS | TLS everywhere + mTLS for service-to-service |
| No auth | SASL/SCRAM for Kafka, IAM for S3 |
| Local embeddings | sentence-transformers or Bedrock |
| Schema validation via Pydantic | Confluent Schema Registry + Avro |
| Checkpoint file for doc connector | Redis/DB offset store |
| Single OpenSearch node | 3+ node cluster with ILM |
| No alerting | Grafana alerts → PagerDuty / Slack |
| No secret management | Vault, AWS Secrets Manager |
| Python consumer (not Flink) | Apache Flink (see ADR-003) |
| No CI/CD | GitHub Actions with schema compat checks |

---

## Suggested Learning Order (Summary)

```
Week 1: README → ADR-001 → REST Connector → Webhook Receiver
         (Understand Kafka and the two push/pull ingestion patterns)

Week 2: ADR-002 → CDC Connector → Stream Processor → ADR-003
         (Understand CDC and stream processing core concepts)

Week 3: Document Connector → Document Processor → OpenSearch → ADR-004
         (Understand the document/vector pipeline and search)

Week 4: PostgreSQL Writer → MinIO Sink → Redis Writer
         (Understand the destination layer, each store's role)

Week 5: PII Detector → Lineage Tracker → ADR-005 (Schema Registry)
         (Understand the governance layer)

Week 6: Prometheus/Grafana → Integration Tests → ADR-006 → Production gaps
         (Observability, testing, and production readiness thinking)
```
