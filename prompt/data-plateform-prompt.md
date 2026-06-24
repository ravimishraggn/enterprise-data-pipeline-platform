# Claude Code Prompt — Enterprise Data Pipeline Platform
# Author: Ravi Mishra
# Purpose: Learn enterprise data architecture by building it
# Strategy: Runnable POC where possible, Architecture Decision Records where not

---

## MASTER PROMPT — PASTE THIS INTO CLAUDE CODE TO START

```
You are helping me build an Enterprise Data Pipeline Platform from scratch.
This is a learning project. I am an AI Platform Architect expanding my 
knowledge into enterprise data engineering.

GOAL: Build a production-grade data pipeline platform that demonstrates:
  - Multiple source connectors (REST API, CDC/Database, S3/Documents, Webhooks)
  - Unified Kafka backbone
  - Stream processing with validation, enrichment, routing
  - Multiple destinations (OpenSearch/VectorDB, PostgreSQL, MinIO/S3, Redis)
  - Full observability (Prometheus + Grafana)
  - Enterprise governance (lineage, PII detection, audit trail)

STRATEGY:
  - Everything that can run locally → build as runnable POC using Docker Compose
  - Everything that cannot run locally → create Architecture Decision Record (ADR)
    explaining what it is, why it exists, how it works, and what the production
    version would look like
  - Every component must have a LEARNING.md explaining the concept in depth

TECH STACK:
  - Language: Python (FastAPI for services)
  - Local infrastructure: Docker Compose
  - Message bus: Apache Kafka + Zookeeper
  - CDC: Debezium
  - Stream processing: Python consumer (Flink ADR for production)
  - VectorDB: OpenSearch (local Docker)
  - Feature store: PostgreSQL + Redis
  - Data lake: MinIO (local S3-compatible)
  - Observability: Prometheus + Grafana
  - CDC source DB: PostgreSQL

DOMAIN: Financial services data (synthetic data only, no real PII)
  - Use synthetic: transactions, market prices, financial documents, risk alerts

PROJECT STRUCTURE TO CREATE:
  enterprise-data-pipeline-platform/
  ├── docker-compose.yml
  ├── README.md                    (architecture diagram in ASCII + explanation)
  ├── LEARNING_PATH.md             (ordered reading guide for learning)
  │
  ├── infrastructure/
  │   ├── kafka/
  │   ├── opensearch/
  │   ├── postgres/
  │   └── grafana/
  │
  ├── connectors/
  │   ├── rest_api_connector/      (runnable POC)
  │   ├── cdc_connector/           (runnable POC via Debezium)
  │   ├── document_connector/      (runnable POC via MinIO)
  │   └── webhook_receiver/        (runnable POC via FastAPI)
  │
  ├── processors/
  │   ├── stream_processor/        (runnable POC)
  │   └── document_processor/      (runnable POC - chunk + embed)
  │
  ├── destinations/
  │   ├── opensearch_writer/       (runnable POC)
  │   ├── postgres_writer/         (runnable POC)
  │   ├── minio_sink/              (runnable POC)
  │   └── redis_writer/            (runnable POC)
  │
  ├── observability/
  │   ├── prometheus/
  │   └── grafana/
  │
  ├── governance/
  │   ├── pii_detector/            (runnable POC)
  │   ├── lineage_tracker/         (runnable POC)
  │   └── schema_registry/         (ADR for Confluent Schema Registry)
  │
  ├── adrs/                        (Architecture Decision Records)
  │   ├── ADR-001-kafka-vs-sqs.md
  │   ├── ADR-002-debezium-cdc.md
  │   ├── ADR-003-flink-stream-processing.md
  │   ├── ADR-004-opensearch-ilm.md
  │   ├── ADR-005-schema-registry.md
  │   └── ADR-006-kafka-connect-vs-custom.md
  │
  └── tests/
      └── integration/

START WITH PHASE 1. Build incrementally. After each phase confirm with me 
before moving to the next.
```

---

## PHASE 1 PROMPT — Kafka Foundation

```
Build Phase 1: Kafka Foundation

CREATE these files:

1. docker-compose.yml
   Include: Kafka, Zookeeper, Kafdrop (UI at port 9000)
   Use Confluent community images
   Named volumes for persistence
   Health checks on all services

2. connectors/rest_api_connector/
   - producer.py
     * Uses kafka-python library
     * Generates synthetic financial market price events every 5 seconds
     * Event schema:
       {
         "event_id": "uuid",
         "source_system": "market-data-vendor",
         "entity_id": "HDFC.NS",
         "event_type": "price_update",
         "price": 1650.50,
         "currency": "INR",
         "timestamp": "ISO8601",
         "schema_version": "v1.0"
       }
     * Partition key: entity_id
     * Topic: raw-market-data
     * Config: acks=all, retries=3
   
   - requirements.txt
   - Dockerfile
   - README.md explaining:
     * What a Kafka producer is
     * What acks=all means and why
     * What partitioning by entity_id achieves
     * When you would use REST polling vs webhooks

3. Two consumer scripts:
   consumers/consumer_group_a.py
     * group_id: "analytics-group"
     * Reads raw-market-data
     * Prints each event with timestamp
     * Shows offset being committed
     * README explains: consumer groups, offsets, at-least-once delivery

   consumers/consumer_group_b.py  
     * group_id: "storage-group"
     * Reads same raw-market-data topic
     * Writes to local PostgreSQL
     * Demonstrates independence from Group A

4. LEARNING.md in connectors/rest_api_connector/
   Explain deeply:
   - What is a Kafka topic
   - What is a partition and why it exists
   - Partition count decision formula with example
   - What consumer groups are and why they matter
   - Difference between Kafka and SQS/SNS with table
   - When NOT to use Kafka (simple cases where SQS is better)

5. adrs/ADR-001-kafka-vs-sqs.md
   Format:
   - Context: what problem we are solving
   - Decision: we chose Kafka
   - Reasons: replay, independent consumers, back pressure visibility
   - Consequences: operational complexity trade-off
   - When we would choose SQS instead

6. Makefile with commands:
   make start      → docker-compose up
   make stop       → docker-compose down
   make produce    → runs producer.py
   make consume-a  → runs consumer_group_a.py
   make consume-b  → runs consumer_group_b.py
   make ui         → opens Kafdrop in browser

After building, I should be able to:
  1. Run: make start
  2. Run: make produce (in terminal 1)
  3. Run: make consume-a (in terminal 2)
  4. Run: make consume-b (in terminal 3)
  5. Open Kafdrop and see the topic, partitions, messages
  6. See both consumers receiving every message independently

Include a VERIFY.md with exact commands to confirm it is working.
```

---

## PHASE 2 PROMPT — CDC Connector (Most Important)

```
Build Phase 2: CDC Connector using Debezium

This is the most important connector to understand.
CDC = Change Data Capture. Debezium tails the database 
transaction log and converts every INSERT/UPDATE/DELETE 
into a Kafka event automatically. Zero application code.

CREATE:

1. Update docker-compose.yml to add:
   - Debezium Connect (port 8083)
   - Configure PostgreSQL with wal_level=logical (required for CDC)
   - Debezium UI (port 8080) for visual monitoring

2. infrastructure/postgres/
   - init.sql:
     CREATE TABLE financial_transactions (
       id SERIAL PRIMARY KEY,
       entity_id VARCHAR(50),
       transaction_type VARCHAR(20),
       amount DECIMAL(15,2),
       currency VARCHAR(3),
       status VARCHAR(20),
       created_at TIMESTAMP DEFAULT NOW(),
       updated_at TIMESTAMP DEFAULT NOW()
     );
     
     CREATE TABLE market_entities (
       id SERIAL PRIMARY KEY,
       entity_id VARCHAR(50) UNIQUE,
       entity_name VARCHAR(100),
       sector VARCHAR(50),
       active BOOLEAN DEFAULT TRUE
     );

3. connectors/cdc_connector/
   - register_connector.sh
     * Calls Debezium REST API to register PostgreSQL connector
     * Config:
       {
         "connector.class": "PostgresConnector",
         "database.hostname": "postgres",
         "database.port": "5432",
         "database.user": "debezium",
         "database.password": "dbz",
         "database.dbname": "financedb",
         "table.include.list": "public.financial_transactions,public.market_entities",
         "topic.prefix": "cdc",
         "plugin.name": "pgoutput"
       }
   
   - simulate_transactions.py
     * Inserts synthetic transactions into PostgreSQL every 3 seconds
     * Also randomly updates status (pending → completed → settled)
     * Shows INSERT, UPDATE, DELETE events all captured automatically
   
   - LEARNING.md explaining:
     * What is Change Data Capture
     * What is a WAL (Write Ahead Log) — explain with analogy
     * How Debezium reads the WAL without touching application
     * What the CDC event envelope looks like (before/after fields)
     * Difference between polling database vs CDC
     * When to use CDC vs polling vs event sourcing
     * Debezium vs AWS DMS comparison

4. adrs/ADR-002-debezium-cdc.md
   - Why CDC over polling
   - WAL-based approach advantages
   - Operational considerations (slot management, lag monitoring)
   - Production: use AWS DMS or MSK Connect instead

5. VERIFY.md:
   Step 1: Start stack
   Step 2: Register Debezium connector via register_connector.sh
   Step 3: Run simulate_transactions.py
   Step 4: Open Kafdrop, find topic: cdc.public.financial_transactions
   Step 5: See INSERT events appearing automatically
   Step 6: Update a row in PostgreSQL directly
   Step 7: See UPDATE event appear in Kafka with before/after fields
   
   THE MOMENT YOU SEE AN UPDATE EVENT IN KAFKA WITHOUT WRITING
   ANY PRODUCER CODE — you understand CDC.
```

---

## PHASE 3 PROMPT — Document and Webhook Connectors

```
Build Phase 3: Document Connector + Webhook Receiver

CREATE:

1. Update docker-compose.yml to add:
   - MinIO (local S3, ports 9001/9002)
   - MinIO console for UI

2. connectors/document_connector/
   - minio_watcher.py
     * Polls MinIO bucket every 10 seconds for new files
     * Supported: PDF, DOCX, TXT, HTML
     * On new file detected:
       - Downloads file
       - Extracts text (PyMuPDF for PDF, python-docx for DOCX)
       - Creates event:
         {
           "event_id": "uuid",
           "source_system": "document-management",
           "document_id": "uuid",
           "filename": "annual_report_2024.pdf",
           "document_type": "annual_report",
           "raw_text": "extracted text here",
           "page_count": 45,
           "file_size_kb": 2048,
           "ingestion_timestamp": "ISO8601",
           "schema_version": "v1.0"
         }
       - Publishes to topic: raw-documents
     
   - upload_test_documents.py
     * Generates 5 synthetic financial documents as TXT files
     * Uploads to MinIO
     * Documents: earnings_report, risk_assessment, 
                  deal_memo, regulatory_filing, market_analysis
   
   - LEARNING.md:
     * S3 event notification patterns (polling vs event-driven)
     * Why Lambda trigger is better than polling for production
     * MinIO as local S3 equivalent
     * Document extraction strategies per file type
     * Why raw text goes to Kafka before processing
       (separation of ingestion from processing)

3. connectors/webhook_receiver/
   - main.py (FastAPI application)
     * POST /webhook/market-data
     * POST /webhook/risk-alert
     * POST /webhook/news-feed
     * Each endpoint:
       - Validates payload against schema
       - Adds governance metadata
       - Publishes to appropriate Kafka topic
       - Returns 200 with event_id for tracing
     
   - models.py (Pydantic models for each webhook type)
   
   - LEARNING.md:
     * Webhook vs polling — when each applies
     * Idempotency — why you need event_id deduplication
     * Payload signature validation for security
     * FastAPI as lightweight webhook receiver

4. adrs/ADR-006-kafka-connect-vs-custom.md
   - When to write custom connector vs use Kafka Connect
   - Kafka Connect S3 Source Connector (production alternative)
   - Trade-offs: flexibility vs operational simplicity
```

---

## PHASE 4 PROMPT — Stream Processor

```
Build Phase 4: Stream Processor

This is the brain of the pipeline. It consumes from all
raw topics, validates, enriches, detects PII, and routes
to processed topics.

CREATE:

1. processors/stream_processor/
   - main.py
     * Subscribes to ALL raw topics:
       raw-market-data, raw-transactions (from CDC),
       raw-documents, raw-webhook
     
     * For each event, pipeline:
       
       Step 1: Schema Validation
         - Validate against expected schema per topic
         - If invalid → publish to dead-letter-queue topic
         - Log validation failure with reason
       
       Step 2: PII Detection
         - Scan for: email patterns, phone patterns,
                     Aadhaar patterns, PAN patterns
         - If detected → redact before forwarding
         - Add pii_detected: true/false to metadata
       
       Step 3: Enrichment
         - Add processed_timestamp
         - Add processing_latency_ms
         - Add lineage_id (carry forward from source event_id)
         - Normalize all timestamps to UTC
         - Standardize currency codes
       
       Step 4: Routing
         - Documents  → processed-documents topic
         - Prices     → processed-market-data topic
         - Transactions → processed-transactions topic
         - Risk alerts  → processed-risk-alerts topic
     
   - schema_validator.py
     * Pydantic models for each event type
     * validate() function returns (is_valid, error_reason)
   
   - pii_detector.py
     * Regex patterns for Indian financial PII
     * redact() function masks sensitive values
     * Returns detection report
   
   - enricher.py
     * Pure functions for each enrichment step
   
   - dead_letter_handler.py
     * Publishes failed events to DLQ topic
     * Logs structured error for alerting
   
   - LEARNING.md:
     * What is stream processing vs batch processing
     * Why process in Kafka consumer vs separate framework
     * Dead letter queue pattern — why essential in production
     * PII detection in pipeline (GDPR / EU AI Act requirement)
     * At-least-once vs exactly-once processing trade-offs
     * When to use Flink/Spark Streaming instead of Python consumer

2. adrs/ADR-003-flink-stream-processing.md
   - What Apache Flink is
   - When Python consumer is enough vs when Flink is needed
   - Flink capabilities: windowing, stateful processing,
     exactly-once semantics
   - Production recommendation for high-volume financial data
   - AWS equivalent: Kinesis Data Analytics
```

---

## PHASE 5 PROMPT — Destinations

```
Build Phase 5: All Four Destinations

CREATE four independent consumer services:

1. destinations/opensearch_writer/
   - main.py
     * Consumes: processed-documents
     * For each document:
       - Chunks text (semantic chunking, 512 tokens, 15% overlap)
       - Generates embeddings (use sentence-transformers locally,
         all-MiniLM-L6-v2 model, 384 dimensions — fast and local)
       - Writes to OpenSearch index: financial-docs
       - Metadata stored:
         document_id, source_system, document_type,
         ingestion_date, entity_id, access_level
     * Implements time-based index naming:
       financial-docs-2024-Q1, financial-docs-2024-Q2 etc.
   
   - opensearch_client.py
     * Index creation with mapping
     * ILM policy setup (hot/warm simulation)
     * Hybrid search query builder (BM25 + vector)
   
   - LEARNING.md:
     * OpenSearch index anatomy
     * ILM policy — hot/warm/cold tiers explained
     * Time-based index partitioning strategy
     * Why hybrid search over pure vector
     * Shard sizing formula
     * How to test retrieval quality

2. destinations/postgres_writer/
   - main.py
     * Consumes: processed-transactions, processed-market-data
     * Writes to feature store tables:
       entity_features, price_history, transaction_aggregates
     * Upsert pattern (INSERT ON CONFLICT UPDATE)
   
   - schema.sql
     * Feature store schema design
     * Proper indexing strategy

3. destinations/minio_sink/
   - main.py
     * Consumes: ALL processed topics
     * Writes every event to MinIO as Parquet
     * Partitioned by: year/month/day/source_system
     * Simulates data lake raw zone
   
   - LEARNING.md:
     * Data lake zones: raw, curated, consumption
     * Parquet vs JSON for analytical storage
     * Partition strategy for query performance
     * Production: S3 + Glue + Athena equivalent

4. destinations/redis_writer/
   - main.py
     * Consumes: processed-market-data, processed-risk-alerts
     * Writes latest entity state to Redis
     * TTL: 24 hours
     * Key pattern: entity:{entity_id}:latest
   
   - LEARNING.md:
     * Redis as online feature store
     * TTL strategy for financial data
     * Redis vs PostgreSQL for real-time lookup
     * Production: AWS ElastiCache

5. adrs/ADR-004-opensearch-ilm.md
   - ILM policy configuration in detail
   - Hot/warm/cold tier sizing
   - Index rollover strategy
   - Production: AWS OpenSearch Service managed ILM
```

---

## PHASE 6 PROMPT — Observability

```
Build Phase 6: Full Observability Stack

CREATE:

1. Update docker-compose.yml to add:
   - Prometheus (port 9090)
   - Grafana (port 3000, admin/admin)
   - Kafka Exporter (exposes Kafka metrics to Prometheus)

2. observability/prometheus/
   - prometheus.yml
     * Scrape configs for all services
     * Kafka exporter scrape
     * Custom app metrics scrape

3. Each service must expose metrics via prometheus-client:
   Add to stream_processor, all writers, all connectors:
   
   from prometheus_client import Counter, Histogram, Gauge
   
   events_processed = Counter(
       'pipeline_events_processed_total',
       'Total events processed',
       ['topic', 'status']
   )
   
   processing_latency = Histogram(
       'pipeline_processing_latency_seconds',
       'Event processing latency',
       ['stage']
   )
   
   consumer_lag = Gauge(
       'pipeline_consumer_lag',
       'Kafka consumer lag',
       ['topic', 'consumer_group']
   )

4. observability/grafana/dashboards/
   Create dashboard JSON for:
   
   Panel 1: Pipeline Throughput
     - Events per second per topic
     - Time series graph
   
   Panel 2: Consumer Lag
     - Lag per consumer group per topic
     - Alert threshold line at 1000
   
   Panel 3: Processing Latency
     - P50, P95, P99 per stage
     - Histogram visualization
   
   Panel 4: Error Rate
     - DLQ events per minute
     - Validation failure rate
     - Source-specific error breakdown
   
   Panel 5: Destination Health
     - Write success rate per destination
     - Write latency per destination

5. governance/lineage_tracker/
   - lineage_api.py (FastAPI)
     * GET /lineage/{event_id}
     * Returns full journey of an event:
       source → kafka → processor → destinations
     * Stored in PostgreSQL lineage table
   
   - LEARNING.md:
     * Why data lineage matters (EU AI Act, debugging)
     * OpenLineage standard (production reference)
     * How to trace an event end-to-end
     * Production: Apache Atlas or AWS Glue Data Catalog

6. LEARNING.md for observability/:
   * Four golden signals: latency, traffic, errors, saturation
   * Why consumer lag is the most important Kafka metric
   * Alert design principles
   * Production: CloudWatch + MSK metrics + DataDog
```

---

## PHASE 7 PROMPT — README and Architecture Documentation

```
Build Phase 7: Final Documentation

CREATE:

1. README.md (root level — this is your GitHub showcase)
   
   Structure:
   
   # Enterprise Data Pipeline Platform
   
   ## Architecture Overview
   [Full ASCII architecture diagram showing all components]
   
   ## What This Demonstrates
   - Four source connector patterns
   - Kafka as unified transport backbone
   - Stream processing with governance
   - Four destination patterns
   - Full observability
   
   ## Quick Start
   Prerequisites: Docker, Docker Compose, Python 3.11+
   
   make start          # starts full stack
   make demo           # runs complete demo end-to-end
   make dashboard      # opens Grafana
   make topology       # opens Kafdrop
   
   ## Component Deep Dives
   [Link to each LEARNING.md]
   
   ## Architecture Decisions
   [Link to each ADR]
   
   ## Production Considerations
   [Brief section on AWS equivalents]

2. LEARNING_PATH.md
   Ordered reading guide:
   
   Week 1: Start here
     1. ADR-001-kafka-vs-sqs.md
     2. connectors/rest_api_connector/LEARNING.md
     3. Run Phase 1, observe Kafdrop
   
   Week 2: CDC
     4. ADR-002-debezium-cdc.md
     5. connectors/cdc_connector/LEARNING.md
     6. Run Phase 2, insert a row, watch Kafka
   
   Week 3: Processing
     7. processors/stream_processor/LEARNING.md
     8. ADR-003-flink-stream-processing.md
     9. Run Phase 4, trace an event end-to-end
   
   Week 4: Destinations + Observability
     10. destinations/opensearch_writer/LEARNING.md
     11. ADR-004-opensearch-ilm.md
     12. Run Phase 6, open Grafana dashboard

3. adrs/ADR-005-schema-registry.md
   Cannot run locally easily — write as ADR:
   - What is Confluent Schema Registry
   - Why schema evolution matters
   - Avro vs JSON Schema vs Protobuf
   - How it prevents breaking changes
   - Production: AWS Glue Schema Registry

4. Make a demo script: scripts/run_full_demo.sh
   - Starts all services
   - Registers Debezium connector
   - Starts all producers
   - Starts all consumers
   - Opens Kafdrop and Grafana
   - Prints: "Demo running. Check Grafana at localhost:3000"
```

---

## INTERVIEW ANSWER THIS PROJECT UNLOCKS

When asked "describe your data pipeline architecture":

```
"I built an enterprise data pipeline platform locally to 
deeply understand this layer. It demonstrates four source 
connector patterns: REST API polling, CDC via Debezium 
which tails the PostgreSQL WAL with zero application code, 
document ingestion from S3-compatible storage, and webhook 
receivers. All sources converge into Kafka which acts as 
the unified transport backbone — giving us replay, 
independent consumer groups, and consumer lag visibility 
that SQS cannot provide. A stream processor validates 
schema, detects PII, enriches with lineage metadata, and 
routes to four destinations: OpenSearch for RAG with 
hybrid search, PostgreSQL for feature storage, MinIO for 
the data lake, and Redis for real-time lookups. Full 
observability runs on Prometheus and Grafana tracking the 
four golden signals across every stage. The whole stack 
runs locally on Docker Compose. GitHub: 
github.com/ravimishraggn/enterprise-data-pipeline-platform"
```

That answer, delivered confidently, ends the data 
infrastructure probing immediately.

---

## NOTES FOR CLAUDE CODE

- Build one phase at a time
- Every service needs a Dockerfile
- All config via environment variables (no hardcoded values)
- Use Python 3.11+
- kafka-python for Kafka client
- FastAPI + uvicorn for HTTP services
- sentence-transformers for local embeddings (no API key needed)
- prometheus-client for metrics
- All synthetic data — no real PII anywhere
- Each LEARNING.md should be 300-500 words minimum
- Each ADR should follow the format: Context, Decision, Reasons, Consequences