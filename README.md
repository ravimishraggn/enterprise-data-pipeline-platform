# Enterprise Data Pipeline Platform

A production-grade data pipeline platform demonstrating enterprise patterns in financial
services data engineering. Everything runs locally via Docker Compose.

---

## Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                    ENTERPRISE DATA PIPELINE PLATFORM                             ║
║                         (Financial Services Domain)                              ║
╚══════════════════════════════════════════════════════════════════════════════════╝

┌─────────────────────────────── SOURCE LAYER ────────────────────────────────────┐
│                                                                                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌────────────┐ │
│  │  REST API        │  │  CDC Connector   │  │  Document        │  │  Webhook   │ │
│  │  Connector       │  │  (Debezium)      │  │  Connector       │  │  Receiver  │ │
│  │                  │  │                  │  │  (MinIO/S3)      │  │  (FastAPI) │ │
│  │ polls synthetic  │  │ PostgreSQL WAL   │  │ polls buckets    │  │ HTTP POST  │ │
│  │ market + txn API │  │ → change events  │  │ → PDF, JSON      │  │ risk alerts│ │
│  │  :8001/docs      │  │  Debezium :8083  │  │                  │  │  :8002/docs│ │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  └─────┬──────┘ │
│           │                     │                      │                   │       │
└───────────┼─────────────────────┼──────────────────────┼───────────────────┼───────┘
            │                     │                      │                   │
            ▼                     ▼                      ▼                   ▼
┌───────────────────────────── KAFKA BACKBONE ────────────────────────────────────┐
│                                                                                   │
│  ┌────────────────┐  ┌────────────────────┐  ┌───────────────┐  ┌────────────┐ │
│  │raw.transactions│  │raw.cdc.transactions│  │ raw.documents │  │raw.webhooks│ │
│  └────────────────┘  └────────────────────┘  └───────────────┘  └────────────┘ │
│                                                                                   │
│  ┌──────────────────────┐  ┌─────────────────────┐  ┌──────────────────────┐   │
│  │processed.transactions│  │  processed.documents │  │     dlq.transactions │   │
│  └──────────────────────┘  └─────────────────────┘  └──────────────────────┘   │
│                                                                                   │
│  ┌──────────────────────┐  ┌─────────────────────┐                              │
│  │    audit.lineage     │  │      audit.pii       │    Kafka UI: :8080          │
│  └──────────────────────┘  └─────────────────────┘                              │
└────────────────────────────────┬────────────────────────────────────────────────┘
                                 │
              ┌──────────────────┴──────────────────┐
              ▼                                     ▼
┌─────────────────────────────────┐   ┌─────────────────────────────────────────┐
│       PROCESSOR LAYER            │   │           GOVERNANCE LAYER               │
│                                  │   │                                          │
│  ┌──────────────────────────┐   │   │  ┌────────────────┐  ┌───────────────┐  │
│  │    Stream Processor      │   │   │  │  PII Detector  │  │Lineage Tracker│  │
│  │                          │   │   │  │                │  │               │  │
│  │ 1. Normalize (3 schemas) │   │   │  │ regex + Luhn   │  │ tracks every  │  │
│  │ 2. Validate              │   │   │  │ email, SSN,    │  │ hop a record  │  │
│  │ 3. PII scan              │   │   │  │ credit card,   │  │ makes through │  │
│  │ 4. Risk enrichment       │   │   │  │ IBAN, phone    │  │ the pipeline  │  │
│  │ 5. Lineage event         │   │   │  │                │  │               │  │
│  │ 6. Route → DLQ or output │   │   │  │  :8011/scan    │  │  :8010/docs   │  │
│  │      :8003/metrics       │   │   │  └────────────────┘  └───────────────┘  │
│  └──────────────────────────┘   │   └─────────────────────────────────────────┘
│                                  │
│  ┌──────────────────────────┐   │
│  │  Document Processor      │   │
│  │                          │   │
│  │ chunk text (500 char     │   │
│  │ with 100 char overlap)   │   │
│  │ embed → 64-dim vector    │   │
│  │ (swap sentence-tfmrs     │   │
│  │  for production)         │   │
│  └──────────────────────────┘   │
└─────────────────────────────────┘
              │
              ▼
┌─────────────────────────────── DESTINATION LAYER ──────────────────────────────┐
│                                                                                   │
│  ┌────────────────────┐  ┌─────────────────┐  ┌──────────────┐  ┌───────────┐  │
│  │  OpenSearch Writer  │  │PostgreSQL Writer │  │  MinIO Sink  │  │Redis Cache│  │
│  │                    │  │                  │  │              │  │           │  │
│  │ financial-          │  │ processed_txns   │  │ raw-events-  │  │acct:latest│  │
│  │  transactions index │  │ table (feature   │  │ archive/     │  │acct:hist  │  │
│  │ financial-docs index│  │ store)           │  │ year/mo/day/ │  │leaderboard│  │
│  │ kNN vector search   │  │ ON CONFLICT      │  │ hour/        │  │ :risk     │  │
│  │ for RAG             │  │ DO NOTHING       │  │ batch*.ndjson│  │ pub/sub   │  │
│  │                    │  │                  │  │              │  │ alerts    │  │
│  │  OS Dash: :5601    │  │  Postgres: :5432 │  │  MinIO: :9001│  │ Redis:6379│  │
│  └────────────────────┘  └─────────────────┘  └──────────────┘  └───────────┘  │
└───────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────── OBSERVABILITY LAYER ────────────────────────────────┐
│                                                                                   │
│  Prometheus :9090  ──scrapes all /metrics endpoints──►  Grafana :3000           │
│  Dashboard: "Pipeline Overview" (throughput, latency, risk, PII, DLQ, lineage) │
└───────────────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites
- Docker Desktop (8GB+ RAM allocated)
- Python 3.11+ (for integration tests or CDC registration)

### 1. Start the full stack

```bash
docker-compose up -d
```

First start takes ~3-5 minutes to pull images and initialize.

### 2. Verify health

```bash
docker-compose ps
curl http://localhost:8001/health   # REST connector
curl http://localhost:8002/health   # Webhook receiver
curl http://localhost:8010/health   # Lineage tracker
curl http://localhost:9200/_cluster/health  # OpenSearch
```

### 3. Register the CDC connector

```bash
pip install httpx
python connectors/cdc_connector/register_connector.py
```

### 4. Send a test webhook

```bash
curl -X POST http://localhost:8002/webhook/simulate-alerts?count=5

curl -X POST http://localhost:8002/webhook/risk-alert \
  -H "Content-Type: application/json" \
  -d '{"alert_type":"FRAUD_SUSPECTED","severity":"HIGH","account_id":"ACC-001","description":"Unusual pattern","risk_score":0.92}'
```

### 5. Explore the UI

| Interface | URL | Credentials |
|-----------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| OpenSearch Dashboards | http://localhost:5601 | — |
| MinIO Console | http://localhost:9001 | admin / password123 |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Lineage API | http://localhost:8010/docs | — |
| Webhook Docs | http://localhost:8002/docs | — |

### 6. Run integration tests

```bash
cd tests/integration
pip install -r requirements.txt
pytest test_pipeline.py -v --timeout=60
```

---

## Kafka Topics

| Topic | Producer | Consumer(s) | Purpose |
|-------|----------|-------------|---------|
| `raw.transactions` | REST connector | Stream processor, MinIO sink | Inbound transactions |
| `raw.cdc.transactions` | Debezium | Stream processor | DB change events |
| `raw.documents` | Document connector | Document processor, MinIO sink | Inbound documents |
| `raw.webhooks` | Webhook receiver | Stream processor, PII detector | Risk alerts |
| `processed.transactions` | Stream processor | OS writer, PG writer, Redis writer | Enriched records |
| `processed.documents` | Document processor | OpenSearch writer | Embedded doc chunks |
| `dlq.transactions` | Stream processor | (manual review) | Failed validation |
| `audit.lineage` | Stream processor | Lineage tracker | Lineage events |
| `audit.pii` | Stream processor, PII detector | Compliance team | PII detections |

---

## Project Structure

```
enterprise-data-pipeline-platform/
├── docker-compose.yml
├── README.md
├── LEARNING_PATH.md
├── infrastructure/postgres/init.sql
├── connectors/
│   ├── rest_api_connector/    (main.py, Dockerfile, requirements.txt, LEARNING.md)
│   ├── cdc_connector/         (register_connector.py, LEARNING.md)
│   ├── document_connector/    (main.py, Dockerfile, requirements.txt, LEARNING.md)
│   └── webhook_receiver/      (main.py, Dockerfile, requirements.txt, LEARNING.md)
├── processors/
│   ├── stream_processor/      (main.py, Dockerfile, requirements.txt, LEARNING.md)
│   └── document_processor/    (main.py, Dockerfile, requirements.txt)
├── destinations/
│   ├── opensearch_writer/     (main.py, Dockerfile, requirements.txt)
│   ├── postgres_writer/       (main.py, Dockerfile, requirements.txt)
│   ├── minio_sink/            (main.py, Dockerfile, requirements.txt)
│   └── redis_writer/          (main.py, Dockerfile, requirements.txt)
├── governance/
│   ├── pii_detector/          (main.py, Dockerfile, requirements.txt)
│   ├── lineage_tracker/       (main.py, Dockerfile, requirements.txt)
│   └── schema_registry/       (LEARNING.md — ADR only)
├── observability/
│   ├── prometheus/prometheus.yml
│   └── grafana/provisioning + dashboards
├── adrs/                      (6 Architecture Decision Records)
└── tests/integration/test_pipeline.py
```

---

## Tech Stack

| Component | Technology | Local Port |
|-----------|-----------|------------|
| Message bus | Apache Kafka 7.5 + Zookeeper | 9092 / 29092 |
| CDC | Debezium 2.4 on Kafka Connect | 8083 |
| Vector store | OpenSearch 2.11 | 9200 / 5601 |
| Feature store | PostgreSQL 15 | 5432 |
| Cache | Redis 7 | 6379 |
| Object store | MinIO | 9000 / 9001 |
| Metrics | Prometheus | 9090 |
| Dashboards | Grafana | 3000 |
| Services | Python 3.11 + FastAPI | 8001-8011 |

---

## Stopping

```bash
# Stop without removing data:
docker-compose stop

# Stop and remove all data volumes:
docker-compose down -v
```
