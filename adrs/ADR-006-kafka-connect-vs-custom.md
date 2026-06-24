# ADR-006: Kafka Connect vs Custom Connector Services

**Status:** Accepted (hybrid approach)  
**Date:** 2024-01-15  
**Domain:** Data Ingestion Architecture

---

## Context

We have multiple data sources to ingest. Two approaches exist:

1. **Kafka Connect** — declarative connector framework; thousands of pre-built connectors
2. **Custom services** — write your own producer in Python/Java/Go

---

## Decision

**Hybrid approach:**
- Use **Kafka Connect + Debezium** for CDC (it's the industry standard, no custom code needed)
- Use **custom Python services** for REST API polling, webhook receiving, and document scanning

---

## Kafka Connect Overview

Kafka Connect is a framework (part of Apache Kafka) that runs connectors as plugins.
You configure a connector via REST API (no code needed for 90% of use cases):

```json
POST /connectors
{
  "name": "jdbc-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://...",
    "mode": "timestamp",
    "timestamp.column.name": "updated_at",
    "topic.prefix": "jdbc."
  }
}
```

Available connectors (open source):
- **Debezium** — CDC for PostgreSQL, MySQL, MongoDB, Oracle, SQL Server
- **JDBC Connector** — poll any SQL database
- **S3/GCS/ADLS Connector** — read/write object stores
- **HTTP Connector** — HTTP source (limited)
- **Elasticsearch/OpenSearch Sink** — write to search indices

### When Kafka Connect is the right choice:
✅ Standard connectors with excellent community support  
✅ Built-in offset management, error handling, retry logic  
✅ Schema Registry integration out of the box  
✅ Horizontal scaling (distribute tasks across workers)  
✅ Configuration-only (no code for most use cases)  

### When custom services are better:
✅ Custom business logic in the connector (PII redaction, enrichment at ingest)  
✅ Webhook receivers (HTTP server can't be modeled as a Kafka Connect task)  
✅ Non-standard sources with no existing connector  
✅ When you need FastAPI endpoints for health checks and operational controls  
✅ Learning purposes — understanding the underlying concepts  

---

## Why We Use Custom Services Here

For this learning project, custom services are intentional:

1. **Understand the internals**: Writing `Producer.produce()` teaches you exactly
   what Kafka Connect is doing behind the scenes
2. **Business logic at ingest**: Our REST API connector applies risk scoring hints
   at ingest time — hard to do in a pure Kafka Connect declarative config
3. **HTTP server dual purpose**: Our webhook receiver is both a Kafka producer AND
   an HTTP server — this can't be done in Kafka Connect's pull model

---

## Production Hybrid Architecture

```
REST APIs       → Custom Python Service (full control over polling logic)
Webhooks        → Custom Python FastAPI (HTTP server requirement)
PostgreSQL CDC  → Kafka Connect + Debezium (best-in-class CDC)
S3/MinIO sink   → Kafka Connect S3 Sink (handles partitioning, batching)
JDBC databases  → Kafka Connect JDBC Connector (handles watermarks)
OpenSearch sink → Kafka Connect OpenSearch Connector (in production)
```

In production, the OpenSearch Writer and MinIO Sink would likely be replaced with
Kafka Connect connectors to get:
- Automatic dead letter queue routing
- Schema Registry integration
- Restart without offset loss
- Built-in retry with exponential backoff

---

## Operational Comparison

| Aspect | Kafka Connect | Custom Service |
|--------|--------------|----------------|
| Deployment | Plugin JAR in Connect cluster | Docker container |
| Monitoring | JMX metrics, Connect REST API | Custom Prometheus metrics |
| Error handling | Built-in DLQ, retry | Must implement yourself |
| Scaling | Horizontal (task distribution) | Horizontal (consumer groups) |
| Schema | Avro/Protobuf via SR | Pydantic / manual |
| Debugging | Kafka Connect logs | Application logs |
| Config changes | REST API, no redeploy | Env var change + restart |
