# Schema Registry — ADR & Learning Notes

## What Is a Schema Registry?

A schema registry is a centralized service that stores and enforces the schema
(structure/shape) of every message flowing through Kafka. Without it, producers can
publish messages in any format and consumers break silently when the schema changes.

## The Problem It Solves

```
Producer v1 publishes: {"amount": 100.00, "currency": "USD"}
Producer v2 publishes: {"amount": "100.00", "currency": "USD"}  ← type changed to string!

Consumer tries to parse "100.00" as a float → CRASH
```

A schema registry prevents this by requiring producers to register a schema and
reject messages that don't conform.

## How It Works (Confluent Schema Registry)

```
1. Producer registers schema:
   POST /subjects/raw.transactions-value/versions
   {"schema": "{\"type\":\"record\",\"name\":\"Transaction\",...}"}
   ← Returns schema_id: 42

2. Producer serializes message:
   [magic_byte=0][schema_id=42][avro_bytes...]

3. Consumer deserializes:
   Read schema_id 42 → fetch schema → deserialize bytes

4. Schema evolution:
   Producer updates schema (adds optional field) → registers v2
   Old consumers still work (BACKWARD compatible)
   New consumers get the new field with a default
```

## Schema Compatibility Modes

| Mode | Meaning | Use When |
|------|---------|----------|
| `BACKWARD` | New schema can read old data | Adding optional fields (most common) |
| `FORWARD` | Old schema can read new data | Removing optional fields |
| `FULL` | Both directions | Ultra-stable APIs |
| `NONE` | No checks | Development only |

In financial services, `BACKWARD` is the standard choice.

## Avro vs JSON vs Protobuf

| Format | Size | Speed | Schema Evolution | Readability |
|--------|------|-------|-----------------|-------------|
| JSON | Large | Slow | Manual | Human-readable |
| Avro | Small | Fast | Schema Registry enforced | Binary |
| Protobuf | Smallest | Fastest | Explicit field numbers | Binary |

Confluent Schema Registry supports all three. Avro is the historical default.
Protobuf is increasingly preferred for new systems (explicit field IDs = safer evolution).

## Why We Use ADR Instead of Running It

Confluent Schema Registry requires:
- A Confluent Platform license (or use of Confluent Cloud) for full features
- JVM-based service (adds 512MB+ memory to local stack)
- Integration with every producer/consumer (requires Confluent's serializer libraries)

For this learning project, we simulate schema validation with Pydantic in each
service. The local `schema_catalog` PostgreSQL table serves as a simple registry.

## Production Setup

```yaml
# Add to docker-compose.yml for production-like local testing:
schema-registry:
  image: confluentinc/cp-schema-registry:7.5.0
  depends_on: [kafka]
  ports:
    - "8081:8081"
  environment:
    SCHEMA_REGISTRY_HOST_NAME: schema-registry
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
    SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
```

Then replace:
```python
# Before (JSON):
producer.produce(topic, value=json.dumps(record).encode())

# After (Avro with schema enforcement):
from confluent_kafka.avro import AvroProducer
producer = AvroProducer(
    {"bootstrap.servers": ..., "schema.registry.url": "http://schema-registry:8081"},
    default_value_schema=avro.loads(SCHEMA_JSON)
)
producer.produce(topic=..., value=record)
```

## Our Local Schema Catalog

The `schema_catalog` table in PostgreSQL provides a simple registry for learning:
```sql
INSERT INTO schema_catalog (subject, schema_json, version, compatibility)
VALUES ('raw.transactions-value', '{"type": "object", ...}', 1, 'BACKWARD');
```

Query it:
```bash
docker exec -it postgres psql -U pipeline -d financial_db -c \
  "SELECT subject, version, compatibility FROM schema_catalog;"
```
