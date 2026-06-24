# ADR-005: Schema Registry for Contract Enforcement

**Status:** Accepted (Confluent Schema Registry for production; PostgreSQL catalog for POC)  
**Date:** 2024-01-15  
**Domain:** Data Governance / Schema Management

---

## Context

Without schema enforcement, Kafka is a wild west where producers can change message
format without warning consumers. In a financial pipeline:

- A producer renames `amount` → `transaction_amount`
- All consumers start throwing `KeyError: 'amount'`
- Risk alerts stop firing
- The incident is discovered 4 hours later during EOD reconciliation

Schema Registry prevents this by making schema compatibility a deployment gate.

---

## Decision

**Confluent Schema Registry** for production (with Avro or Protobuf serialization).  
**PostgreSQL `schema_catalog` table** for this local POC.

---

## See LEARNING.md

Full technical details are in [schema_registry/LEARNING.md](./LEARNING.md).

---

## Schema Evolution Rules

### BACKWARD Compatible Changes (safe to deploy):
✅ Add optional field with a default value  
✅ Remove a field that consumers don't use  
✅ Add an enum value  

### FORWARD Compatible Changes (require consumer update first):
⚠️  Remove a field consumers depend on  
⚠️  Change field type (int → long is ok, string → int is not)  

### BREAKING Changes (never do in production):
❌ Rename a field  
❌ Change field type incompatibly  
❌ Remove required fields  

---

## Schema Registration Workflow

```
Developer proposes schema change
    │
    ▼
CI/CD pipeline runs schema compatibility check:
  $ curl -X POST .../subjects/raw.transactions-value/compatibility/versions/latest \
    -d @new_schema.avsc
  → {"is_compatible": true}  ← merge allowed
  → {"is_compatible": false} ← build blocked
    │
    ▼ (if compatible)
Deploy producer with new schema
Schema Registry auto-registers new version
    │
    ▼
Consumers read with old schema version
Old schema still works (BACKWARD compatible)
    │
    ▼
Deploy consumers at your own pace
```

---

## Production Configuration

```yaml
# Kafka producer with Avro + Schema Registry:
producer:
  bootstrap.servers: kafka:9092
  schema.registry.url: http://schema-registry:8081
  value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer

# Consumer:
consumer:
  schema.registry.url: http://schema-registry:8081
  value.deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
  specific.avro.reader: true
```

---

## In This Project

Our local POC uses:
1. **Pydantic models** in each service for schema validation
2. **PostgreSQL `schema_catalog` table** as a human-readable schema registry
3. **`schema_version` field** in every message to track which schema was used

This gives us the **documentation** and **audit trail** benefits of a schema registry
without the operational overhead. For production, swap Pydantic for Avro + Confluent SR.
