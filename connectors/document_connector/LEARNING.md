# Document Connector — Learning Notes

## The Object Store Polling Pattern

Object stores (S3, MinIO, GCS) don't push events by default — they hold files. To
ingest documents as they arrive, you have two choices:

| Approach | How | Pros | Cons |
|----------|-----|------|------|
| **Poll** | Connector lists the bucket periodically and finds new objects | Simple, no infra needed | Latency = poll interval, wasted calls when empty |
| **Event notification** | S3/MinIO sends a webhook on PutObject | Near-real-time | Requires infra (SNS, Lambda, or MinIO webhook config) |

This connector uses **polling + checkpointing** for simplicity. In production you'd
wire up MinIO's webhook notification → webhook_receiver → Kafka.

## Checkpointing: Avoiding Reprocessing

The checkpoint file (`/tmp/doc_connector_checkpoint.json`) stores object names of
everything already sent to Kafka. On each poll:

```
1. List all objects in bucket
2. Filter to objects NOT in checkpoint
3. Process only the new ones
4. Append to checkpoint file
```

This gives you **exactly-once** delivery at the connector level. Real production systems
use:
- **Kafka consumer group offsets** → Kafka itself tracks "where you left off"
- **Database checkpoints** → Store offset in PostgreSQL so it survives restarts
- **Watermark columns** → `WHERE created_at > last_seen_timestamp`

## The Big File Problem

What happens when a document is 500MB? You can't put it in a Kafka message (default
max: 1MB). Two strategies:

1. **Reference pattern** ← what we use: Kafka message contains the MinIO path
   (`s3://bucket/file.pdf`); downstream services fetch the actual bytes from MinIO.
2. **Chunked upload**: Split the document into pages/chunks and send each chunk as
   a separate Kafka message.

For financial documents (typically PDFs < 10MB), we include a 2KB content preview
inline and keep the full reference. The `document_processor` then fetches the full
content from MinIO for chunking and embedding.

## MinIO as Local S3

MinIO is 100% S3-API-compatible. The Python `minio` library works identically against
AWS S3 by changing the endpoint. This is a common pattern for:
- Local development (no AWS account needed)
- Hybrid cloud (data on-prem, processing in cloud)
- Air-gapped environments (financial institutions, government)

## Document Metadata

We extract metadata from the MinIO object's user-defined metadata headers
(`x-amz-meta-*`). This is S3 standard — you set it when uploading:

```python
client.put_object(..., metadata={"doc-type": "TRADE_CONFIRMATION", "tags": "trade,equity"})
```

This metadata flows through the pipeline, enabling routing rules like
"send all COMPLIANCE_MEMO documents to the legal team's index".
