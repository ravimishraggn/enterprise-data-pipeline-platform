# ADR-004: OpenSearch with Index Lifecycle Management

**Status:** Accepted  
**Date:** 2024-01-15  
**Domain:** Search / Vector Store / Data Lifecycle

---

## Context

We need a destination for processed transactions and document chunks that supports:
1. **Full-text search** (find transactions by description, account, etc.)
2. **Vector similarity search** (semantic search over financial documents)
3. **Time-series data management** (old data should age out or move to cheaper storage)
4. **Aggregations** (dashboards, risk reports)

Options: Elasticsearch, OpenSearch, Pinecone, Weaviate, pgvector (PostgreSQL)

---

## Decision

**OpenSearch** for both full-text search and vector storage, with Index Lifecycle
Management (ILM) for time-series data management.

---

## Why OpenSearch Over Alternatives

### vs Elasticsearch
OpenSearch is the open-source fork of Elasticsearch 7.10 (before the license change).
It's API-compatible and adds:
- Native k-NN (vector search) as a first-class feature
- No licensing concerns (Apache 2.0)
- AWS OpenSearch Service for production

### vs Pinecone / Weaviate (dedicated vector DBs)
Dedicated vector DBs are excellent for pure semantic search but:
- No full-text search (you'd need both OpenSearch AND Pinecone)
- Can't aggregate or filter on non-vector fields without a separate database
- More operational complexity (two systems)

OpenSearch gives us **one system** for both full-text + vector search.

### vs pgvector (PostgreSQL)
pgvector is excellent for small-to-medium vector workloads but:
- Sequential scan for ANN (approximate nearest neighbor) doesn't scale past ~1M vectors
- No distributed sharding for large document corpora
- No native ILM / data tiering

OpenSearch's HNSW index scales to billions of vectors with sub-10ms search.

---

## Index Lifecycle Management (ILM)

Financial transaction data has a predictable lifecycle:

```
HOT phase (0-7 days):
  - Active reads and writes
  - SSD-backed nodes
  - 1 shard per index

WARM phase (7-30 days):
  - Still queryable, no writes
  - HDD-backed nodes (cheaper)
  - Force merge to 1 segment (smaller, faster reads)

COLD phase (30-90 days):
  - Rarely queried
  - Frozen indices (unmounted on demand)

DELETE phase (>90 days):
  - Regulatory minimum retention reached
  - Index deleted (or snapshot to S3 for long-term archival)
```

**ISM Policy** (OpenSearch's ILM equivalent):
```json
{
  "policy": {
    "phases": {
      "hot": { "min_index_age": "0d", "actions": {} },
      "warm": {
        "min_index_age": "7d",
        "actions": {
          "forcemerge": {"max_num_segments": 1},
          "read_only": {}
        }
      },
      "delete": {
        "min_index_age": "90d",
        "actions": { "delete": {} }
      }
    }
  }
}
```

---

## kNN Vector Search Setup

For document embeddings, we use OpenSearch's kNN plugin with HNSW:

```json
"embedding": {
  "type": "knn_vector",
  "dimension": 1536,
  "method": {
    "name": "hnsw",
    "space_type": "cosinesimil",
    "engine": "lucene",
    "parameters": {
      "m": 16,          // connections per node (higher = more accuracy, more memory)
      "ef_construction": 256  // quality of graph build
    }
  }
}
```

**Semantic search query** (find documents similar to "risk exposure in tech sector"):
```json
{
  "knn": {
    "embedding": {
      "vector": [0.1, 0.3, -0.2, ...],
      "k": 10
    }
  }
}
```

**Hybrid search** (combine vector similarity with keyword filter):
```json
{
  "query": {
    "bool": {
      "must": [
        { "match": { "doc_type": "RISK_REPORT" }},
        { "knn": { "embedding": { "vector": [...], "k": 10 }}}
      ]
    }
  }
}
```

---

## In This Project

OpenSearch runs as a single-node local Docker container with security disabled
(development only). The `opensearch_writer` creates two indices:
- `financial-transactions` → full-text + aggregations
- `financial-documents` → kNN vector search with 64-dim embeddings

Access OpenSearch Dashboards at http://localhost:5601 to explore data.

In production:
- Use **AWS OpenSearch Service** (3+ nodes, zone awareness, automated backups)
- Enable HTTPS + fine-grained access control
- Configure ISM policies for data lifecycle management
