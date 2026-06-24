# Enterprise Data Pipeline — System Guide

> Phase 1: Kafka Foundation  
> Stack: Kafka · Zookeeper · PostgreSQL · FastAPI · WebSocket · Docker Compose

---

## Table of Contents

1. [End-to-End Flow](#1-end-to-end-flow)
2. [Dashboard UI — http://localhost:8888](#2-dashboard-ui--httplocalhost8888)
3. [Kafdrop UI — http://localhost:9000](#3-kafdrop-ui--httplocalhost9000)
4. [What Each Container Is Doing](#4-what-each-container-is-doing)
5. [Challenges & Failures as the System Evolves](#5-challenges--failures-as-the-system-evolves)

---

## 1. End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DOCKER NETWORK: kafka-net                           │
│                                                                             │
│  ┌─────────────┐    every 5s     ┌──────────────────────────────────────┐  │
│  │  producer   │ ─── 8 events ──▶│           KAFKA BROKER               │  │
│  │ (Python)    │                 │  topic: raw-market-data              │  │
│  └─────────────┘                 │  ┌──────────┬──────────┬──────────┐ │  │
│                                  │  │Partition0│Partition1│Partition2│ │  │
│                                  │  │ WIPRO.NS │  TCS.NS  │ HDFC.NS  │ │  │
│                                  │  │          │ICICIBANK │RELIANCE  │ │  │
│                                  │  │          │BAJFINANCE│ INFY.NS  │ │  │
│                                  │  │          │          │HCLTECH   │ │  │
│                                  │  └──────────┴──────────┴──────────┘ │  │
│                                  └──────────────────┬───────────────────┘  │
│                                                     │                       │
│                          ┌──────────────────────────┼──────────────────┐   │
│                          │                          │                  │   │
│                          ▼                          ▼                  ▼   │
│                 ┌─────────────────┐     ┌────────────────┐  ┌───────────┐ │
│                 │  consumer-a     │     │  consumer-b    │  │ dashboard │ │
│                 │ group_id:       │     │ group_id:      │  │ group_id: │ │
│                 │ analytics-group │     │ storage-group  │  │ dashboard │ │
│                 │                 │     │                │  │ -group    │ │
│                 │ prints colored  │     │ batches 10 →   │  │           │ │
│                 │ output to logs  │     │ writes to PG   │  │ streams   │ │
│                 └─────────────────┘     └───────┬────────┘  │ via WS to │ │
│                                                 │           │ browser   │ │
│                                          ┌──────▼──────┐   └─────┬─────┘ │
│                                          │ PostgreSQL  │         │        │
│                                          │market_prices│         │        │
│                                          └─────────────┘         │        │
│                                                                   │        │
└───────────────────────────────────────────────────────────────────┼────────┘
                                                                    │
                                                          ┌─────────▼────────┐
                                                          │   Your Browser   │
                                                          │ localhost:8888   │
                                                          └──────────────────┘
```

### Step-by-step walkthrough of one message

**Step 1 — Producer generates an event**

```
producer.py runs build_event("HDFC.NS")
→ drift  = random(-0.8%, +0.8%)
→ price  = 1650 * (1 + drift) = 1,651.32
→ event  = {event_id, entity_id: "HDFC.NS", price: 1651.32, ...}
```

**Step 2 — Kafka routes to a partition**

```
partition = hash("HDFC.NS") % 3  →  always partition 2
```

Every HDFC.NS event lands on Partition 2. This means:
- A consumer can read all HDFC.NS events in exact chronological order
- Multiple consumers reading from different partitions can work in parallel
- If the producer crashes and restarts, the next HDFC.NS event still goes to P2

**Step 3 — Kafka stores the event (durably)**

Kafka appends the message to its commit log on disk. The message gets an offset — a sequential integer unique within that partition (P2 offset 42, 43, 44 ...). The message stays on disk for 7 days (configured via `KAFKA_LOG_RETENTION_HOURS: 168`). Any consumer can re-read it at any time.

**Step 4 — Three independent consumer groups each read the event**

All three groups have their own offset pointer. Kafka sends every message to every group independently.

| Consumer Group | What it does with the event |
|---|---|
| `analytics-group` (consumer-a) | Prints a colored line to stdout with symbol, price, drift, partition, offset |
| `storage-group` (consumer-b) | Accumulates 10 messages, then INSERTs a batch into PostgreSQL, then commits the offset |
| `dashboard-group` (dashboard) | Puts the message into an asyncio Queue; each connected browser gets it via WebSocket |

**Step 5 — Dashboard pushes to browser over WebSocket**

The dashboard's background thread pushes the event into `asyncio.Queue`. The WebSocket handler reads from the queue and calls `await ws.send_text(json.dumps(...))`. The browser receives it in under 100ms of it arriving at Kafka.

**Step 6 — consumer-b writes to PostgreSQL**

After every 10th message, consumer-b runs:
```sql
INSERT INTO market_prices (event_id, entity_id, price, kafka_partition, kafka_offset, ...)
VALUES (...) ON CONFLICT DO NOTHING;
```
Then it calls `consumer.commit()` — only after the DB write succeeds. If Postgres crashes between message 5 and message 10, the next startup will re-read from offset 5 and re-insert. The `ON CONFLICT DO NOTHING` on `event_id` makes this safe — duplicate messages are ignored.

---

## 2. Dashboard UI — http://localhost:8888

The dashboard is a FastAPI app with a single-page HTML/JS frontend. No framework — pure vanilla JS. Communication with the server happens two ways: **WebSocket** (for live messages) and **REST polling** (for consumer group stats and PostgreSQL counts).

### 2.1 Header Bar

```
📡 Kafka Pipeline — Phase 1 Dashboard
● Kafka connected   ● PostgreSQL (320 rows)   Topic: raw-market-data | 3 partitions
                                               [312 messages]  [⚡ Produce Now]  [⏸ Pause Feed]
```

| Element | What it shows | How it works |
|---|---|---|
| **Kafka connected** badge | Green = WebSocket is open and receiving; Yellow = connecting; Red = disconnected | Changes state in `ws.onopen` / `ws.onclose` |
| **PostgreSQL (N rows)** badge | Green with total row count = DB connected; Red = unreachable | Updates every 8s via `/api/db` |
| **Topic info** | Topic name and partition count | Static from server config |
| **N messages counter** | Total messages received since you opened the page | Incremented in `handleMessage()` |
| **⚡ Produce Now** | Sends 8 extra events immediately, bypassing the 5-second interval | `POST /api/produce` → server creates a KafkaProducer, sends all 8 symbols, closes it |
| **⏸ Pause Feed** | Freezes the live feed display without disconnecting | Sets `paused = true`; WebSocket still receives but `handleMessage()` skips rendering |

### 2.2 Live Prices Panel (top-left)

Shows the **most recent price for each of the 8 symbols**. One row per symbol, updated in place on every tick.

```
Symbol    Price (INR)    Change      Partition   Offset
HDFC      ₹ 1,651.32    ↑ 0.081%    P2          offset=48
RELIANCE  ₹ 2,893.41    ↓ 0.226%    P2          offset=49
TCS       ₹ 3,712.88    ↑ 0.347%    P1          offset=33
...
```

| Column | Meaning |
|---|---|
| **Symbol pill** | Short name (`.NS` stripped). Color-coded: each symbol gets a consistent color across all panels |
| **Price (INR)** | Current synthetic price in Indian Rupees, formatted with Indian number grouping (1,00,000 style) |
| **Change** | `drift_pct` from the event — the random ±0.8% applied this cycle. Green with ↑ = positive drift, red with ↓ = negative |
| **Partition** | Which Kafka partition this symbol hashes to. Stays constant — HDFC.NS always shows P2 |
| **Offset** | The Kafka offset of the last message received for this symbol. Monotonically increasing |

When a new message arrives, the row flashes green (price went up) or red (price went down) for 400ms, then fades back to normal.

**Key insight**: The partition column demonstrates the core Kafka concept — hash-based routing is deterministic. No matter how many times you restart the producer, HDFC.NS always lands on the same partition. This guarantees ordering.

### 2.3 Consumer Groups Panel (top-right)

Auto-refreshes every 5 seconds via `GET /api/groups`. Shows the **lag** for each of the three consumer groups.

```
analytics-group                              ● running
Consumed: 384   Lag: 0

  Partition  Committed  End   Lag
  P0         128        128   0
  P1         128        128   0
  P2         128        128   0

storage-group                               ● running
Consumed: 380   Lag: 4

  Partition  Committed  End   Lag
  P0         127        128   1
  P1         127        128   1
  P2         126        128   2

dashboard-group                             ● waiting
...
```

| Field | Meaning |
|---|---|
| **status: running** | `total_msgs > 0` — the group has consumed at least one message |
| **Consumed** | Sum of committed offsets across all partitions — total messages processed |
| **Lag** | `end_offset - committed_offset` per partition, summed. Lag = 0 means fully caught up. Lag > 0 means the consumer is behind the producer |
| **Lag color** | Green = 0 (healthy), Yellow = 1–50 (mild delay), Red = >50 (falling behind) |

**Why does storage-group lag behind analytics-group?**  
consumer-b only commits the offset after writing 10 messages to PostgreSQL. So it's always up to 9 messages behind in committed offset, even though it has received and processed them. This is intentional — at-least-once delivery means you commit only after durable storage.

**How lag is calculated (the real mechanism):**
```python
admin.list_consumer_group_offsets(group_id)   # → what the group committed
consumer.end_offsets(topic_partitions)         # → what's been produced
lag = end_offset - committed_offset
```
This requires two API calls, hence the 5-second polling interval (not WebSocket).

### 2.4 Live Message Feed (full-width)

The most visually active panel. Every Kafka message appears here in real-time, newest at the top. Capped at 100 DOM rows to prevent memory bloat.

```
18:53:47  HDFC   price_update   ₹ 1,637.35  ↓ 0.766%    P2 · offset=44 · v1.0
18:53:47  RELI   price_update   ₹ 2,900.04  ↑ 0.001%    P2 · offset=45 · v1.0
18:53:47  INFY   price_update   ₹ 1,548.44  ↓ 0.101%    P2 · offset=46 · v1.0
...
```

| Column | Source |
|---|---|
| Timestamp | `received_at` — when the dashboard's Kafka consumer received the message (server-side) |
| Symbol pill | `entity_id` with consistent color |
| Event type | Always `price_update` in Phase 1 |
| Price | Formatted in INR |
| Drift | ↑/↓ with percentage |
| Right side | `P{n}` partition, offset number, schema version |

**On first connect**: the server sends the last 20 messages from `message_history` (a server-side `deque(maxlen=200)`) so the feed isn't blank when you open the page.

**WebSocket lifecycle**:  
1. Browser opens `ws://localhost:8888/ws`  
2. Server sends last 20 historical messages immediately  
3. Server streams new messages as they arrive from Kafka  
4. Every 30 seconds with no messages, server sends a heartbeat `{type: "heartbeat"}` — browser ignores it, but it prevents proxy timeouts  
5. If WebSocket closes for any reason, browser auto-reconnects after 3 seconds  

### 2.5 PostgreSQL Records Panel (bottom-left)

Polls `/api/db` every 8 seconds. Shows what consumer-b has actually persisted.

```
Symbol     Events   Min ₹         Max ₹         Avg ₹
HDFC       47       ₹ 1,633.12    ₹ 1,668.90    ₹ 1,651.23
RELIANCE   47       ₹ 2,879.41    ₹ 2,921.03    ₹ 2,900.87
TCS        47       ₹ 3,668.11    ₹ 3,731.44    ₹ 3,700.02
...
```

This panel bridges Kafka and the database — you can compare the live price (WebSocket panel) with the historical min/max/avg (PostgreSQL panel) in real time. The divergence between "Events in Kafka" and "Events in PostgreSQL" shows consumer-b's current batch progress.

### 2.6 Partition Routing Panel (bottom-right)

Built entirely from observed messages — no configuration required. As messages flow through the WebSocket, the browser detects which symbol arrived on which partition.

```
Partition 0    [WIPRO]
Partition 1    [TCS] [ICICIBANK] [BAJFINANCE]
Partition 2    [HDFC] [RELIANCE] [INFY] [HCLTECH]

Key routing: hash(entity_id) % 3 — deterministic per symbol
```

This panel makes the partition assignment visible without needing Kafdrop. The assignment is stable — if you add more messages, nothing moves. This demonstrates why partition keys matter: a consumer assigned to Partition 2 will always see HDFC, RELIANCE, INFY, and HCLTECH — and can build a local state (e.g., a sorted book) knowing it sees all events for those symbols in order.

### 2.7 REST API — Endpoints You Can Call Directly

The dashboard exposes these endpoints (testable in a browser or curl):

| Endpoint | Returns |
|---|---|
| `GET /api/prices` | Latest price per symbol + partition map |
| `GET /api/groups` | Consumer lag for all 3 groups |
| `GET /api/db` | PostgreSQL per-symbol stats (min/max/avg/count) |
| `GET /api/history?limit=50` | Last N messages from server-side deque |
| `GET /api/topic` | Topic name, broker address, partition count |
| `POST /api/produce` | Trigger one extra produce cycle immediately |

---

## 3. Kafdrop UI — http://localhost:9000

Kafdrop is a read-only Kafka browser — it does not produce or consume messages itself. It talks directly to the Kafka broker using the Admin API.

### 3.1 Home Page

When you open Kafdrop you see a list of all topics on the broker:

```
Topic                  Partitions   Replication   Messages
__consumer_offsets     50           1             (internal)
raw-market-data        3            1             ~384
```

`__consumer_offsets` is Kafka's internal topic — it stores the committed offsets for every consumer group. You normally don't read it directly, but Kafdrop shows it because `KAFKA_AUTO_CREATE_TOPICS_ENABLE: false` doesn't affect internal topics.

### 3.2 Topic Detail Page — `/topic/raw-market-data`

Click on `raw-market-data` to see the partition summary:

```
Partition   First Offset   Last Offset   Messages   Leader   Replicas
0           0              128           128        1        [1]
1           0              128           128        1        [1]
2           0              128           128        1        [1]
```

| Column | Meaning |
|---|---|
| **First Offset** | Always 0 in a fresh run (no compaction or retention expiry yet) |
| **Last Offset** | Highest offset written. In a single-broker setup with 8 symbols and 3 partitions: ~1/3 of total messages per partition |
| **Messages** | `Last Offset - First Offset`. Kafdrop counts this per partition |
| **Leader** | Broker ID 1 — our only broker. In a real cluster each partition would have a different leader for load balancing |
| **Replicas** | `[1]` — only one replica (replication-factor=1). In production this would be `[1, 2, 3]` across different brokers |

**Partition message distribution**: Because only 8 symbols hash across 3 partitions unevenly (3+3+2 or 4+2+2 depending on hash), some partitions have more messages than others. This is a natural result of key-based routing — not a bug.

### 3.3 Viewing Individual Messages

From the topic detail page, select a partition and click **View Messages**:

```
Offset   Timestamp            Key         Value (JSON)
44       2026-06-24 18:53:47  HDFC.NS     {"event_id":"...", "entity_id":"HDFC.NS", "price":1637.35, ...}
45       2026-06-24 18:53:47  RELIANCE.NS {"event_id":"...", "entity_id":"RELIANCE.NS", "price":2900.04, ...}
```

**What you can learn here:**
- The raw JSON payload exactly as produced
- The message key (entity_id) — confirms partition routing
- The exact timestamp of when the broker received each message
- You can use "Oldest" / "Newest" / specific offset to jump anywhere in the log
- You can re-read old messages — this is Kafka's replayability. The messages are still there even though consumers read them long ago

### 3.4 Consumer Groups Page — `/consumer` or click "Consumer Groups"

Shows all consumer groups and their per-partition lag:

```
Group ID           Topic              Partition   Start   End    Lag   Member
analytics-group    raw-market-data    0           0       128    0     consumer-a/...
analytics-group    raw-market-data    1           0       128    0     consumer-a/...
analytics-group    raw-market-data    2           0       128    0     consumer-a/...
storage-group      raw-market-data    0           0       127    1     consumer-b/...
storage-group      raw-market-data    1           0       127    1     consumer-b/...
storage-group      raw-market-data    2           0       126    2     consumer-b/...
```

| Column | Meaning |
|---|---|
| **Start** | The committed offset for this group on this partition — where the consumer will start reading next if it restarts |
| **End** | The current end offset — the latest message produced |
| **Lag** | `End - Start`. Zero means fully caught up. If you stop consumer-a for 30 seconds, lag will grow as producer keeps writing; when consumer-a restarts, it catches up and lag returns to 0 |
| **Member** | The consumer instance assigned to this partition. With one container per group, all 3 partitions are assigned to the same member |

**What the "Member" field tells you about rebalancing:**  
If you scaled consumer-a to 2 containers, Kafka would split the 3 partitions between them (2 + 1). Kafdrop would show 2 different member IDs, each owning different partitions. If one crashed, Kafka would trigger a rebalance and reassign its partitions to the surviving consumer — you'd see the member IDs change in Kafdrop within ~30 seconds.

### 3.5 Difference: Kafdrop vs Dashboard

| Capability | Dashboard (port 8888) | Kafdrop (port 9000) |
|---|---|---|
| Live streaming | Yes — WebSocket, sub-second | No — page refresh only |
| Message content | Decoded, formatted | Raw JSON, any format |
| Consumer lag | Yes, computed live | Yes, from Admin API |
| PostgreSQL stats | Yes | No |
| Partition routing map | Yes, visual | Yes, tabular |
| Produce messages | Yes, via button | No (read-only) |
| Replay old messages | No (latest only) | Yes — browse any offset |
| Historical message search | No | Yes — by offset range |
| Schema/format info | No | Shows key/value encoding |

Use the **Dashboard** to watch the pipeline working in real-time. Use **Kafdrop** to inspect specific messages, debug offset problems, or verify what exactly was written to Kafka.

---

## 4. What Each Container Is Doing

### Runtime view (what's happening right now)

```
docker compose ps
```

| Container | Image | Role | Key env vars |
|---|---|---|---|
| `zookeeper` | cp-zookeeper:7.5.0 | Stores Kafka cluster metadata: which broker owns which partition, who is leader, ISR list | ZOOKEEPER_CLIENT_PORT=2181 |
| `kafka` | cp-kafka:7.5.0 | The message broker. Receives from producers, serves to consumers, writes commit logs to disk | KAFKA_ADVERTISED_LISTENERS (two: internal 9092, external 29092) |
| `kafka-init` | cp-kafka:7.5.0 | One-shot container. Creates `raw-market-data` topic with 3 partitions. Exits 0 immediately after. | — |
| `kafdrop` | obsidiandynamics/kafdrop | Read-only Kafka browser UI | KAFKA_BROKERCONNECT=kafka:9092 |
| `postgres` | postgres:15 | Relational store for consumer-b's output | POSTGRES_DB=market_data |
| `producer` | (built locally) | Runs `producer.py` — infinite loop, 8 events every 5s | KAFKA_BROKERS=kafka:9092, INTERVAL=5 |
| `consumer-a` | (built locally) | Runs `consumer_group_a.py` — `analytics-group`, prints to stdout | KAFKA_BROKERS=kafka:9092 |
| `consumer-b` | (built locally) | Runs `consumer_group_b.py` — `storage-group`, batches→PostgreSQL | POSTGRES_DSN=host=postgres... |
| `dashboard` | (built locally) | FastAPI + WebSocket. Background thread reads Kafka, streams to browsers | KAFKA_BROKERS, POSTGRES_DSN, PORT=8888 |

### Startup dependency chain

```
zookeeper → kafka (waits for zookeeper healthy)
         → kafka-init (waits for kafka healthy, creates topic, exits)
         → kafdrop (waits for kafka healthy)
         → producer (waits for kafka healthy)
         → consumer-a (waits for kafka healthy)
         → consumer-b (waits for kafka healthy AND postgres healthy)
         → dashboard (waits for kafka healthy AND postgres healthy)
         → postgres (starts independently, has its own healthcheck)
```

Docker Compose enforces this via `depends_on: condition: service_healthy`. If Kafka takes 45 seconds to start (first boot), all dependent services wait automatically.

---

## 5. Challenges & Failures as the System Evolves

This section documents what will break as you grow this system beyond Phase 1 — and the standard solutions for each.

---

### 5.1 Single Broker = Single Point of Failure

**Current state**: One Kafka broker. Replication factor = 1.

**What breaks**: If the `kafka` container crashes, the topic is unavailable. No consumer can read. No producer can write. Messages produced during the outage are lost.

**Failure mode**:
```
producer.py: KafkaTimeoutError: Failed to update metadata after 30000ms
consumer-a:  CommitFailedError: Commit cannot be completed since the group has already rebalanced
```

**Production fix**:
- Minimum 3 brokers (`KAFKA_BROKER_ID: 1/2/3` across 3 containers or VMs)
- Topic replication factor = 3, `min.insync.replicas = 2`
- This means Kafka can lose one broker and continue serving reads and writes

**ADR note**: `acks=all` (which we already use) only protects you if ISR count ≥ min.insync.replicas. With replication-factor=1, there is one ISR, and losing it means total outage.

---

### 5.2 Consumer Offset Lost After Clean Restart

**Current state**: consumer-b manually commits after DB write. If you run `docker compose down -v` (removes volumes), the `__consumer_offsets` topic is wiped. On next start, `auto_offset_reset="earliest"` means consumer-b re-reads everything from offset 0.

**What breaks**: PostgreSQL gets duplicate rows. Our `ON CONFLICT DO NOTHING` on `event_id` saves us — but only if `event_id` is truly unique. If the producer also restarts and regenerates events with new UUIDs for the same timestamps, duplicates will silently accumulate.

**Failure sequence**:
```
docker compose down -v        # deletes postgres-data and kafka-data
docker compose up -d          # fresh start
# consumer-b starts from offset 0 again
# but postgres is also fresh — so actually fine in dev
# in production: Kafka is persistent but your consumer offset store is separate
```

**Production fix**:
- Never run `down -v` on production data
- Use `auto_offset_reset="earliest"` and idempotent writes (which we have via event_id)
- For true exactly-once: use Kafka transactions (`transactional_id`) + `isolation.level=read_committed`

---

### 5.3 Consumer Group Rebalancing Causes Processing Gaps

**What it is**: When consumer-b restarts, Kafka triggers a group rebalance. During rebalancing, all partition consumption pauses for `session_timeout_ms` (30 seconds in our config). Producer keeps writing. Lag builds up.

**Visible symptom in dashboard**:
```
storage-group   Lag: 47   (was 0 before restart)
```

Then catches up once consumer-b rejoins and starts processing.

**Production fix**:
- Reduce `session_timeout_ms` to 10s for faster detection
- Use `max.poll.interval.ms` tuning — if the consumer takes too long to process a batch, Kafka thinks it's dead and rebalances
- Use incremental cooperative rebalancing (`partition.assignment.strategy=CooperativeStickyAssignor`) — partitions are not revoked all at once; only moved partitions are paused

---

### 5.4 Kafka-Init Race Condition

**Current state**: `kafka-init` creates the topic in a one-shot container. If it runs before Kafka's internal coordinator is fully initialized (even after the healthcheck passes), topic creation fails silently and the container exits with code 0.

**What happened in this session**: The multi-line `kafka-topics` command in docker-compose YAML broke into separate shell commands — `kafka-topics` ran with no arguments, printed help text, then `--create` was treated as a separate command. Exit code 0 (help prints successfully), so Compose thought it succeeded.

**Fix applied**: Moved to `>-` YAML block scalar so the entire command is one line. If this still fails in a fresh environment, the fallback is:
```bash
docker exec kafka kafka-topics --create --if-not-exists \
  --topic raw-market-data --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

**Production fix**: Never use a separate init container for topic creation. Instead, enable `auto.create.topics.enable=true` in dev, or use a Helm chart / Terraform resource for topic provisioning in production. Alternatively, use `kafka-topics.sh` with proper retry logic:
```bash
until kafka-topics --create ... ; do sleep 2; done
```

---

### 5.5 Schema Changes Break Consumers

**Current state**: producer sends `schema_version: "v1.0"`. Consumers directly access `event["price"]`, `event["entity_id"]` etc. with no validation.

**What breaks**: You add a new field `market_cap` in `v1.1`. Old consumer-b tries to access a field that doesn't exist. Or you rename `entity_id` to `symbol` — every consumer that accesses `event["entity_id"]` returns `None` silently.

**Failure mode**: No crash, but silent wrong data:
```python
entity_id = event.get("symbol", "UNKNOWN")  # returns UNKNOWN for all v1.0 messages
```

**Production fix**:
- Schema Registry (Confluent Schema Registry or AWS Glue Schema Registry)
- All producers register the schema before writing. Consumers validate against it before processing.
- Schema evolution rules: BACKWARD (new schema can read old data), FORWARD (old schema can read new data), FULL (both)
- In Phase 1 terms: `schema_version` field in every event is the start of this — but it's unenforced without a registry

---

### 5.6 PostgreSQL Write Bottleneck

**Current state**: consumer-b opens a new `psycopg2` connection per batch, writes 10 rows, closes it. At 8 events/5s = 96 events/min, this is trivial. At 10,000 events/s it breaks.

**What breaks**:
```
psycopg2.OperationalError: FATAL: remaining connection slots are reserved for non-replication superuser connections
```
PostgreSQL default `max_connections = 100`. At scale, each consumer-b replica holds a connection, connection pools overflow.

**Production fix**:
- Connection pooler: PgBouncer in front of PostgreSQL (multiplexes hundreds of app connections into a small pool to Postgres)
- Or use async SQLAlchemy with asyncpg (connection pooled per process)
- Or change the destination entirely: for high-throughput event storage, use a columnar DB (TimescaleDB, ClickHouse) or the object store (MinIO/S3) — Phase 2 in this project

---

### 5.7 Message Queue Overflow in Dashboard

**Current state**: `asyncio.Queue(maxsize=500)`. The background Kafka thread calls `run_coroutine_threadsafe(queue.put(...))`.

**What breaks**: If the dashboard has 0 connected browser clients (no one is viewing), the queue fills to 500 and then blocks. The background Kafka consumer thread stalls. Kafka sees the consumer not polling within `max.poll.interval.ms` and kicks it out of the group. Rebalance triggered.

**Visible symptom**: dashboard-group lag spikes in Kafdrop when no browser is open.

**Production fix**:
```python
# Non-blocking put — drop if queue full
try:
    asyncio.run_coroutine_threadsafe(queue.put_nowait(record), loop)
except asyncio.QueueFull:
    pass  # acceptable data loss for a UI feed
```
A dashboard is not a durable consumer — it's fine to drop messages when no one is watching.

---

### 5.8 No Dead Letter Queue

**Current state**: If a consumer receives a malformed message (truncated JSON, missing required field), it will crash the consumer loop. The container restarts (`restart: unless-stopped`), re-reads from the last committed offset, hits the same bad message again, and enters a crash loop.

**Failure mode**:
```
consumer-a restarts 47 times in 2 hours, crash looping on offset 412
consumer_a exits with code 1 (JSONDecodeError)
```
The producer keeps writing. Partition 0 is blocked from offset 412 onward for analytics-group. Lag grows forever.

**Production fix**: Wrap message processing in try/except. On failure, route to a Dead Letter Queue topic (`dlq.raw-market-data`):
```python
try:
    process(msg)
except Exception as e:
    producer.send("dlq.raw-market-data", value={"original": msg.value, "error": str(e), "offset": msg.offset})
    consumer.commit()  # skip the bad message
```
A separate DLQ consumer alerts on unexpected messages without blocking the main pipeline.

---

### 5.9 Port Conflict in Local Dev (What Happened Here)

**What happened**: `dealprep-postgres` from another project was already using port 5432. Docker refused to start our `postgres` container.

**Fix applied**: Changed host port mapping to `5433:5432` — the internal Docker network still uses 5432, so consumer-b's connection string (`host=postgres port=5432`) is unaffected. Only the host-machine access port changed.

**General rule**: Container-to-container connections use the internal port (5432). Host machine connections use the mapped port (5433). Never hardcode host ports in connection strings used inside Docker — use the service name and internal port.

**Production fix**: In Kubernetes, this problem disappears entirely — services get DNS names and ports are fully internal. In local dev, use explicit port mapping overrides in `.env` files per project.

---

### 5.10 No Observability = Flying Blind

**Current state**: We see Kafka lag in the dashboard, but there are no alerts, no SLOs, no metric history.

**What you miss**:
- Consumer-b lag crosses 10,000 — you don't find out until the dashboard is manually opened
- PostgreSQL write latency spikes from 2ms to 800ms — nobody notices until rows stop appearing
- Producer starts generating corrupt events at 03:00 — dashboard shows green because messages are flowing

**Production fix** (Phase 3 in this project):
- Prometheus: expose `/metrics` from every service. Scrape every 15s.
- Key metrics per service:
  - Producer: `messages_produced_total`, `produce_latency_seconds`
  - Consumer: `messages_consumed_total`, `consumer_lag_gauge`, `db_write_latency_seconds`
  - Dashboard: `ws_clients_connected`, `queue_depth_gauge`
- Grafana: dashboards + alerting rules (alert if lag > 100 for > 2 minutes)
- This is not optional for production — it's the difference between noticing a problem in 2 minutes vs 2 hours

---

### Summary Table — Failure Risk vs Complexity to Fix

| Failure | Likelihood in prod | Fix complexity | Phase to address |
|---|---|---|---|
| Single broker SPOF | Certain at scale | Medium (add 2 brokers) | Phase 2 |
| Schema change breaks consumers | Certain over time | Medium (schema registry) | Phase 3 |
| Consumer rebalancing gaps | Frequent | Low (tune timeouts) | Phase 2 |
| DLQ missing — crash loop | Occasional | Low (try/except + DLQ topic) | Phase 2 |
| PG connection exhaustion | At scale | Medium (PgBouncer) | Phase 2 |
| Queue overflow in dashboard | Rare (no users) | Trivial (put_nowait) | Now |
| Port conflicts in dev | Common | Trivial (.env overrides) | Now |
| No observability | Guaranteed pain | High (Prometheus + Grafana) | Phase 3 |
| Kafka-init race | Occasional | Low (retry loop) | Phase 2 |
| Offset lost on volume wipe | Dev only | None (dev practice) | — |
