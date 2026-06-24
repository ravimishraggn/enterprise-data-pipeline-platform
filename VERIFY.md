# Verification Guide — Phase 1: Kafka Foundation

Run these checks in order. Each one confirms a specific component works correctly.

---

## Prerequisites

```bash
# Install Python dependencies (once)
make install

# Or manually:
pip install kafka-python==2.0.2 psycopg2-binary==2.9.9
```

---

## Step 1 — Start the Stack

```bash
make start
```

**Expected output:**
```
Starting Kafka Foundation stack ...
...
Stack is ready!
  Kafdrop UI  → http://localhost:9000
  PostgreSQL  → localhost:5432
```

**Verify containers are healthy:**
```bash
make status
# or:
docker compose ps
```

Expected: all 5 services (`zookeeper`, `kafka`, `kafka-init`, `kafdrop`, `postgres`)
show as `running` or `healthy`. `kafka-init` exits after creating topics — that is expected.

---

## Step 2 — Verify Kafka Topics Were Created

```bash
make topics
```

**Expected output:**
```
Kafka topics:
raw-market-data
```

---

## Step 3 — Verify Kafdrop UI

Open: **http://localhost:9000**

What to look for:
- `raw-market-data` topic listed on the home screen
- Click it → see **3 partitions** (P0, P1, P2)
- Each partition shows **0 messages** (nothing produced yet)
- No consumer groups yet (Groups panel is empty)

---

## Step 4 — Start the Producer (Terminal 1)

```bash
# In terminal 1:
make produce
```

**Expected output:**
```
Connecting to Kafka at localhost:29092 ...
Connected. Producing to topic 'raw-market-data' every 5.0s.

[14:23:01] Cycle 1 — producing 8 events:
  → HDFC.NS           price=  1,651.32 INR  drift=+0.080%
  → RELIANCE.NS       price=  2,895.40 INR  drift=-0.159%
  → TCS.NS            price=  3,698.14 INR  drift=-0.051%
  → INFY.NS           price=  1,548.77 INR  drift=-0.079%
  → ICICIBANK.NS      price=    951.22 INR  drift=+0.129%
  → WIPRO.NS          price=    450.90 INR  drift=+0.200%
  → HCLTECH.NS        price=  1,249.38 INR  drift=-0.050%
  → BAJFINANCE.NS     price=  7,098.45 INR  drift=-0.022%
  ✓ delivered → topic=raw-market-data  partition=1  offset=0
  ✓ delivered → topic=raw-market-data  partition=0  offset=0
  ... (8 delivery confirmations)
```

**Verify in Kafdrop (refresh http://localhost:9000):**
- `raw-market-data` now shows message counts growing
- Click a partition → click **View Messages** → see the JSON payloads
- Confirm `entity_id`, `price`, `currency: "INR"`, `schema_version: "v1.0"` are present

---

## Step 5 — Start Consumer Group A (Terminal 2)

```bash
# In terminal 2 (keep terminal 1 producing):
make consume-a
```

**Expected output:**
```
======================================================================
  Consumer Group A  (group_id: analytics-group)
  Topic: raw-market-data  |  Broker: localhost:29092
======================================================================
  This group receives ALL messages independently of Group B.
  Offsets are committed automatically every 1 second.
  Press Ctrl-C to stop.

  Connected. Waiting for messages ...

[14:23:01] HDFC.NS           ₹  1,651.32  ↑ 0.080%   │ partition=1  offset=0  key=HDFC.NS
[14:23:01] RELIANCE.NS       ₹  2,895.40  ↓ 0.159%   │ partition=2  offset=0  key=RELIANCE.NS
...
```

**Verify:**
- Messages appear for every cycle the producer runs
- `partition=` numbers vary (different symbols route to different partitions)
- `offset=` increases with each message on that partition
- Refresh Kafdrop → **Groups** → `analytics-group` appears

---

## Step 6 — Start Consumer Group B (Terminal 3)

```bash
# In terminal 3 (keep terminals 1 and 2 running):
make consume-b
```

**Expected output:**
```
======================================================================
  Consumer Group B  (group_id: storage-group)
  Topic: raw-market-data  |  Broker: localhost:29092
  Writing to PostgreSQL (market_prices table)
======================================================================
  Offsets committed MANUALLY after each successful DB write.
  Group B's position is tracked independently from Group A.
  Press Ctrl-C to stop.

  PostgreSQL connected.
  Kafka connected. Waiting for messages ...

  [received] HDFC.NS           ₹  1,651.32  partition=1  offset=0  (batch 1/10)
  [received] RELIANCE.NS       ₹  2,895.40  partition=2  offset=0  (batch 2/10)
  ...
  ✓ Committed 10 rows to PostgreSQL + Kafka offset. Total stored: 10
```

**Key observation:** Notice Consumer Group B may show messages from offset 0
even though Group A already consumed them. This proves the two groups are independent.

---

## Step 7 — Verify Both Groups Receive All Messages Independently

Keep all three terminals running for 30 seconds. Then check Kafdrop:

**http://localhost:9000 → Groups:**

```
analytics-group   raw-market-data   Partition 0: offset 12, lag 0
                                    Partition 1: offset 11, lag 0
                                    Partition 2: offset 13, lag 0

storage-group     raw-market-data   Partition 0: offset 12, lag 0
                                    Partition 1: offset 11, lag 0
                                    Partition 2: offset 13, lag 0
```

Both groups should show the **same offsets** (they've both consumed the same messages)
and **lag 0** (they're keeping up with the producer).

**This confirms:** two groups, same messages consumed independently.

---

## Step 8 — Verify PostgreSQL Has Data (Consumer Group B)

```bash
make pg-check
```

**Expected output:**
```
Records stored by Consumer Group B:
  entity_id     | events | min_price | max_price | last_stored
----------------+--------+-----------+-----------+---------------------------
 BAJFINANCE.NS  |     12 |   7091.36 |   7108.54 | 2024-01-15 14:23:31+00
 HCLTECH.NS     |     12 |   1245.23 |   1255.88 | 2024-01-15 14:23:31+00
 HDFC.NS        |     12 |   1647.14 |   1653.80 | 2024-01-15 14:23:31+00
 ICICIBANK.NS   |     12 |    947.62 |    953.95 | 2024-01-15 14:23:31+00
 INFY.NS        |     12 |   1543.19 |   1557.31 | 2024-01-15 14:23:31+00
 RELIANCE.NS    |     12 |   2890.14 |   2908.77 | 2024-01-15 14:23:31+00
 TCS.NS         |     12 |   3686.42 |   3713.08 | 2024-01-15 14:23:31+00
 WIPRO.NS       |     12 |    447.15 |    452.78 | 2024-01-15 14:23:31+00
(8 rows)
```

All 8 symbols with growing event counts. Consumer Group B stored the data in
PostgreSQL completely independently of Consumer Group A.

---

## Step 9 — Verify Consumer Independence: Stop and Restart Group A

```bash
# In terminal 2: press Ctrl-C to stop Consumer Group A

# Wait 30 seconds (producer keeps running, Group A accumulates lag)

# Restart Group A:
make consume-a
```

**Expected behavior:**
- Consumer Group A re-joins and catches up from where it left off (not from 0)
- Consumer Group B is unaffected — it continued normally during the outage
- Kafdrop shows Group A's lag decreasing as it catches up

---

## Step 10 — Bonus: Consumer Partition Assignment

Run three instances of Consumer Group A simultaneously (in 3 separate terminals):

```bash
# Terminal A1:
make consume-a

# Terminal A2:
make consume-a

# Terminal A3:
make consume-a
```

Watch Kafdrop → Groups → `analytics-group`. You should see:
- Partition 0 assigned to one consumer instance
- Partition 1 assigned to another
- Partition 2 assigned to the third

Each terminal will only show messages from ITS assigned partition.
HDFC.NS events always go to the same partition (keyed routing is deterministic).

---

## Troubleshooting

### Producer fails with "NoBrokersAvailable"
```bash
# Is Kafka running?
docker compose ps kafka
# Check logs:
docker compose logs kafka --tail=20
# Give it more time:
sleep 15 && make produce
```

### Consumer shows no messages
```bash
# Is the producer running? Check terminal 1.
# Did the topic get created?
make topics
# Reset offset to re-read from the beginning:
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group analytics-group \
  --topic raw-market-data \
  --reset-offsets --to-earliest --execute
```

### Consumer Group B can't connect to PostgreSQL
```bash
# Is postgres healthy?
docker compose ps postgres
# Check init logs:
docker compose logs postgres --tail=30
# Test connection directly:
docker exec postgres psql -U pipeline -d market_data -c "SELECT 1;"
```

### Kafdrop not loading
```bash
docker compose logs kafdrop --tail=20
# Kafdrop needs Kafka healthy first:
docker compose restart kafdrop
```

---

## Summary Checklist

- [ ] `make start` → all 5 containers healthy
- [ ] `make topics` → `raw-market-data` listed
- [ ] http://localhost:9000 → Kafdrop loads, shows topic with 3 partitions
- [ ] `make produce` → events flow every 5 seconds with delivery confirmations
- [ ] Kafdrop → messages visible with JSON content
- [ ] `make consume-a` → terminal prints colored price updates
- [ ] `make consume-b` → PostgreSQL writes confirmed every 10 messages
- [ ] `make pg-check` → 8 rows with growing event counts
- [ ] Kafdrop → Groups shows BOTH `analytics-group` AND `storage-group`
- [ ] Both groups at same offsets (both consumed all messages independently)
