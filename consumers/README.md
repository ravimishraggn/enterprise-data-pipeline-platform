# Consumers — Learning Notes

## Why Two Consumer Groups?

Both `consumer_group_a.py` and `consumer_group_b.py` read from the **same topic**.
Each has a **different `group_id`**. This is the most important concept in Kafka
consumption to understand:

```
raw-market-data topic (3 partitions)
    │
    ├─── analytics-group ──► consumer_group_a.py  (prints to terminal)
    │         └─ offset tracked independently
    │
    └─── storage-group   ──► consumer_group_b.py  (writes to PostgreSQL)
              └─ offset tracked independently
```

Kafka does not "deliver" messages to consumers. Each consumer group maintains its
own read cursor (offset) per partition. The broker just stores messages; groups
read at their own pace.

## Consumer Groups: What Offsets Mean

```
Partition 0 messages: [0] [1] [2] [3] [4] [5] ...
                                            ↑
                                  analytics-group committed up to here

Partition 0 messages: [0] [1] [2] [3] [4] [5] ...
                               ↑
                     storage-group committed up to here (slightly behind)
```

Each group's offset is completely independent. Stopping Group A does NOT affect
Group B's position. Restarting Group A picks up from where it left off.

## At-Least-Once Delivery

```
Consumer reads message at offset 42
       ↓
Consumer processes it (prints, or writes to DB)
       ↓
Consumer commits offset 42 to Kafka
```

If the consumer crashes AFTER processing but BEFORE committing:
- On restart, it reads offset 42 again (re-processes)
- Result: message processed at least once (possibly twice)

Group A uses `enable_auto_commit=True` — Kafka commits automatically every 1s.
Group B uses `enable_auto_commit=False` — we commit manually after the DB write.

## Partition Assignment

With 3 partitions and 1 consumer in a group, that consumer handles all 3.
With 3 partitions and 3 consumers in the same group, each handles exactly 1.
With 3 partitions and 4 consumers, 1 consumer sits idle (Kafka won't split a partition).

```bash
# Scale Group A to 3 consumers (run in 3 terminals):
python consumers/consumer_group_a.py   # terminal A1 — handles partition 0
python consumers/consumer_group_a.py   # terminal A2 — handles partition 1
python consumers/consumer_group_a.py   # terminal A3 — handles partition 2
```
