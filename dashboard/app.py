"""
Pipeline Dashboard
==================
FastAPI web app that provides a live view of everything happening in
the Phase 1 Kafka pipeline:

  http://localhost:8888

Shows:
  - Live message feed (WebSocket from Kafka)
  - Latest price per symbol (updated in real-time)
  - Consumer group lag for analytics-group and storage-group
  - PostgreSQL record counts (what consumer-b wrote)
  - Partition-to-symbol routing map
  - Manual produce trigger button

Architecture:
  - A background Kafka consumer thread reads from raw-market-data
    and puts messages into an asyncio Queue.
  - Each connected browser gets a WebSocket handler that reads from
    the queue and streams JSON events.
  - REST endpoints poll Kafka AdminClient and PostgreSQL for stats.
"""

import asyncio
import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from threading import Thread
from typing import Any

import psycopg2
import psycopg2.extras
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError, UnknownTopicOrPartitionError

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:29092")
TOPIC         = os.environ.get("TOPIC", "raw-market-data")
POSTGRES_DSN  = os.environ.get(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=market_data user=pipeline password=pipeline123"
)
PORT = int(os.environ.get("PORT", "8888"))

# ── Shared state (updated by background consumer thread) ─────────────────────

latest_prices: dict[str, dict]     = {}          # symbol → latest event
message_history: deque             = deque(maxlen=200)  # last 200 messages
message_queue: asyncio.Queue       = None          # initialised on startup
partition_map: dict[str, int]      = {}           # symbol → partition

# ── Background Kafka consumer (runs in a thread) ─────────────────────────────

def kafka_consumer_thread(loop: asyncio.AbstractEventLoop):
    """
    Reads from Kafka in a background thread and puts events into
    the asyncio queue for WebSocket streaming.
    Uses group_id='dashboard-group' — completely independent of
    analytics-group and storage-group.
    """
    global message_queue

    consumer = None
    for _ in range(30):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id="dashboard-group",
                auto_offset_reset="latest",        # only show live messages
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                key_deserializer=lambda b: b.decode("utf-8") if b else None,
                consumer_timeout_ms=-1,
                session_timeout_ms=30_000,
            )
            print("[dashboard] Kafka consumer connected.")
            break
        except Exception as e:
            print(f"[dashboard] Kafka not ready: {e}. Retrying...")
            time.sleep(5)

    if consumer is None:
        print("[dashboard] Could not connect to Kafka consumer.")
        return

    for msg in consumer:
        event   = msg.value
        symbol  = event.get("entity_id", "UNKNOWN")
        price   = event.get("price", 0)

        # Update latest price and partition mapping
        latest_prices[symbol] = {
            "symbol":    symbol,
            "price":     price,
            "currency":  event.get("currency", "INR"),
            "drift":     event.get("drift_pct", 0),
            "timestamp": event.get("timestamp", ""),
            "partition": msg.partition,
            "offset":    msg.offset,
        }
        partition_map[symbol] = msg.partition

        # Build the message record for history + WebSocket
        record = {
            **event,
            "kafka_partition": msg.partition,
            "kafka_offset":    msg.offset,
            "received_at":     datetime.now().strftime("%H:%M:%S.%f")[:-3],
        }
        message_history.appendleft(record)

        # Push to WebSocket queue (thread-safe via run_coroutine_threadsafe)
        if message_queue is not None:
            asyncio.run_coroutine_threadsafe(
                message_queue.put(record), loop
            )


# ── Kafka stats helpers ───────────────────────────────────────────────────────

def get_topic_info() -> dict:
    """Get partition count and message counts via AdminClient."""
    try:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            client_id="dashboard-admin",
            request_timeout_ms=5000,
        )
        meta = admin.describe_topics([TOPIC])
        admin.close()
        if meta and meta[0].get("partitions"):
            partitions = meta[0]["partitions"]
            return {"partition_count": len(partitions), "healthy": True}
    except Exception as e:
        pass
    return {"partition_count": 3, "healthy": False}


def get_consumer_lag(group_id: str) -> list[dict]:
    """
    Calculate consumer lag = end_offset - committed_offset per partition.
    Lag > 0 means the consumer is behind and catching up.
    """
    results = []
    try:
        # Get committed offsets for the group
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            client_id="dashboard-admin-lag",
            request_timeout_ms=5000,
        )
        try:
            committed = admin.list_consumer_group_offsets(group_id)
        except Exception:
            admin.close()
            return []
        admin.close()

        # Get end offsets (latest produced)
        temp_consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKERS)
        try:
            topic_partitions = [
                tp for tp in committed.keys() if tp.topic == TOPIC
            ]
            if not topic_partitions:
                return []
            end_offsets = temp_consumer.end_offsets(topic_partitions)
        finally:
            temp_consumer.close()

        for tp, offset_meta in committed.items():
            if tp.topic != TOPIC:
                continue
            committed_offset = offset_meta.offset if offset_meta else 0
            end_offset       = end_offsets.get(tp, 0)
            lag              = max(0, end_offset - committed_offset)
            results.append({
                "partition":        tp.partition,
                "committed_offset": committed_offset,
                "end_offset":       end_offset,
                "lag":              lag,
            })

    except Exception as e:
        pass

    return sorted(results, key=lambda x: x["partition"])


def get_all_group_stats() -> list[dict]:
    """Stats for both consumer groups."""
    groups = ["analytics-group", "storage-group", "dashboard-group"]
    stats  = []
    for g in groups:
        partitions = get_consumer_lag(g)
        total_lag  = sum(p["lag"] for p in partitions)
        total_msgs = sum(p["committed_offset"] for p in partitions)
        stats.append({
            "group_id":    g,
            "partitions":  partitions,
            "total_lag":   total_lag,
            "total_msgs":  total_msgs,
            "status":      "running" if total_msgs > 0 else "waiting",
        })
    return stats


# ── PostgreSQL helpers ────────────────────────────────────────────────────────

def get_pg_stats() -> dict:
    try:
        conn   = psycopg2.connect(POSTGRES_DSN)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("""
            SELECT entity_id,
                   COUNT(*) as events,
                   ROUND(MIN(price)::numeric, 2) as min_price,
                   ROUND(MAX(price)::numeric, 2) as max_price,
                   ROUND(AVG(price)::numeric, 2) as avg_price,
                   MAX(stored_at) as last_stored
            FROM market_prices
            GROUP BY entity_id
            ORDER BY entity_id
        """)
        rows = [dict(r) for r in cursor.fetchall()]
        cursor.execute("SELECT COUNT(*) as total FROM market_prices")
        total = cursor.fetchone()["total"]
        cursor.close()
        conn.close()
        for r in rows:
            if r.get("last_stored"):
                r["last_stored"] = str(r["last_stored"])
        return {"rows": rows, "total": total, "connected": True}
    except Exception as e:
        return {"rows": [], "total": 0, "connected": False, "error": str(e)}


# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="Kafka Pipeline Dashboard", version="1.0.0")


@app.on_event("startup")
async def startup():
    global message_queue
    message_queue = asyncio.Queue(maxsize=500)
    loop = asyncio.get_event_loop()
    t = Thread(target=kafka_consumer_thread, args=(loop,), daemon=True)
    t.start()
    print(f"[dashboard] Started. Open http://localhost:{PORT}")


# ── REST API endpoints ────────────────────────────────────────────────────────

@app.get("/api/prices")
def api_prices():
    return {"prices": list(latest_prices.values()), "partition_map": partition_map}


@app.get("/api/groups")
def api_groups():
    return {"groups": get_all_group_stats()}


@app.get("/api/db")
def api_db():
    return get_pg_stats()


@app.get("/api/history")
def api_history(limit: int = 50):
    return {"messages": list(message_history)[:limit]}


@app.get("/api/topic")
def api_topic():
    info = get_topic_info()
    info["topic"] = TOPIC
    info["brokers"] = KAFKA_BROKERS
    return info


@app.post("/api/produce")
def api_produce():
    """Trigger one extra produce cycle (for manual testing from the UI)."""
    import random, uuid
    SYMBOLS = {
        "HDFC.NS": 1650, "RELIANCE.NS": 2900, "TCS.NS": 3700,
        "INFY.NS": 1550, "ICICIBANK.NS": 950, "WIPRO.NS": 450,
        "HCLTECH.NS": 1250, "BAJFINANCE.NS": 7100,
    }
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            acks="all",
            retries=3,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            request_timeout_ms=10_000,
        )
        produced = []
        for symbol, base in SYMBOLS.items():
            drift = random.uniform(-0.008, 0.008)
            event = {
                "event_id":      str(uuid.uuid4()),
                "source_system": "market-data-vendor",
                "entity_id":     symbol,
                "event_type":    "price_update",
                "price":         round(base * (1 + drift), 2),
                "currency":      "INR",
                "timestamp":     datetime.now(timezone.utc).isoformat(),
                "schema_version":"v1.0",
                "drift_pct":     round(drift * 100, 4),
            }
            producer.send(TOPIC, key=symbol, value=event)
            produced.append(symbol)
        producer.flush()
        producer.close()
        return {"produced": len(produced), "symbols": produced}
    except Exception as e:
        return {"error": str(e), "produced": 0}


# ── WebSocket ─────────────────────────────────────────────────────────────────

connected_clients: list[WebSocket] = []

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.append(ws)
    try:
        # Send last 20 historical messages immediately on connect
        history = list(message_history)[:20]
        for msg in reversed(history):
            await ws.send_text(json.dumps({"type": "message", "data": msg}))

        # Stream live messages
        while True:
            try:
                record = await asyncio.wait_for(message_queue.get(), timeout=30.0)
                await ws.send_text(json.dumps({"type": "message", "data": record}))
            except asyncio.TimeoutError:
                # Send a heartbeat to keep the connection alive
                await ws.send_text(json.dumps({"type": "heartbeat"}))
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if ws in connected_clients:
            connected_clients.remove(ws)


# ── Dashboard HTML ────────────────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Kafka Pipeline Dashboard</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg:       #0d1117;
      --surface:  #161b22;
      --border:   #30363d;
      --text:     #e6edf3;
      --muted:    #8b949e;
      --green:    #3fb950;
      --red:      #f85149;
      --yellow:   #d29922;
      --blue:     #58a6ff;
      --purple:   #bc8cff;
      --orange:   #ffa657;
      --cyan:     #39d353;
    }

    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
      font-size: 14px;
      min-height: 100vh;
    }

    header {
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 14px 24px;
      display: flex;
      align-items: center;
      gap: 16px;
      position: sticky;
      top: 0;
      z-index: 100;
    }

    header h1 {
      font-size: 16px;
      font-weight: 600;
      color: var(--text);
      letter-spacing: 0.3px;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      padding: 3px 10px;
      border-radius: 12px;
      font-size: 12px;
      font-weight: 500;
    }

    .badge-green  { background: rgba(63,185,80,.15); color: var(--green); }
    .badge-red    { background: rgba(248,81,73,.15);  color: var(--red); }
    .badge-yellow { background: rgba(210,153,34,.15); color: var(--yellow); }
    .badge-blue   { background: rgba(88,166,255,.15); color: var(--blue); }

    .dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
    .dot.pulse { animation: pulse 1.5s infinite; }

    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50%       { opacity: .3; }
    }

    .header-actions { margin-left: auto; display: flex; gap: 10px; align-items: center; }

    button {
      background: #21262d;
      color: var(--text);
      border: 1px solid var(--border);
      padding: 6px 14px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 13px;
      transition: background .15s;
    }
    button:hover { background: #30363d; }
    button.primary { background: #1f6feb; border-color: #388bfd; color: #fff; }
    button.primary:hover { background: #388bfd; }

    .grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      grid-template-rows: auto auto;
      gap: 16px;
      padding: 20px 24px;
      max-width: 1400px;
      margin: 0 auto;
    }

    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
    }

    .panel {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
    }

    .panel.full-width { grid-column: 1 / -1; }

    .panel-header {
      padding: 12px 16px;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 13px;
      font-weight: 600;
      color: var(--text);
    }

    .panel-header small {
      font-weight: 400;
      color: var(--muted);
      font-size: 11px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
    }

    th {
      text-align: left;
      padding: 8px 16px;
      font-size: 11px;
      font-weight: 600;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: .5px;
      border-bottom: 1px solid var(--border);
    }

    td {
      padding: 8px 16px;
      border-bottom: 1px solid rgba(48,54,61,.5);
      vertical-align: middle;
    }

    tr:last-child td { border-bottom: none; }
    tr:hover td { background: rgba(255,255,255,.02); }

    .price-up   { color: var(--green); }
    .price-down { color: var(--red); }

    .arrow { font-size: 12px; margin-right: 2px; }

    .mono { font-family: 'Cascadia Code', 'JetBrains Mono', 'Fira Code', monospace; }

    .symbol-pill {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 600;
      font-family: monospace;
    }

    .lag-bar-wrap { width: 100%; background: rgba(255,255,255,.05); border-radius: 3px; height: 6px; }
    .lag-bar      { height: 6px; border-radius: 3px; transition: width .4s; }

    /* Message feed */
    #feed {
      height: 360px;
      overflow-y: auto;
      padding: 8px 0;
      scroll-behavior: smooth;
    }

    .feed-row {
      display: flex;
      align-items: baseline;
      gap: 10px;
      padding: 5px 16px;
      border-bottom: 1px solid rgba(48,54,61,.3);
      animation: fadeIn .2s ease;
      font-size: 13px;
    }

    @keyframes fadeIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; } }

    .feed-time { color: var(--muted); font-size: 11px; flex-shrink: 0; font-family: monospace; }

    .feed-meta { color: var(--muted); font-size: 11px; margin-left: auto; flex-shrink: 0; }

    /* Scrollbar */
    ::-webkit-scrollbar       { width: 6px; }
    ::-webkit-scrollbar-track { background: var(--bg); }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

    .counter { font-size: 11px; color: var(--muted); margin-left: 8px; }

    .stat-row { display: flex; justify-content: space-between; padding: 10px 16px; border-bottom: 1px solid rgba(48,54,61,.5); }
    .stat-row:last-child { border-bottom: none; }
    .stat-label { color: var(--muted); }
    .stat-value { font-weight: 600; }

    .empty { text-align: center; color: var(--muted); padding: 32px 16px; font-size: 13px; }

    /* Symbol colors */
    .s0 { background: rgba(88,166,255,.15); color: #58a6ff; }
    .s1 { background: rgba(63,185,80,.15);  color: #3fb950; }
    .s2 { background: rgba(188,140,255,.15);color: #bc8cff; }
    .s3 { background: rgba(255,166,87,.15); color: #ffa657; }
    .s4 { background: rgba(57,211,83,.15);  color: #39d353; }
    .s5 { background: rgba(248,81,73,.15);  color: #f85149; }
    .s6 { background: rgba(210,153,34,.15); color: #d29922; }
    .s7 { background: rgba(121,192,255,.15);color: #79c0ff; }
  </style>
</head>
<body>

<header>
  <span>📡</span>
  <h1>Kafka Pipeline — Phase 1 Dashboard</h1>
  <span id="kafka-badge" class="badge badge-yellow">
    <span class="dot"></span> Connecting ...
  </span>
  <span id="pg-badge" class="badge badge-yellow">
    <span class="dot"></span> PostgreSQL
  </span>
  <span class="badge badge-blue">
    Topic: raw-market-data &nbsp;|&nbsp; 3 partitions
  </span>
  <div class="header-actions">
    <span id="msg-count" class="counter">0 messages</span>
    <button class="primary" onclick="triggerProduce()">⚡ Produce Now</button>
    <button onclick="togglePause()" id="pause-btn">⏸ Pause Feed</button>
  </div>
</header>

<div class="grid">

  <!-- Live Prices -->
  <div class="panel">
    <div class="panel-header">
      📈 Live Prices <small>— updates on each tick</small>
    </div>
    <table>
      <thead>
        <tr>
          <th>Symbol</th>
          <th>Price (INR)</th>
          <th>Change</th>
          <th>Partition</th>
          <th>Offset</th>
        </tr>
      </thead>
      <tbody id="prices-body">
        <tr><td colspan="5" class="empty">Waiting for first message ...</td></tr>
      </tbody>
    </table>
  </div>

  <!-- Consumer Group Lag -->
  <div class="panel">
    <div class="panel-header">
      👥 Consumer Groups <small>— auto-refreshes every 5s</small>
    </div>
    <div id="groups-body">
      <div class="empty">Loading consumer group stats ...</div>
    </div>
  </div>

  <!-- Live Feed -->
  <div class="panel full-width">
    <div class="panel-header">
      🔴 Live Message Feed <small>— streaming via WebSocket</small>
      <span id="feed-status" class="badge badge-yellow" style="font-size:11px">connecting</span>
    </div>
    <div id="feed">
      <div class="empty">Waiting for messages ...</div>
    </div>
  </div>

  <!-- PostgreSQL Data -->
  <div class="panel">
    <div class="panel-header">
      🗄 PostgreSQL Records <small>— written by consumer-b</small>
    </div>
    <table>
      <thead>
        <tr>
          <th>Symbol</th>
          <th>Events</th>
          <th>Min ₹</th>
          <th>Max ₹</th>
          <th>Avg ₹</th>
        </tr>
      </thead>
      <tbody id="db-body">
        <tr><td colspan="5" class="empty">Loading ...</td></tr>
      </tbody>
    </table>
  </div>

  <!-- Partition Map -->
  <div class="panel">
    <div class="panel-header">
      🗂 Partition Routing <small>— which symbols land where</small>
    </div>
    <div id="partition-map-body">
      <div class="empty">Detected after first messages arrive ...</div>
    </div>
  </div>

</div>

<script>
  // ── State ──────────────────────────────────────────────────────────────────

  const prices       = {};
  const symbolColors = {};
  const colorKeys    = ['s0','s1','s2','s3','s4','s5','s6','s7'];
  let   colorIdx     = 0;
  let   msgCount     = 0;
  let   paused       = false;
  let   ws           = null;

  function getColor(symbol) {
    if (!symbolColors[symbol]) {
      symbolColors[symbol] = colorKeys[colorIdx++ % colorKeys.length];
    }
    return symbolColors[symbol];
  }

  // ── WebSocket ─────────────────────────────────────────────────────────────

  function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${proto}://${location.host}/ws`);

    ws.onopen = () => {
      document.getElementById('feed-status').className = 'badge badge-green';
      document.getElementById('feed-status').textContent = '● live';
      document.getElementById('kafka-badge').className = 'badge badge-green';
      document.getElementById('kafka-badge').innerHTML = '<span class="dot pulse"></span> Kafka connected';
    };

    ws.onmessage = (evt) => {
      const pkt = JSON.parse(evt.data);
      if (pkt.type === 'heartbeat') return;
      if (pkt.type === 'message' && !paused) {
        handleMessage(pkt.data);
      }
    };

    ws.onclose = () => {
      document.getElementById('feed-status').className = 'badge badge-red';
      document.getElementById('feed-status').textContent = '✗ disconnected';
      setTimeout(connectWS, 3000);  // auto-reconnect
    };

    ws.onerror = () => ws.close();
  }

  // ── Handle one Kafka message ───────────────────────────────────────────────

  function handleMessage(data) {
    msgCount++;
    document.getElementById('msg-count').textContent = `${msgCount} messages`;

    const symbol = data.entity_id || '?';
    const price  = data.price || 0;

    const prev = prices[symbol];
    prices[symbol] = data;

    updatePricesTable(symbol, data, prev);
    appendFeedRow(data);
    updatePartitionMap(symbol, data.kafka_partition);
  }

  // ── Prices table ──────────────────────────────────────────────────────────

  function updatePricesTable(symbol, data, prev) {
    const tbody   = document.getElementById('prices-body');
    const drift   = data.drift_pct || 0;
    const isUp    = drift >= 0;
    const arrow   = isUp ? '↑' : '↓';
    const cls     = isUp ? 'price-up' : 'price-down';
    const color   = getColor(symbol);

    // Find or create row
    let row = tbody.querySelector(`tr[data-symbol="${symbol}"]`);
    if (!row) {
      // Remove empty placeholder
      const empty = tbody.querySelector('.empty');
      if (empty) empty.parentElement.remove();

      row = tbody.insertRow();
      row.dataset.symbol = symbol;
    }

    row.innerHTML = `
      <td><span class="symbol-pill ${color}">${symbol.replace('.NS','')}</span></td>
      <td class="mono" style="font-size:15px;font-weight:600">
        ₹${data.price.toLocaleString('en-IN', {minimumFractionDigits:2,maximumFractionDigits:2})}
      </td>
      <td class="${cls} mono">
        <span class="arrow">${arrow}</span>${Math.abs(drift).toFixed(3)}%
      </td>
      <td><span class="badge badge-blue" style="font-size:11px">P${data.kafka_partition}</span></td>
      <td class="mono" style="color:var(--muted)">${data.kafka_offset}</td>
    `;

    // Flash on update
    row.style.background = isUp ? 'rgba(63,185,80,.08)' : 'rgba(248,81,73,.08)';
    setTimeout(() => row.style.background = '', 400);
  }

  // ── Feed rows ─────────────────────────────────────────────────────────────

  function appendFeedRow(data) {
    const feed   = document.getElementById('feed');
    const empty  = feed.querySelector('.empty');
    if (empty) feed.innerHTML = '';

    const drift  = data.drift_pct || 0;
    const isUp   = drift >= 0;
    const color  = getColor(data.entity_id || '?');
    const symbol = (data.entity_id || '?').replace('.NS','');

    const row     = document.createElement('div');
    row.className = 'feed-row';
    row.innerHTML = `
      <span class="feed-time">${data.received_at || ''}</span>
      <span class="symbol-pill ${color}" style="font-size:11px">${symbol}</span>
      <span style="color:var(--muted)">price_update</span>
      <span style="font-weight:600">
        ₹${(data.price||0).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2})}
      </span>
      <span class="${isUp?'price-up':'price-down'}">
        ${isUp?'↑':'↓'}${Math.abs(drift).toFixed(3)}%
      </span>
      <span class="feed-meta mono" style="font-size:10px">
        P${data.kafka_partition} · offset=${data.kafka_offset} · ${data.schema_version||'v1.0'}
      </span>
    `;
    feed.insertBefore(row, feed.firstChild);

    // Keep max 100 rows in DOM
    while (feed.children.length > 100) feed.removeChild(feed.lastChild);
  }

  // ── Partition map ─────────────────────────────────────────────────────────

  const partitionGroups = {};

  function updatePartitionMap(symbol, partition) {
    if (partition === undefined || partition === null) return;
    const sym = symbol.replace('.NS','');
    if (!partitionGroups[partition]) partitionGroups[partition] = new Set();
    partitionGroups[partition].add(sym);

    const container = document.getElementById('partition-map-body');
    const empty     = container.querySelector('.empty');
    if (empty) container.innerHTML = '';

    let html = '';
    [0, 1, 2].forEach(p => {
      const syms = partitionGroups[p] || new Set();
      const pills = [...syms].map(s => {
        const c = getColor(s + '.NS');
        return `<span class="symbol-pill ${c}" style="font-size:11px">${s}</span>`;
      }).join(' ');

      html += `
        <div class="stat-row">
          <span class="stat-label">
            <span class="badge badge-blue" style="font-size:11px">Partition ${p}</span>
          </span>
          <span style="display:flex;gap:4px;flex-wrap:wrap">${pills || '<span style="color:var(--muted)">no messages yet</span>'}</span>
        </div>
      `;
    });

    container.innerHTML = html + `
      <div class="stat-row" style="font-size:11px;color:var(--muted)">
        Key routing: hash(entity_id) % 3 — deterministic per symbol
      </div>
    `;
  }

  // ── Consumer groups (polls /api/groups every 5s) ──────────────────────────

  async function refreshGroups() {
    try {
      const res  = await fetch('/api/groups');
      const data = await res.json();
      renderGroups(data.groups || []);
    } catch (e) {}
  }

  function renderGroups(groups) {
    const el = document.getElementById('groups-body');
    if (!groups.length) { el.innerHTML = '<div class="empty">No groups yet</div>'; return; }

    let html = '';
    groups.forEach(g => {
      const statusCls = g.status === 'running' ? 'badge-green' : 'badge-yellow';
      const lagColor  = g.total_lag > 50 ? 'var(--red)' : g.total_lag > 0 ? 'var(--yellow)' : 'var(--green)';

      html += `
        <div style="padding:12px 16px; border-bottom:1px solid var(--border)">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
            <span style="font-weight:600;font-size:13px">${g.group_id}</span>
            <span class="badge ${statusCls}" style="font-size:11px">${g.status}</span>
          </div>
          <div style="display:flex;gap:16px;font-size:12px;margin-bottom:8px">
            <span>Consumed: <strong>${g.total_msgs.toLocaleString()}</strong></span>
            <span>Lag: <strong style="color:${lagColor}">${g.total_lag}</strong></span>
          </div>
          <table style="font-size:11px;width:100%">
            <tr style="color:var(--muted)">
              <td>Partition</td><td>Committed</td><td>End</td><td>Lag</td>
            </tr>
            ${(g.partitions||[]).map(p => `
              <tr>
                <td><span class="badge badge-blue" style="font-size:10px">P${p.partition}</span></td>
                <td class="mono">${p.committed_offset}</td>
                <td class="mono">${p.end_offset}</td>
                <td style="color:${p.lag>0?'var(--yellow)':'var(--green)'}">
                  ${p.lag}
                </td>
              </tr>
            `).join('')}
          </table>
        </div>
      `;
    });
    el.innerHTML = html;
  }

  // ── PostgreSQL (polls /api/db every 8s) ───────────────────────────────────

  async function refreshDB() {
    try {
      const res  = await fetch('/api/db');
      const data = await res.json();

      const pgBadge = document.getElementById('pg-badge');
      if (data.connected) {
        pgBadge.className = 'badge badge-green';
        pgBadge.innerHTML = `<span class="dot"></span> PostgreSQL (${data.total} rows)`;
      } else {
        pgBadge.className = 'badge badge-red';
        pgBadge.innerHTML = '<span class="dot"></span> PostgreSQL — no data yet';
      }

      const tbody = document.getElementById('db-body');
      if (!data.rows || data.rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="5" class="empty">
          No rows yet — consumer-b writes here after collecting 10 messages
        </td></tr>`;
        return;
      }

      tbody.innerHTML = data.rows.map(r => {
        const color = getColor(r.entity_id);
        const sym   = (r.entity_id||'').replace('.NS','');
        return `
          <tr>
            <td><span class="symbol-pill ${color}">${sym}</span></td>
            <td class="mono" style="color:var(--cyan)">${r.events}</td>
            <td class="mono">₹${parseFloat(r.min_price).toLocaleString('en-IN',{minimumFractionDigits:2})}</td>
            <td class="mono">₹${parseFloat(r.max_price).toLocaleString('en-IN',{minimumFractionDigits:2})}</td>
            <td class="mono">₹${parseFloat(r.avg_price).toLocaleString('en-IN',{minimumFractionDigits:2})}</td>
          </tr>
        `;
      }).join('');
    } catch (e) {}
  }

  // ── Produce button ────────────────────────────────────────────────────────

  async function triggerProduce() {
    const btn = document.querySelector('button.primary');
    btn.textContent = '⏳ Producing ...';
    btn.disabled    = true;
    try {
      const res  = await fetch('/api/produce', { method: 'POST' });
      const data = await res.json();
      btn.textContent = `✓ Produced ${data.produced || 0} events`;
      setTimeout(() => {
        btn.textContent = '⚡ Produce Now';
        btn.disabled    = false;
      }, 2000);
    } catch (e) {
      btn.textContent = '⚡ Produce Now';
      btn.disabled    = false;
    }
  }

  // ── Pause toggle ──────────────────────────────────────────────────────────

  function togglePause() {
    paused = !paused;
    document.getElementById('pause-btn').textContent = paused ? '▶ Resume Feed' : '⏸ Pause Feed';
  }

  // ── Boot ──────────────────────────────────────────────────────────────────

  connectWS();
  refreshGroups();
  refreshDB();
  setInterval(refreshGroups, 5000);
  setInterval(refreshDB, 8000);
</script>

</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
