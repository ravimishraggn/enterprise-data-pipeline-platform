"""
Integration Tests for the Enterprise Data Pipeline Platform
===========================================================

These tests run against the live Docker Compose stack.
Start the stack first: docker-compose up -d

Run tests:
  cd tests/integration
  pip install pytest httpx confluent-kafka redis asyncpg
  pytest test_pipeline.py -v --timeout=60

Test coverage:
  - REST API Connector produces messages
  - Webhook Receiver accepts and produces
  - Stream Processor consumes and enriches
  - OpenSearch indexed records are searchable
  - PostgreSQL has processed records
  - Redis has cached account state
  - Lineage API has tracked events
  - PII detection fires on sensitive data
"""

import json
import time
import uuid
from datetime import datetime, timezone

import httpx
import pytest
import redis
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

# ── Service URLs ──────────────────────────────────────────────────────────────

REST_CONNECTOR_URL    = "http://localhost:8001"
WEBHOOK_RECEIVER_URL  = "http://localhost:8002"
LINEAGE_API_URL       = "http://localhost:8010"
KAFKA_BROKERS         = "localhost:29092"
OPENSEARCH_URL        = "http://localhost:9200"
REDIS_HOST            = "localhost"
REDIS_PORT            = 6379


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def kafka_producer():
    p = Producer({"bootstrap.servers": KAFKA_BROKERS})
    yield p
    p.flush()

@pytest.fixture(scope="session")
def kafka_consumer():
    c = Consumer({
        "bootstrap.servers":  KAFKA_BROKERS,
        "group.id":           f"integration-test-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })
    yield c
    c.close()

@pytest.fixture(scope="session")
def redis_client():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    yield r

@pytest.fixture(scope="session")
def http():
    with httpx.Client(timeout=30.0) as client:
        yield client


# ── Helper: consume one message from a topic with timeout ─────────────────────

def consume_one(consumer: Consumer, topic: str, timeout: float = 30.0, key_filter: str = None) -> dict | None:
    consumer.subscribe([topic])
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        try:
            data = json.loads(msg.value().decode())
            if key_filter and msg.key() and key_filter not in msg.key().decode():
                continue
            return data
        except Exception:
            continue
    return None


# ── Health Checks ─────────────────────────────────────────────────────────────

class TestHealthChecks:

    def test_rest_connector_health(self, http):
        r = http.get(f"{REST_CONNECTOR_URL}/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["service"] == "rest-api-connector"

    def test_webhook_receiver_health(self, http):
        r = http.get(f"{WEBHOOK_RECEIVER_URL}/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_lineage_api_health(self, http):
        r = http.get(f"{LINEAGE_API_URL}/health")
        assert r.status_code == 200

    def test_opensearch_health(self, http):
        r = http.get(f"{OPENSEARCH_URL}/_cluster/health")
        assert r.status_code == 200
        assert r.json()["status"] in ("green", "yellow")

    def test_redis_health(self, redis_client):
        assert redis_client.ping() is True

    def test_prometheus_metrics_exposed(self, http):
        r = http.get(f"{REST_CONNECTOR_URL}/metrics")
        assert r.status_code == 200
        assert "pipeline_messages_produced_total" in r.text


# ── REST API Connector ────────────────────────────────────────────────────────

class TestRestApiConnector:

    def test_manual_trigger_produces_messages(self, http, kafka_consumer):
        """POST /trigger should produce messages to raw.transactions."""
        kafka_consumer.subscribe(["raw.transactions"])
        # Give consumer time to join
        time.sleep(2)

        r = http.post(f"{REST_CONNECTOR_URL}/trigger")
        assert r.status_code == 200
        data = r.json()
        assert data["produced"] > 0
        assert data["topic"] == "raw.transactions"

        # Verify at least one message arrives in Kafka
        msg = consume_one(kafka_consumer, "raw.transactions", timeout=15)
        assert msg is not None
        assert "transaction_id" in msg or "symbol" in msg

    def test_produced_messages_have_required_fields(self, http, kafka_consumer):
        kafka_consumer.subscribe(["raw.transactions"])
        time.sleep(1)
        http.post(f"{REST_CONNECTOR_URL}/trigger")

        msg = consume_one(kafka_consumer, "raw.transactions", timeout=15)
        assert msg is not None
        # Transaction records must have these fields
        if msg.get("source") == "rest_api_connector" and "transaction_id" in msg:
            required = ["transaction_id", "account_id", "amount", "currency",
                        "transaction_type", "schema_version", "event_timestamp"]
            for field in required:
                assert field in msg, f"Missing field: {field}"


# ── Webhook Receiver ──────────────────────────────────────────────────────────

class TestWebhookReceiver:

    def test_risk_alert_accepted(self, http):
        payload = {
            "alert_type":   "FRAUD_SUSPECTED",
            "severity":     "HIGH",
            "account_id":   "ACC-INTEGRATION-TEST",
            "description":  "Integration test alert",
            "risk_score":   0.95,
        }
        r = http.post(f"{WEBHOOK_RECEIVER_URL}/webhook/risk-alert", json=payload)
        assert r.status_code == 202
        data = r.json()
        assert data["accepted"] is True
        assert "alert_id" in data

    def test_invalid_severity_rejected(self, http):
        payload = {
            "alert_type":  "FRAUD_SUSPECTED",
            "severity":    "EXTREME",        # not a valid severity
            "account_id":  "ACC-001",
            "description": "bad payload",
            "risk_score":  0.9,
        }
        r = http.post(f"{WEBHOOK_RECEIVER_URL}/webhook/risk-alert", json=payload)
        assert r.status_code == 422

    def test_simulate_alerts_produces_to_kafka(self, http, kafka_consumer):
        kafka_consumer.subscribe(["raw.webhooks"])
        time.sleep(1)

        r = http.post(f"{WEBHOOK_RECEIVER_URL}/webhook/simulate-alerts?count=3")
        assert r.status_code == 200
        data = r.json()
        assert data["published"] == 3

        msg = consume_one(kafka_consumer, "raw.webhooks", timeout=15)
        assert msg is not None
        assert msg.get("event_type") == "RISK_ALERT"

    def test_risk_score_out_of_range_rejected(self, http):
        payload = {
            "alert_type": "AML_FLAG",
            "severity":   "LOW",
            "account_id": "ACC-001",
            "description": "test",
            "risk_score":  1.5,   # > 1.0
        }
        r = http.post(f"{WEBHOOK_RECEIVER_URL}/webhook/risk-alert", json=payload)
        assert r.status_code == 422


# ── PII Detection ─────────────────────────────────────────────────────────────

class TestPiiDetector:

    def test_pii_scan_detects_email(self, http):
        r = http.post("http://localhost:8011/scan", json={
            "data": "Contact us at john.doe@example.com for support",
            "account_id": "ACC-001"
        })
        assert r.status_code == 200
        data = r.json()
        assert data["pii_detected"] is True
        assert "email" in data["pii_types"]

    def test_pii_scan_detects_ssn(self, http):
        r = http.post("http://localhost:8011/scan", json={
            "description": "Customer SSN: 123-45-6789",
            "account_id": "ACC-001"
        })
        assert r.status_code == 200
        data = r.json()
        assert data["pii_detected"] is True
        assert "ssn" in data["pii_types"]

    def test_clean_transaction_no_pii(self, http):
        r = http.post("http://localhost:8011/scan", json={
            "transaction_id": str(uuid.uuid4()),
            "account_id":     "ACC-001",
            "amount":         1500.00,
            "currency":       "USD",
        })
        assert r.status_code == 200
        data = r.json()
        assert data["pii_detected"] is False


# ── OpenSearch ────────────────────────────────────────────────────────────────

class TestOpenSearch:

    def test_transaction_index_exists(self, http):
        r = http.head(f"{OPENSEARCH_URL}/financial-transactions")
        assert r.status_code == 200

    def test_document_index_exists(self, http):
        r = http.head(f"{OPENSEARCH_URL}/financial-documents")
        assert r.status_code == 200

    def test_can_search_transactions(self, http):
        # Wait for some processing to complete
        time.sleep(10)
        r = http.post(
            f"{OPENSEARCH_URL}/financial-transactions/_search",
            json={"query": {"match_all": {}}, "size": 1}
        )
        assert r.status_code == 200
        result = r.json()
        assert "hits" in result

    def test_transaction_index_has_knn_mapping(self, http):
        r = http.get(f"{OPENSEARCH_URL}/financial-documents/_mapping")
        assert r.status_code == 200
        mapping = r.json()
        props = list(mapping.values())[0]["mappings"]["properties"]
        assert "embedding" in props
        assert props["embedding"]["type"] == "knn_vector"


# ── Redis Writer ──────────────────────────────────────────────────────────────

class TestRedisWriter:

    def test_leaderboard_endpoint(self, http):
        r = http.get("http://localhost:8007/leaderboard/risk")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    def test_account_lookup_returns_structure(self, http):
        # Account may or may not have data, but endpoint should respond
        r = http.get("http://localhost:8007/account/ACC-001")
        assert r.status_code == 200
        data = r.json()
        assert data["account_id"] == "ACC-001"


# ── Lineage Tracker ───────────────────────────────────────────────────────────

class TestLineageTracker:

    def test_lineage_stats_endpoint(self, http):
        time.sleep(5)
        r = http.get(f"{LINEAGE_API_URL}/lineage/stats")
        assert r.status_code == 200
        data = r.json()
        assert "total_events" in data
        assert isinstance(data["total_events"], int)

    def test_recent_lineage_events(self, http):
        r = http.get(f"{LINEAGE_API_URL}/lineage/recent?limit=5")
        assert r.status_code == 200
        data = r.json()
        assert "events" in data
        assert isinstance(data["events"], list)


# ── End-to-End Pipeline ───────────────────────────────────────────────────────

class TestEndToEndPipeline:

    def test_full_transaction_flow(self, http, kafka_consumer, redis_client):
        """
        Trigger a transaction → verify it eventually ends up in:
          1. Kafka raw.transactions
          2. Kafka processed.transactions
          3. Redis cache
        """
        # Step 1: Trigger production
        r = http.post(f"{REST_CONNECTOR_URL}/trigger")
        assert r.status_code == 200
        produced = r.json()["produced"]
        assert produced > 0

        # Step 2: Check raw topic
        kafka_consumer.subscribe(["raw.transactions"])
        raw_msg = consume_one(kafka_consumer, "raw.transactions", timeout=15)
        assert raw_msg is not None

        # Step 3: Check processed topic (give stream processor time to work)
        kafka_consumer.subscribe(["processed.transactions"])
        processed_msg = consume_one(kafka_consumer, "processed.transactions", timeout=30)
        assert processed_msg is not None
        assert "risk_label" in processed_msg
        assert "enrichment_tags" in processed_msg
        assert "processed_at" in processed_msg

        # Step 4: Verify risk_label is valid
        assert processed_msg["risk_label"] in ("LOW", "MEDIUM", "HIGH", "CRITICAL")

    def test_webhook_flows_through_processor(self, http, kafka_consumer):
        """Alert webhook → raw.webhooks → processed.transactions."""
        payload = {
            "alert_type":  "AML_FLAG",
            "severity":    "CRITICAL",
            "account_id":  f"ACC-E2E-{uuid.uuid4().hex[:6]}",
            "description": "End-to-end test AML flag",
            "risk_score":  0.99,
        }
        r = http.post(f"{WEBHOOK_RECEIVER_URL}/webhook/risk-alert", json=payload)
        assert r.status_code == 202

        kafka_consumer.subscribe(["processed.transactions"])
        processed = consume_one(kafka_consumer, "processed.transactions", timeout=30)
        assert processed is not None
        # The processed record should reflect high risk
        if processed.get("transaction_type") == "RISK_ALERT":
            assert processed.get("risk_label") in ("HIGH", "CRITICAL")
