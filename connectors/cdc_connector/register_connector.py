"""
CDC Connector Registration Script
===================================
Registers the Debezium PostgreSQL connector with Kafka Connect via REST API.

Run this AFTER docker-compose is up:
  python connectors/cdc_connector/register_connector.py

Or use the curl command at the bottom of this file.

What this demonstrates:
  - Debezium connector configuration for PostgreSQL CDC
  - WAL (Write-Ahead Log) based change data capture
  - Topic naming conventions for CDC events
"""

import json
import sys
import time
import httpx

KAFKA_CONNECT_URL = "http://localhost:8083"

# ── Debezium PostgreSQL Connector Configuration ────────────────────────────────
#
# Key parameters explained:
#
# connector.class         → Uses Debezium's PostgreSQL connector
# database.hostname       → Points to our PostgreSQL container
# database.server.name    → Namespace prefix for Kafka topics (e.g. "financial_db.public.transactions")
# plugin.name: pgoutput   → Uses native PostgreSQL logical replication (no extra PG plugin needed)
# publication.name        → Must match the CREATE PUBLICATION in init.sql
# slot.name               → Must match the replication slot in init.sql
# table.include.list      → Only capture changes from these tables
# snapshot.mode: initial  → Read all existing data first, then stream changes

CONNECTOR_CONFIG = {
    "name": "postgres-financial-cdc",
    "config": {
        "connector.class":                    "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname":                  "postgres",
        "database.port":                      "5432",
        "database.user":                      "pipeline",
        "database.password":                  "pipeline123",
        "database.dbname":                    "financial_db",
        "database.server.name":               "financial_db",
        "plugin.name":                        "pgoutput",
        "publication.name":                   "financial_pub",
        "slot.name":                          "debezium_slot",
        "table.include.list":                 "public.transactions,public.market_prices",
        "snapshot.mode":                      "initial",
        "topic.prefix":                       "cdc",
        "transforms":                         "route",
        "transforms.route.type":              "org.apache.kafka.connect.transforms.ReplaceField$Value",
        "transforms.route.include":           "after,before,op,ts_ms,source",
        "key.converter":                      "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable":       "false",
        "value.converter":                    "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable":     "false",
        # CDC events land in: raw.cdc.transactions
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "3",
    }
}

# The CDC topic for transactions will be:
# cdc.public.transactions  (by Debezium convention: prefix.schema.table)
# We re-route this in stream_processor by consuming from both
# raw.transactions AND raw.cdc.transactions


def wait_for_connect(max_retries: int = 30) -> bool:
    print(f"Waiting for Kafka Connect at {KAFKA_CONNECT_URL} ...")
    for i in range(max_retries):
        try:
            r = httpx.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=5)
            if r.status_code == 200:
                print("Kafka Connect is ready.")
                return True
        except Exception:
            pass
        time.sleep(3)
        print(f"  Retry {i+1}/{max_retries} ...")
    return False


def register_connector(config: dict) -> dict:
    name = config["name"]

    # Check if already registered
    r = httpx.get(f"{KAFKA_CONNECT_URL}/connectors/{name}", timeout=10)
    if r.status_code == 200:
        print(f"Connector '{name}' already exists. Updating config...")
        r = httpx.put(
            f"{KAFKA_CONNECT_URL}/connectors/{name}/config",
            json=config["config"],
            timeout=10,
        )
    else:
        print(f"Registering connector '{name}' ...")
        r = httpx.post(
            f"{KAFKA_CONNECT_URL}/connectors",
            json=config,
            timeout=10,
        )

    r.raise_for_status()
    return r.json()


def check_connector_status(name: str) -> dict:
    r = httpx.get(f"{KAFKA_CONNECT_URL}/connectors/{name}/status", timeout=10)
    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    if not wait_for_connect():
        print("ERROR: Kafka Connect did not become ready in time.")
        sys.exit(1)

    result = register_connector(CONNECTOR_CONFIG)
    print(f"\nConnector registered:\n{json.dumps(result, indent=2)}")

    time.sleep(5)
    status = check_connector_status(CONNECTOR_CONFIG["name"])
    print(f"\nConnector status:\n{json.dumps(status, indent=2)}")

    connector_state = status.get("connector", {}).get("state", "UNKNOWN")
    if connector_state == "RUNNING":
        print("\n✓ CDC connector is RUNNING.")
        print("  CDC events will appear in Kafka topic: cdc.public.transactions")
        print("\n  Test it:")
        print("  1. INSERT INTO transactions (account_id, amount, currency, transaction_type)")
        print("     VALUES ('ACC-TEST', 9999.99, 'USD', 'PAYMENT');")
        print("  2. Check Kafka UI → topic: cdc.public.transactions")
    else:
        print(f"\n⚠ Connector state: {connector_state}. Check Debezium logs.")


# ── Equivalent curl command ────────────────────────────────────────────────────
CURL_EXAMPLE = """
# Register via curl:
curl -X POST http://localhost:8083/connectors \\
  -H "Content-Type: application/json" \\
  -d '{
    "name": "postgres-financial-cdc",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "pipeline",
      "database.password": "pipeline123",
      "database.dbname": "financial_db",
      "database.server.name": "financial_db",
      "plugin.name": "pgoutput",
      "publication.name": "financial_pub",
      "slot.name": "debezium_slot",
      "table.include.list": "public.transactions,public.market_prices",
      "snapshot.mode": "initial",
      "topic.prefix": "cdc",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }'
"""
