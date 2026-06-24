#!/usr/bin/env bash
# ============================================================
# Start the Enterprise Data Pipeline Platform
# ============================================================

set -euo pipefail

echo "Starting Enterprise Data Pipeline Platform..."
echo ""

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Check available memory (Docker needs ~6GB)
echo "Starting docker-compose stack..."
docker compose up -d

echo ""
echo "Waiting for Kafka to be ready..."
timeout=120
elapsed=0
while ! docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    sleep 5
    elapsed=$((elapsed + 5))
    if [ $elapsed -ge $timeout ]; then
        echo "ERROR: Kafka did not start in time."
        echo "Check logs: docker compose logs kafka"
        exit 1
    fi
    echo "  Waiting... ($elapsed/${timeout}s)"
done

echo ""
echo "Stack is ready!"
echo ""
echo "=== UI Endpoints ==="
echo "  Kafka UI:             http://localhost:8080"
echo "  OpenSearch Dashboards: http://localhost:5601"
echo "  MinIO Console:        http://localhost:9001  (admin/password123)"
echo "  Grafana:              http://localhost:3000  (admin/admin)"
echo "  Prometheus:           http://localhost:9090"
echo ""
echo "=== API Endpoints ==="
echo "  REST Connector:       http://localhost:8001/docs"
echo "  Webhook Receiver:     http://localhost:8002/docs"
echo "  Lineage API:          http://localhost:8010/docs"
echo ""
echo "=== Next Steps ==="
echo "  1. Register CDC connector:"
echo "     pip install httpx && python connectors/cdc_connector/register_connector.py"
echo ""
echo "  2. Send a test webhook:"
echo "     curl -X POST 'http://localhost:8002/webhook/simulate-alerts?count=5'"
echo ""
echo "  3. Run integration tests:"
echo "     cd tests/integration && pip install -r requirements.txt && pytest -v"
echo ""
echo "  4. Read LEARNING_PATH.md for the structured learning guide"
