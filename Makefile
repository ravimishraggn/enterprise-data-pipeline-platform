# ============================================================
# Enterprise Data Pipeline Platform — Phase 1 Makefile
# ============================================================
# QUICK START (everything in Docker, no extra terminals needed):
#   make start      → docker compose up -d (starts ALL services)
#   make dashboard  → open http://localhost:8888  (live UI)
#   make ui         → open Kafdrop + dashboard in browser
#
# Individual commands (for local dev without Docker):
#   make produce    Run the market data producer locally
#   make consume-a  Run Consumer Group A (analytics)
#   make consume-b  Run Consumer Group B (storage → PostgreSQL)
#
# Observability:
#   make logs       Tail logs from all containers
#   make kafka-logs Tail Kafka broker logs only
#   make topics     List all Kafka topics
#   make offsets    Show consumer group offsets (lag)
#   make pg-check   Query PostgreSQL to verify Consumer B stored data
#
# Stack management:
#   make stop       Stop the stack (keep data volumes)
#   make clean      Stop + remove all volumes (fresh start)
#   make install    Install Python dependencies for local dev
#   make status     Show status of all containers
# ============================================================

.PHONY: start stop clean produce consume-a consume-b ui dashboard logs \
        kafka-logs topics offsets pg-check install status

# ── Stack management ───────────────────────────────────────────────────────────

start:
	@echo "Starting full pipeline stack (infrastructure + producer + consumers + dashboard) ..."
	docker compose up -d
	@echo ""
	@echo "Waiting for Kafka to be ready (may take ~30s) ..."
	@until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 \
		> /dev/null 2>&1; do sleep 2; printf "."; done
	@echo ""
	@echo ""
	@echo "Stack is ready! All services are running:"
	@echo "  Dashboard  → http://localhost:8888  (live prices, consumer lag, PG records)"
	@echo "  Kafdrop    → http://localhost:9000  (browse topics, partitions, messages)"
	@echo "  PostgreSQL → localhost:5432"
	@echo ""
	@echo "Run 'make ui' to open both UIs, or 'make logs' to watch all output."

stop:
	@echo "Stopping stack (data volumes preserved) ..."
	docker compose stop

clean:
	@echo "Stopping and removing all volumes (fresh start) ..."
	docker compose down -v
	@echo "Clean."

status:
	docker compose ps

# ── Python environment ─────────────────────────────────────────────────────────

install:
	@echo "Installing Python dependencies ..."
	pip install kafka-python==2.0.2 psycopg2-binary==2.9.9
	@echo "Done."

# ── Producer ──────────────────────────────────────────────────────────────────

produce:
	@echo "Starting market data producer (Ctrl-C to stop) ..."
	python connectors/rest_api_connector/producer.py

# ── Consumers ─────────────────────────────────────────────────────────────────

consume-a:
	@echo "Starting Consumer Group A — analytics (Ctrl-C to stop) ..."
	python consumers/consumer_group_a.py

consume-b:
	@echo "Starting Consumer Group B — storage/PostgreSQL (Ctrl-C to stop) ..."
	python consumers/consumer_group_b.py

# ── Browser UI ────────────────────────────────────────────────────────────────

dashboard:
	@echo "Opening Pipeline Dashboard at http://localhost:8888 ..."
	start http://localhost:8888 2>/dev/null || open http://localhost:8888 2>/dev/null || \
		xdg-open http://localhost:8888 2>/dev/null || \
		echo "  Open http://localhost:8888 in your browser."

ui:
# Opens both UIs: live dashboard + Kafdrop topic browser
	@echo "Opening Dashboard → http://localhost:8888"
	@echo "Opening Kafdrop  → http://localhost:9000"
	start http://localhost:8888 2>/dev/null || open http://localhost:8888 2>/dev/null || \
		xdg-open http://localhost:8888 2>/dev/null || true
	start http://localhost:9000 2>/dev/null || open http://localhost:9000 2>/dev/null || \
		xdg-open http://localhost:9000 2>/dev/null || \
		echo "  Open http://localhost:8888 and http://localhost:9000 in your browser."

# ── Observability ─────────────────────────────────────────────────────────────

logs:
	docker compose logs -f

kafka-logs:
	docker compose logs -f kafka

topics:
	@echo "Kafka topics:"
	docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

offsets:
	@echo "Consumer group offsets (lag visible here):"
	docker exec kafka kafka-consumer-groups \
		--bootstrap-server localhost:9092 \
		--describe --all-groups 2>/dev/null || \
		echo "No consumer groups yet. Run 'make consume-a' or 'make consume-b' first."

# ── PostgreSQL verification ────────────────────────────────────────────────────

pg-check:
	@echo "Records stored by Consumer Group B:"
	docker exec postgres psql -U pipeline -d market_data -c \
		"SELECT entity_id, COUNT(*) as events, \
		        ROUND(MIN(price)::numeric, 2) as min_price, \
		        ROUND(MAX(price)::numeric, 2) as max_price, \
		        MAX(stored_at) as last_stored \
		 FROM market_prices \
		 GROUP BY entity_id \
		 ORDER BY entity_id;"
