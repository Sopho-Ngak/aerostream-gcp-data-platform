SHELL := /bin/bash
.PHONY: help setup up down restart logs clean test airflow-init

help:
	@echo "Available commands:"
	@echo "  make setup      - Initial setup (create directories, set permissions)"
	@echo "  make up         - Start all services"
	@echo "  make down       - Stop all services"
	@echo "  make restart    - Restart all services"
	@echo "  make logs       - View logs from all services"
	@echo "  make clean      - Remove all containers and volumes"
	@echo "  make test       - Run health checks"
	@echo "  make airflow-init - Initialize Airflow 3"

setup:
	@echo "📁 Creating directory structure..."
	@mkdir -p logs/airflow logs/ingestion logs/spark
	@mkdir -p spark-apps data/raw data/processed
	@mkdir -p config/{airflow,spark}
	@mkdir -p dags src scripts
	@chmod +x scripts/*.sh scripts/*.py
	@. scripts/pull_docker_images.sh
	@echo "✅ Setup complete"

up:
	@echo "🚀 Starting all services..."
	@echo "Running command: docker compose up $(args) -d"
	@docker compose up $(args) -d
	@echo "✅ Services started"
	@sleep 10
	@echo ""
	@echo "Access URLs:"
	@echo "  - Spark Master: http://localhost:8080"
	@echo "  - Airflow 3 API Server: http://localhost:8082"
	@echo "  - Kafka UI: http://localhost:8085"
	@echo "  - BigQuery + Looker Studio live in GCP"
	@echo ""
	@echo "Airflow 3 Credentials: admin / admin"

down:
	@echo "🛑 Stopping all services..."
	@echo "Running command: docker compose down $(args)"
	@docker compose down $(args)
	@echo "✅ Services stopped"

restart: down up

logs:
	@echo "📜 Tailing logs from services..."
	@echo "Running command: docker compose logs -f --tail=100 $(args)"
	@if [[ -n "$(args)" ]]; then \
		if ! docker compose config --services | grep -qE '^($(args))$$'; then \
			echo "❌ Error: Service '$(args)' not found."; \
			echo "Available services are:"; \
			docker compose config --services | sed 's/^/  - /'; \
			exit 1; \
		fi; \
	fi
	@docker compose logs -f --tail=100 $(args)
clean:
	@echo "🧹 Cleaning up..."
	@docker compose down -v
	@docker system prune -f
	@rm -rf logs/* data/* 2>/dev/null || true
	@echo "✅ Cleanup complete"

test:
	@echo "🔍 Running health checks..."
	@python scripts/test_setup.py

airflow-init:
	@echo "🚀 Initializing Airflow 3..."
	@docker compose up -d airflow-postgres
	@sleep 5
	@docker compose run --rm airflow-init
	@echo "✅ Airflow 3 initialized"
	@echo "   Access Airflow API Server at http://localhost:8082"
	@echo "   Credentials: admin / admin"

# Development helpers
dev-shell:
	@docker compose exec ingestion /bin/bash

spark-shell:
	@docker compose exec spark-master /opt/spark/bin/spark-shell

pyspark:
	@docker compose exec spark-master /opt/spark/bin/pyspark

kafka-produce:
	@docker compose exec ingestion python /app/ingestion/producer.py

airflow-shell:
	@docker compose exec airflow-api-server /bin/bash

airflow-dags:
	@docker compose exec airflow-api-server airflow dags list

airflow-unpause:
	@docker compose exec airflow-api-server airflow dags unpause aerostream_flight_pipeline_v3

airflow-connections:
	@docker compose exec airflow-api-server airflow connections list