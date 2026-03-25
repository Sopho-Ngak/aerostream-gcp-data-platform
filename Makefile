.PHONY: help setup up down restart logs clean test init-hdfs airflow-init

help:
	@echo "Available commands:"
	@echo "  make setup      - Initial setup (create directories, set permissions)"
	@echo "  make up         - Start all services"
	@echo "  make down       - Stop all services"
	@echo "  make restart    - Restart all services"
	@echo "  make logs       - View logs from all services"
	@echo "  make clean      - Remove all containers and volumes"
	@echo "  make test       - Run health checks"
	@echo "  make init-hdfs  - Initialize HDFS directories"
	@echo "  make airflow-init - Initialize Airflow 3"

setup:
	@echo "📁 Creating directory structure..."
	@mkdir -p logs/airflow logs/ingestion logs/spark
	@mkdir -p spark-apps data/raw data/processed
	@mkdir -p config/{airflow,hadoop,spark,prometheus}
	@mkdir -p dags src scripts
	@chmod +x scripts/*.sh scripts/*.py
	@echo "✅ Setup complete"

up:
	@echo "🚀 Starting all services..."
	@docker-compose up -d
	@echo "✅ Services started"
	@sleep 10
	@echo ""
	@echo "Access URLs:"
	@echo "  Spark Master: http://localhost:8080"
	@echo "  Airflow 3 API Server: http://localhost:8082"
	@echo "  Airflow Flower: http://localhost:5555"
	@echo "  Superset: http://localhost:8088"
	@echo "  Kafka UI: http://localhost:8083"
	@echo "  Hadoop NameNode: http://localhost:9870"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"
	@echo "  Prometheus: http://localhost:9090"
	@echo ""
	@echo "Airflow 3 Credentials: admin / admin"

down:
	@echo "🛑 Stopping all services..."
	@docker-compose down
	@echo "✅ Services stopped"

restart: down up

logs:
	@docker-compose logs -f --tail=100

clean:
	@echo "🧹 Cleaning up..."
	@docker-compose down -v
	@docker system prune -f
	@rm -rf logs/* data/* 2>/dev/null || true
	@echo "✅ Cleanup complete"

test:
	@echo "🔍 Running health checks..."
	@python scripts/test_setup.py

init-hdfs:
	@echo "📁 Initializing HDFS..."
	@docker exec namenode bash -c "hdfs dfs -mkdir -p /aviation/flights/raw /aviation/flights/processed /aviation/aggregates /aviation/checkpoints"
	@docker exec namenode bash -c "hdfs dfs -chmod -R 755 /aviation"
	@echo "✅ HDFS initialized"

airflow-init:
	@echo "🚀 Initializing Airflow 3..."
	@docker-compose up -d airflow-postgres redis
	@sleep 5
	@docker-compose run --rm airflow-init
	@echo "✅ Airflow 3 initialized"
	@echo "   Access Airflow API Server at http://localhost:8082"
	@echo "   Credentials: admin / admin"

# Development helpers
dev-shell:
	@docker-compose exec ingestion /bin/bash

spark-shell:
	@docker-compose exec spark-master /opt/spark/bin/spark-shell

pyspark:
	@docker-compose exec spark-master /opt/spark/bin/pyspark

kafka-produce:
	@docker-compose exec ingestion python /app/ingestion/producer.py

airflow-shell:
	@docker-compose exec airflow-api-server /bin/bash

airflow-dags:
	@docker-compose exec airflow-api-server airflow dags list

airflow-unpause:
	@docker-compose exec airflow-api-server airflow dags unpause aerostream_flight_pipeline_v3

airflow-connections:
	@docker-compose exec airflow-api-server airflow connections list