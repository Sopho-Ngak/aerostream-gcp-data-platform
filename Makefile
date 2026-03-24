SHELL := /bin/bash
.PHONY: help setup up down restart logs clean test init-hdfs

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

setup:
	@echo "📁 Creating directory structure..."
	@mkdir -p logs/airflow logs/ingestion logs/spark
	@mkdir -p spark-apps data/raw data/processed
	@mkdir -p config/{airflow,hadoop,spark,prometheus}
	@chmod +x scripts/*.sh
	@echo "✅ Setup complete"

up:
	@echo "🚀 Starting all services..."
	@docker compose up -d
	@echo "✅ Services started"
	@echo "Access URLs:"
	@echo "  Spark Master: http://localhost:8080"
	@echo "  Airflow: http://localhost:8082"
	@echo "  Superset: http://localhost:8088"
	@echo "  Kafka UI: http://localhost:8083"
	@echo "  Hadoop NameNode: http://localhost:9870"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"
	@echo "  Prometheus: http://localhost:9090"

down:
	@echo "🛑 Stopping all services..."
	@docker compose down
	@echo "✅ Services stopped"

restart: down up

logs:
	@docker compose logs -f

clean:
	@echo "🧹 Cleaning up..."
	@docker compose down -v
	@docker system prune -f
	@echo "✅ Cleanup complete"

test:
	@echo "🔍 Running health checks..."
	@python scripts/test_setup.py

init-hdfs:
	@echo "📁 Initializing HDFS..."
	@./scripts/init_hdfs_dirs.sh
	@echo "✅ HDFS initialized"

# Development helpers
dev-shell:
	@docker compose exec ingestion /bin/bash

spark-shell:
	@docker compose exec spark-master /opt/spark/bin/spark-shell

pyspark:
	@docker compose exec spark-master /opt/spark/bin/pyspark

kafka-produce:
	@docker compose exec ingestion python /app/ingestion/producer.py

kafka-consume:
	@docker compose exec spark-master /opt/spark/bin/spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
		/opt/spark/work-dir/src/streaming/consumer.py