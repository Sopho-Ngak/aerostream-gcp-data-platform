#!/bin/bash
# Simplified Airflow 3 initialization

set -e

echo "🚀 Initializing Airflow 3 with FAB provider..."

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
until pg_isready -h airflow-postgres -U airflow; do
  sleep 2
done

# Initialize database
echo "Initializing database..."
airflow db migrate

# Create admin user using FAB
echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Add connections
echo "Adding Spark connection..."
airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077'

echo "Adding HDFS connection..."
airflow connections add 'hdfs_default' \
    --conn-type 'hdfs' \
    --conn-host 'hdfs://namenode' \
    --conn-port '9000'

echo "✅ Airflow 3 initialization complete!"
echo ""
echo "========================================="
echo "Airflow UI: http://localhost:8082"
echo "Username: admin"
echo "Password: admin"
echo "========================================="