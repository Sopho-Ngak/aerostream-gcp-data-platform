#!/bin/bash
# Airflow 3 entrypoint with proper database initialization

set -e

echo "🚀 Starting Airflow 3..."

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
until pg_isready -h airflow-postgres -U airflow; do
    echo "PostgreSQL is unavailable - sleeping"
    sleep 2
done
echo "✅ PostgreSQL is ready"

# Install FAB provider if not installed
if ! python -c "import airflow.providers.fab" 2>/dev/null; then
    echo "📦 Installing FAB provider..."
    pip install apache-airflow-providers-fab==1.0.0
fi

# Initialize database if needed
if ! airflow db check 2>/dev/null; then
    echo "📦 Initializing database..."
    airflow db migrate
fi

# Create admin user if not exists
if ! airflow users list 2>/dev/null | grep -q admin; then
    echo "👤 Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin || true
fi

# Add connections if they don't exist
if ! airflow connections get spark_default 2>/dev/null; then
    echo "🔌 Adding Spark connection..."
    airflow connections add 'spark_default' \
        --conn-type 'spark' \
        --conn-host 'spark://spark-master' \
        --conn-port '7077' || true
fi

if ! airflow connections get hdfs_default 2>/dev/null; then
    echo "🔌 Adding HDFS connection..."
    airflow connections add 'hdfs_default' \
        --conn-type 'hdfs' \
        --conn-host 'hdfs://namenode' \
        --conn-port '9000' || true
fi

echo "✅ Airflow initialization complete"
echo ""
echo "========================================="
echo "Airflow UI: http://localhost:8082"
echo "Username: admin"
echo "Password: admin"
echo "========================================="

# Execute the command
exec airflow "$@"