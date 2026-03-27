#!/bin/bash
# Superset initialization script

set -e

echo "🚀 Starting Superset initialization..."

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
for i in {1..30}; do
    if nc -z superset-db 5432 2>/dev/null; then
        echo "✅ PostgreSQL is ready"
        break
    fi
    echo "Waiting for PostgreSQL... ($i/30)"
    sleep 2
done

# Verify psycopg2 is installed
echo "Checking PostgreSQL driver..."
python -c "import psycopg2; print('✅ psycopg2 version:', psycopg2.__version__)" || {
    echo "Installing psycopg2..."
    pip install --no-cache-dir psycopg2-binary
}

# Upgrade database
echo "Running database migrations..."
superset db upgrade || {
    echo "❌ Database migration failed"
    exit 1
}

# Create admin user
echo "Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password admin 2>/dev/null || echo "Admin user already exists"

# Initialize roles and permissions
echo "Initializing Superset..."
superset init || {
    echo "❌ Superset initialization failed"
    exit 1
}

# Load examples (optional)
if [ "${LOAD_EXAMPLES}" = "true" ]; then
    echo "Loading examples..."
    superset load_examples || echo "Failed to load examples"
fi

echo "✅ Superset initialization complete!"

# Start Superset
echo "Starting Superset server..."
exec superset run -p 8088 --with-threads --reload --debugger