# #!/bin/bash
# # Initialize HDFS directories for the pipeline

# echo "📁 Creating HDFS directories..."

# # Wait for namenode to be ready
# echo "Waiting for namenode..."
# while ! nc -z namenode 9000; do
#   sleep 1
# done
# echo "✅ Namenode is ready"

# # Create directory structure
# docker exec namenode hdfs dfs -mkdir -p /aviation/flights/raw
# docker exec namenode hdfs dfs -mkdir -p /aviation/flights/processed
# docker exec namenode hdfs dfs -mkdir -p /aviation/aggregates/country
# docker exec namenode hdfs dfs -mkdir -p /aviation/aggregates/hourly
# docker exec namenode hdfs dfs -mkdir -p /aviation/checkpoints
# docker exec namenode hdfs dfs -mkdir -p /aviation/metadata

# # Set permissions
# docker exec namenode hdfs dfs -chmod -R 755 /aviation

# # Verify
# echo "📊 HDFS directory structure:"
# docker exec namenode hdfs dfs -ls -R /aviation

# echo "✅ HDFS initialization complete!"

#!/bin/bash
set -euo pipefail

# Initialize HDFS directories for project workloads.
# Works both:
# - inside a Hadoop container (preferred for docker-compose startup)
# - from host via docker exec (if HDFS_DFS_CMD is overridden)

echo "📁 Initializing HDFS directories..."

NAMENODE_HOST="${NAMENODE_HOST:-namenode}"
NAMENODE_PORT="${NAMENODE_PORT:-9000}"
HDFS_URI="${HDFS_URI:-hdfs://${NAMENODE_HOST}:${NAMENODE_PORT}}"
HDFS_DFS_CMD="${HDFS_DFS_CMD:-hdfs dfs -fs ${HDFS_URI}}"

echo "Waiting for namenode ${NAMENODE_HOST}:${NAMENODE_PORT}..."
until nc -z "${NAMENODE_HOST}" "${NAMENODE_PORT}"; do
  sleep 2
done
echo "✅ Namenode is ready"

# Ensure common roots exist with expected permissions.
${HDFS_DFS_CMD} -mkdir -p /tmp
${HDFS_DFS_CMD} -chmod 1777 /tmp || true

# Keep legacy and /tmp paths to support existing and non-superuser pipelines.
DIRS=(
  /spark-logs
  /aviation/flights
  /aviation/flights/raw
  /aviation/flights/processed
  /aviation/aggregates/country
  /aviation/aggregates/hourly
  /aviation/checkpoints
  /aviation/metadata
  /aviation/processed/flight_metrics
  /tmp/aviation/flights
  /tmp/aviation/flights/raw
  /tmp/aviation/flights/processed
  /tmp/aviation/aggregates/country
  /tmp/aviation/aggregates/hourly
  /tmp/aviation/checkpoints
  /tmp/aviation/metadata
  /tmp/aviation/processed/flight_metrics
)

echo "Creating directories if missing..."
for d in "${DIRS[@]}"; do
  ${HDFS_DFS_CMD} -mkdir -p "${d}"
done

# Writable for pipeline users; idempotent.
${HDFS_DFS_CMD} -chmod -R 777 /tmp/aviation || true
${HDFS_DFS_CMD} -chmod -R 755 /aviation || true
${HDFS_DFS_CMD} -chmod -R 755 /spark-logs || true

# Verify expected dirs exist in HDFS (fail fast if init didn't target real HDFS).
for d in /tmp/aviation /tmp/aviation/flights /tmp/aviation/processed/flight_metrics /spark-logs; do
  ${HDFS_DFS_CMD} -test -d "${d}"
done

echo "✅ HDFS directories initialized"