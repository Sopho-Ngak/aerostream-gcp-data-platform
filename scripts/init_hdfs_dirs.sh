#!/bin/bash
# Initialize HDFS directories for the pipeline

echo "📁 Creating HDFS directories..."

# Wait for namenode to be ready
echo "Waiting for namenode..."
while ! nc -z namenode 9000; do
  sleep 1
done
echo "✅ Namenode is ready"

# Create directory structure
docker exec namenode hdfs dfs -mkdir -p /aviation/flights/raw
docker exec namenode hdfs dfs -mkdir -p /aviation/flights/processed
docker exec namenode hdfs dfs -mkdir -p /aviation/aggregates/country
docker exec namenode hdfs dfs -mkdir -p /aviation/aggregates/hourly
docker exec namenode hdfs dfs -mkdir -p /aviation/checkpoints
docker exec namenode hdfs dfs -mkdir -p /aviation/metadata

# Set permissions
docker exec namenode hdfs dfs -chmod -R 755 /aviation

# Verify
echo "📊 HDFS directory structure:"
docker exec namenode hdfs dfs -ls -R /aviation

echo "✅ HDFS initialization complete!"