#!/bin/bash
# Setup script for Airflow 3

echo "🚀 Setting up Airflow 3 for AeroStream..."

# Set Airflow home
export AIRFLOW_HOME=/opt/airflow

# Create directories
mkdir -p $AIRFLOW_HOME/{dags,logs,plugins,config,data}

# Set permissions
chmod -R 777 $AIRFLOW_HOME/logs

# # Install Airflow 3 with providers
# pip install "apache-airflow==3.0.0" \
#     "apache-airflow-providers-google==10.6.0" \
#     "apache-airflow-providers-apache-spark==4.1.0" \
#     "apache-airflow-providers-postgres==5.7.0"

# Initialize database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Add connections
airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077'

airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{"project_id": "'${GCP_PROJECT_ID}'", "keyfile_path": "/opt/airflow/gcp-credentials.json"}'

# Set variables
airflow variables set GCP_PROJECT_ID ${GCP_PROJECT_ID}
airflow variables set GCS_BUCKET_NAME ${GCS_BUCKET_NAME}
airflow variables set BIGQUERY_DATASET ${BIGQUERY_DATASET}

echo "✅ Airflow 3 setup complete!"