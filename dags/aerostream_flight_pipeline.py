from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.sdk import dag, task

from datetime import datetime, timedelta
import logging
import os
import time

# Default arguments
default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=0.5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'on_skipped_callback': another_function, #or list of functions
    # 'trigger_rule': 'all_success'
}

# Environment variables
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'aerostream-project')
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME', 'aerostream-data-lake')
BQ_DATASET = os.getenv('BIGQUERY_DATASET', 'aviation')

# Define the main DAG using Airflow 3 syntax
@dag(
    dag_id='aerostream_flight_pipeline_v3',
    default_args=default_args,
    description='Complete flight data pipeline from ingestion to dashboard (Airflow 3)',
    schedule='0 */2 * * *',  # Every 2 hours
    catchup=False,
    start_date=datetime(2021, 1, 1),
    tags=['aerostream', 'flight-data', 'production', 'airflow3'],
    max_active_runs=1,
)
def aerostream_pipeline():
    """
    AeroStream Flight Data Pipeline
    Orchestrates the entire flight data processing workflow
    """
    
    # 1. Check if Kafka stream is active
    @task(task_id='check_kafka_stream', retries=3, retry_delay=timedelta(minutes=1))
    def check_kafka_stream():
        """Check if Kafka stream is active"""
        import socket
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex(('kafka', 9092))
            sock.close()
            
            if result == 0:
                logging.info("✅ Kafka is running")
                return {"status": "success", "message": "Kafka is running"}
            else:
                raise Exception("❌ Kafka is not running")
        except Exception as e:
            raise Exception(f"❌ Failed to connect to Kafka: {e}")

    @task(task_id='wait_for_raw_flight_data', retries=0)
    def wait_for_raw_flight_data():
        """Wait for streaming parquet files to appear in GCS before batch processing."""
        from google.cloud import storage

        gcs_prefix = "aviation/flights/raw/"
        poll_seconds = int(os.getenv("RAW_DATA_POLL_SECONDS", "30"))
        max_attempts = int(os.getenv("RAW_DATA_MAX_ATTEMPTS", "20"))

        storage_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET)

        for attempt in range(1, max_attempts + 1):
            blobs = list(bucket.list_blobs(prefix=gcs_prefix, max_results=10))
            parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]

            if parquet_blobs:
                logging.info("Found %s raw parquet file(s) in GCS; continuing.", len(parquet_blobs))
                return {"status": "ready", "raw_parquet_files": len(parquet_blobs)}

            logging.info(
                "No raw parquet files found yet (attempt %s/%s). Sleeping %ss.",
                attempt,
                max_attempts,
                poll_seconds,
            )
            time.sleep(poll_seconds)

        raise ValueError(
            "No raw parquet files found in GCS after waiting. "
            "Ensure ingestion and streaming services are running and writing to GCS."
        )
    
    # 2. Run Spark batch processing — reads GCS raw, writes GCS processed
    process_flight_batch = SparkSubmitOperator(
        task_id='process_flight_batch',
        application='/opt/airflow/src/batch_processing/flight_transformations.py',
        name='flight-batch-processing-{{ ds }}',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '1g',
            'spark.driver.memory': '1g',
            'spark.pyspark.python': 'python3.11',
            'spark.pyspark.driver.python': 'python3.11',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.jars.packages': 'com.google.cloud.bigdataoss:gcs-connector:4.0.4',
            'spark.jars.ivy': '/tmp/.ivy2',
            'spark.hadoop.fs.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
            'spark.hadoop.fs.AbstractFileSystem.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS',
            'spark.hadoop.google.cloud.auth.service.account.enable': 'true',
            'spark.hadoop.google.cloud.auth.service.account.json.keyfile': '/opt/airflow/config/gcp/gcp-credentials.json',
        },
        application_args=[
            '--processing_date', '{{ ds }}',
        ],
        verbose=True,
    )
    
    # 3. Load to BigQuery
    @task(task_id='load_to_bigquery')
    def load_to_bigquery(**context):
        """Load files from GCS to BigQuery"""
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound
        
        client = bigquery.Client(project=GCP_PROJECT_ID)
        execution_date = context['ds']

        # Ensure dataset exists before loading tables.
        dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET)
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = os.getenv('BIGQUERY_LOCATION', 'US')
            client.create_dataset(dataset)
            logging.info("Created missing dataset: %s.%s", GCP_PROJECT_ID, BQ_DATASET)
        
        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
            ),
        )
        
        # Load all files
        uri = f"gs://{GCS_BUCKET}/flight_metrics/ingestion_date={execution_date}/*.parquet"
        table_ref = client.dataset(BQ_DATASET).table('flight_metrics')
        
        load_job = client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config
        )
        
        result = load_job.result()  # Wait for completion
        
        return {
            'rows_loaded': result.output_rows,
            'input_file_bytes': getattr(result, 'input_file_bytes', None),
            'input_files': getattr(result, 'input_files', None),
            'job_id': load_job.job_id,
            'table': f'{GCP_PROJECT_ID}.{BQ_DATASET}.flight_metrics'
        }
    
    # 4. Create BigQuery analytics views
    create_analytics_views = BigQueryInsertJobOperator(
        task_id='create_analytics_views',
        configuration={
            'query': {
                'query': f"""
                    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{BQ_DATASET}.country_summary` AS
                    SELECT 
                        origin_country,
                        DATE(ingestion_date) as flight_date,
                        COUNT(DISTINCT icao24) as unique_aircraft,
                        COUNT(*) as total_flights,
                        AVG(altitude_km) as avg_altitude_km,
                        AVG(speed_kmh) as avg_speed_kmh
                    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.flight_metrics`
                    WHERE ingestion_date = '{{{{ ds }}}}'
                    GROUP BY origin_country, flight_date;
                """,
                'useLegacySql': False
            }
        },
        location='US',
    )
    
    # 5. Data quality check
    @task(task_id='data_quality_check')
    def data_quality_check(**context):
        """Basic data quality check"""
        from google.cloud import bigquery
        
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        query = f"""
            SELECT COUNT(*) as row_count
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.flight_metrics`
            WHERE ingestion_date = '{context['ds']}'
        """
        
        result = client.query(query).result()
        row = next(result)
        
        if row.row_count == 0:
            raise ValueError(f"No data found for date {context['ds']}")
        
        logging.info(f"✅ Data quality check passed: {row.row_count} rows loaded")
        return {"row_count": row.row_count}
    
    # 6. Send success notification
    @task(
        task_id='send_notification',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    def send_success(**context):
        """Send success notification"""
        execution_date = context['ds']
        logging.info(f"✅ Pipeline completed successfully for {execution_date}")
        return {'status': 'success', 'execution_date': execution_date}
    
    # Build the DAG
    kafka_check = check_kafka_stream()
    raw_data_ready = wait_for_raw_flight_data()
    bq_load = load_to_bigquery()
    quality_check = data_quality_check()
    
    kafka_check >> raw_data_ready >> process_flight_batch >> bq_load >> create_analytics_views >> quality_check >> send_success()

# Create the DAG
dag = aerostream_pipeline()