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

# Default arguments
default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
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
    
    # 2. Run Spark batch processing on HDFS data
    process_flight_batch = SparkSubmitOperator(
        task_id='process_flight_batch',
        application='/opt/airflow/src/batch_processing/flight_transformations.py',
        name='flight-batch-processing-{{ ds }}',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '2g',
            'spark.pyspark.python': 'python3.11',
            'spark.pyspark.driver.python': 'python3.11',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        },
        application_args=[
            '--processing_date', '{{ ds }}',
        ],
        verbose=True,
    )
    
    # 3. Export to GCS
    @task(
        task_id='export_to_gcs',
        retries=2,
        retry_delay=timedelta(minutes=2)
    )
    def export_to_gcs(**context):
        """Export processed data from HDFS to GCS"""
        import requests
        from google.cloud import storage

        execution_date = context['ds']
        hdfs_dir = f"/tmp/aviation/processed/flight_metrics/ingestion_date={execution_date}"
        gcs_path = f"flight_metrics/ingestion_date={execution_date}"
        webhdfs_base = "http://namenode:9870/webhdfs/v1"
        list_url = f"{webhdfs_base}{hdfs_dir}"

        try:
            # List files from HDFS using WebHDFS.
            logging.info("Listing files in HDFS path: %s", hdfs_dir)
            list_resp = requests.get(
                list_url,
                params={"op": "LISTSTATUS", "user.name": "root"},
                timeout=30,
            )
            list_resp.raise_for_status()
            statuses = list_resp.json()["FileStatuses"]["FileStatus"]
            parquet_files = [s["pathSuffix"] for s in statuses if s["type"] == "FILE" and s["pathSuffix"].endswith(".parquet")]

            if not parquet_files:
                raise ValueError(f"No parquet files found in HDFS path: {hdfs_dir}")

            storage_client = storage.Client(project=GCP_PROJECT_ID)
            bucket = storage_client.bucket(GCS_BUCKET)

            uploaded = 0
            for parquet_file in parquet_files:
                hdfs_file_path = f"{hdfs_dir}/{parquet_file}"
                open_url = f"{webhdfs_base}{hdfs_file_path}"
                logging.info("Uploading file from HDFS path: %s", hdfs_file_path)
                file_resp = requests.get(
                    open_url,
                    params={"op": "OPEN", "user.name": "root"},
                    timeout=120,
                    stream=True,
                )
                file_resp.raise_for_status()

                blob = bucket.blob(f"{gcs_path}/{parquet_file}")
                blob.upload_from_file(file_resp.raw, rewind=False)
                uploaded += 1

            logging.info("Uploaded %s parquet file(s) to gs://%s/%s", uploaded, GCS_BUCKET, gcs_path)
            return {
                'status': 'success',
                'gcs_path': f'gs://{GCS_BUCKET}/{gcs_path}',
                'execution_date': execution_date,
                'uploaded_files': uploaded,
            }
        except Exception as e:
            logging.error("Export failed: %s", e)
            raise
    
    # 4. Load to BigQuery
    @task(task_id='load_to_bigquery')
    def load_to_bigquery(**context):
        """Load files from GCS to BigQuery"""
        from google.cloud import bigquery
        
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field='ingestion_date'
            ),
        )
        
        # Load all files
        uri = f"gs://{GCS_BUCKET}/flight_metrics/ingestion_date={{ ds }}/*.parquet"
        table_ref = client.dataset(BQ_DATASET).table('flight_metrics')
        
        load_job = client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config
        )
        
        result = load_job.result()  # Wait for completion
        
        return {
            'rows_loaded': result.output_rows,
            'bytes_processed': result.total_bytes_processed,
            'table': f'{GCP_PROJECT_ID}.{BQ_DATASET}.flight_metrics'
        }
    
    # 5. Create BigQuery analytics views
    create_analytics_views = BigQueryInsertJobOperator(
        task_id='create_analytics_views',
        configuration={
            'query': {
                'query': """
                    CREATE OR REPLACE VIEW `{{ var.value.GCP_PROJECT_ID }}.{{ var.value.BIGQUERY_DATASET }}.country_summary` AS
                    SELECT 
                        origin_country,
                        DATE(ingestion_date) as flight_date,
                        COUNT(DISTINCT icao24) as unique_aircraft,
                        COUNT(*) as total_flights,
                        AVG(altitude_km) as avg_altitude_km,
                        AVG(speed_kmh) as avg_speed_kmh
                    FROM `{{ var.value.GCP_PROJECT_ID }}.{{ var.value.BIGQUERY_DATASET }}.flight_metrics`
                    WHERE ingestion_date = '{{ ds }}'
                    GROUP BY origin_country, flight_date;
                """,
                'useLegacySql': False
            }
        },
        location='US',
    )
    
    # 6. Data quality check
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
    
    # 7. Send success notification
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
    gcs_export = export_to_gcs()
    bq_load = load_to_bigquery()
    quality_check = data_quality_check()
    
    kafka_check >> process_flight_batch >> gcs_export >> bq_load >> create_analytics_views >> quality_check >> send_success()

# Create the DAG
dag = aerostream_pipeline()