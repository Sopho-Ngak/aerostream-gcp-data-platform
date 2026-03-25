"""
AeroStream Flight Data Pipeline DAG for Airflow 3
Complete pipeline from ingestion to dashboard
"""

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta
import logging
import os

# Default arguments
default_args = {
    'owner': 'aerostream',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'start_date': datetime(2024, 1, 1),
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
    tags=['aerostream', 'flight-data', 'production', 'airflow3'],
    max_active_runs=1,
    concurrency=10,
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
        import subprocess
        import tempfile
        
        execution_date = context['ds']
        tmp_dir = tempfile.mkdtemp()
        
        try:
            # Copy from HDFS to local via docker exec
            subprocess.run([
                'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-copyToLocal',
                f'/aviation/processed/flight_metrics/ingestion_date={execution_date}',
                f'/tmp/{execution_date}'
            ], check=True, capture_output=True)
            
            # Upload to GCS
            gcs_path = f"flight_metrics/ingestion_date={execution_date}"
            subprocess.run([
                'gsutil', '-m', 'cp', '-r',
                f'/tmp/{execution_date}/*',
                f'gs://{GCS_BUCKET}/{gcs_path}/'
            ], check=True, capture_output=True)
            
            return {
                'status': 'success',
                'gcs_path': f'gs://{GCS_BUCKET}/{gcs_path}',
                'execution_date': execution_date,
            }
        except subprocess.CalledProcessError as e:
            logging.error(f"Export failed: {e.stderr}")
            raise
        finally:
            subprocess.run(['rm', '-rf', tmp_dir])
    
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