from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.sdk import dag, task

from datetime import datetime, timedelta
import logging
import os
import subprocess
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
RAW_BQ_TABLE = os.getenv('BIGQUERY_RAW_TABLE', 'flight_stream_raw')
TRANSFORMED_BQ_TABLE = os.getenv('BIGQUERY_TRANSFORMED_TABLE', 'flight_metrics')
DATA_QUALITY_DAG_ID = os.getenv('DATA_QUALITY_DAG_ID', 'aerostream_data_quality')

# Define the main DAG using Airflow 3 syntax
@dag(
    dag_id='aerostream_flight_pipeline_v3',
    default_args=default_args,
    description='Complete flight data pipeline from ingestion to dashboard (Airflow 3)',
    schedule='*/2 * * * *',  # Every 2 minutes for near-real-time processing
    catchup=False,
    start_date=datetime(2021, 1, 1),
    tags=['aerostream', 'flight-data', 'production', 'airflow3'],
    max_active_runs=1,
)
def aerostream_pipeline():
    def resolve_execution_date(context):
        ds = context.get('ds')
        if ds:
            return ds

        logical_date = context.get('logical_date')
        if logical_date:
            return logical_date.date().isoformat()

        return datetime.utcnow().date().isoformat()

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
        scan_limit = int(os.getenv("RAW_DATA_SCAN_LIMIT", "20000"))

        storage_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET)

        for attempt in range(1, max_attempts + 1):
            parquet_count = 0
            scanned_count = 0
            latest_blob_name = None
            latest_blob_updated = None
            for blob in bucket.list_blobs(prefix=gcs_prefix, page_size=200):
                scanned_count += 1
                if blob.name.endswith(".parquet"):
                    if getattr(blob, 'size', 0) == 0:
                        continue
                    parquet_count += 1
                    if latest_blob_updated is None or blob.updated > latest_blob_updated:
                        latest_blob_updated = blob.updated
                        latest_blob_name = blob.name
                if scanned_count >= scan_limit:
                    break

            if parquet_count > 0:
                latest_blob_path = f"gs://{GCS_BUCKET}/{latest_blob_name}"
                logging.info(
                    "Found %s parquet file(s) after scanning %s object(s); newest=%s",
                    parquet_count,
                    scanned_count,
                    latest_blob_path,
                )
                return {
                    "status": "ready",
                    "raw_parquet_files": parquet_count,
                    "scanned_objects": scanned_count,
                    "latest_blob_path": latest_blob_path,
                }

            logging.info(
                "No raw parquet files found yet (attempt %s/%s, scanned=%s). Sleeping %ss.",
                attempt,
                max_attempts,
                scanned_count,
                poll_seconds,
            )
            time.sleep(poll_seconds)

        raise ValueError(
            "No raw parquet files found in GCS after waiting. "
            "Ensure ingestion and streaming services are running and writing to GCS."
        )
    
    # 2. Run Spark batch processing — reads GCS raw, writes GCS processed
    @task(task_id='process_flight_batch')
    def process_flight_batch_task(raw_data_info, **context):
        """Call Spark batch processing with subprocess"""
        import subprocess
        
        execution_date = resolve_execution_date(context)
        latest_blob_path = raw_data_info.get('latest_blob_path') if isinstance(raw_data_info, dict) else None
        logging.info("Starting Spark batch processing for date: %s", execution_date)
        if latest_blob_path:
            logging.info("Incremental mode enabled: processing newest raw parquet only: %s", latest_blob_path)
        
        gcs_jar = '/opt/airflow/include/gcs-connector-hadoop3-latest.jar'
        cmd = [
            'spark-submit',
            '--master', 'spark://spark-master:7077',
            '--jars', gcs_jar,
            '--conf', 'spark.cores.max=1',
            '--conf', 'spark.executor.memory=512m',
            '--conf', 'spark.driver.memory=512m',
            '--conf', 'spark.pyspark.python=python3.11',
            '--conf', 'spark.pyspark.driver.python=python3.11',
            '--conf', 'spark.sql.adaptive.enabled=true',
            '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
            '--conf', 'spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
            '--conf', 'spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS',
            '--conf', 'spark.hadoop.google.cloud.auth.service.account.enable=true',
            '/opt/airflow/src/batch_processing/flight_transformations.py',
            '--processing_date', execution_date,
        ]

        if latest_blob_path:
            cmd.extend(['--input_path', latest_blob_path])
        
        result = subprocess.run(cmd, capture_output=False, text=True, check=True)
        logging.info("Spark batch processing completed successfully")
        return {"status": "success", "processing_date": execution_date}
    
    # 3. Load raw streaming parquet to BigQuery (all incoming columns)
    @task(task_id='load_raw_stream_to_bigquery')
    def load_raw_stream_to_bigquery(raw_data_info):
        """Load new raw streaming parquet files from GCS to BigQuery."""
        from google.cloud import bigquery
        from google.cloud import storage
        from google.api_core.exceptions import NotFound

        if not isinstance(raw_data_info, dict):
            raise ValueError('Missing raw_data_info payload from wait_for_raw_flight_data task')

        latest_blob_path = raw_data_info.get('latest_blob_path')
        if not latest_blob_path:
            raise ValueError('latest_blob_path not found in raw_data_info payload')

        watermark_key = 'raw_bq_last_loaded_blob_updated'
        last_loaded_ts_raw = Variable.get(watermark_key, default_var='1970-01-01T00:00:00+00:00')
        last_loaded_ts = datetime.fromisoformat(last_loaded_ts_raw.replace('Z', '+00:00'))

        storage_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET)
        new_uris = []
        newest_seen_ts = last_loaded_ts

        for blob in bucket.list_blobs(prefix='aviation/flights/raw/', page_size=500):
            if not blob.name.endswith('.parquet'):
                continue
            if getattr(blob, 'size', 0) == 0:
                continue
            if blob.updated and blob.updated > last_loaded_ts:
                new_uris.append(f'gs://{GCS_BUCKET}/{blob.name}')
                if blob.updated > newest_seen_ts:
                    newest_seen_ts = blob.updated

        if not new_uris:
            logging.info('No new raw parquet files to load since %s', last_loaded_ts.isoformat())
            return {
                'rows_loaded': 0,
                'job_id': None,
                'source_uris': [],
                'table': f'{GCP_PROJECT_ID}.{BQ_DATASET}.{RAW_BQ_TABLE}',
            }

        client = bigquery.Client(project=GCP_PROJECT_ID)

        dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET)
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = os.getenv('BIGQUERY_LOCATION', 'US')
            client.create_dataset(dataset)
            logging.info("Created missing dataset: %s.%s", GCP_PROJECT_ID, BQ_DATASET)

        table_ref = client.dataset(BQ_DATASET).table(RAW_BQ_TABLE)
        staging_table_name = f"{RAW_BQ_TABLE}_staging"
        staging_table_ref = client.dataset(BQ_DATASET).table(staging_table_name)

        staging_load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            autodetect=True,
        )

        load_job = client.load_table_from_uri(new_uris, staging_table_ref, job_config=staging_load_config)
        result = load_job.result()

        staging_table = client.get_table(staging_table_ref)
        staging_columns = {field.name for field in staging_table.schema}

        if 'icao24' not in staging_columns:
            raise ValueError('Raw staging table does not contain required column: icao24')

        order_terms = []
        if 'last_contact' in staging_columns:
            order_terms.append('COALESCE(SAFE_CAST(last_contact AS INT64), 0) DESC')
        if 'time_position' in staging_columns:
            order_terms.append('COALESCE(SAFE_CAST(time_position AS INT64), 0) DESC')
        order_terms.append('icao24 DESC')
        order_clause = ', '.join(order_terms)

        try:
            target_table = client.get_table(table_ref)
            target_columns = [field.name for field in target_table.schema]
            common_columns = [col_name for col_name in target_columns if col_name in staging_columns]

            if 'icao24' not in common_columns:
                raise ValueError('Raw target table does not contain required column: icao24')

            update_columns = [col_name for col_name in common_columns if col_name != 'icao24']
            update_set_clause = ',\n                    '.join(
                [f"{col_name} = S.{col_name}" for col_name in update_columns]
            )
            when_matched_clause = ""
            if update_set_clause:
                when_matched_clause = f"""
                WHEN MATCHED THEN
                  UPDATE SET
                    {update_set_clause}
                """
            insert_columns = ', '.join(common_columns)
            insert_values = ', '.join([f"S.{col_name}" for col_name in common_columns])
            projection = ',\n                        '.join(common_columns)

            merge_query = f"""
                MERGE `{GCP_PROJECT_ID}.{BQ_DATASET}.{RAW_BQ_TABLE}` T
                USING (
                    SELECT
                        {projection}
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY icao24
                                ORDER BY {order_clause}
                            ) AS rn
                        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{staging_table_name}`
                        WHERE icao24 IS NOT NULL AND TRIM(icao24) != ''
                    )
                    WHERE rn = 1
                ) S
                ON T.icao24 = S.icao24
                {when_matched_clause}
                WHEN NOT MATCHED THEN
                  INSERT ({insert_columns})
                  VALUES ({insert_values})
            """

            merge_job = client.query(merge_query)
            merge_job.result()
        except NotFound:
            create_from_staging_query = f"""
                CREATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{RAW_BQ_TABLE}` AS
                SELECT * EXCEPT(rn)
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY icao24
                            ORDER BY {order_clause}
                        ) AS rn
                    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{staging_table_name}`
                    WHERE icao24 IS NOT NULL AND TRIM(icao24) != ''
                )
                WHERE rn = 1
            """
            merge_job = client.query(create_from_staging_query)
            merge_job.result()

        dedupe_query = f"""
            CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{RAW_BQ_TABLE}` AS
            SELECT * EXCEPT(rn)
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY icao24
                        ORDER BY {order_clause}
                    ) AS rn
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{RAW_BQ_TABLE}`
                WHERE icao24 IS NOT NULL AND TRIM(icao24) != ''
            )
            WHERE rn = 1
        """
        dedupe_job = client.query(dedupe_query)
        dedupe_job.result()

        # Persist watermark only after successful merge+dedupe to avoid skipping files on retries.
        Variable.set(watermark_key, newest_seen_ts.isoformat())

        return {
            'rows_loaded': result.output_rows,
            'job_id': load_job.job_id,
            'merge_job_id': merge_job.job_id,
            'dedupe_job_id': dedupe_job.job_id,
            'source_uris': new_uris,
            'table': f'{GCP_PROJECT_ID}.{BQ_DATASET}.{RAW_BQ_TABLE}',
        }

    # 4. Load transformed parquet to BigQuery (dashboard table)
    @task(task_id='load_transformed_to_bigquery')
    def load_transformed_to_bigquery(**context):
        """Load transformed files from GCS to BigQuery with upsert on icao24."""
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound
        
        client = bigquery.Client(project=GCP_PROJECT_ID)
        execution_date = resolve_execution_date(context)

        # Ensure dataset exists before loading tables.
        dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET)
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = os.getenv('BIGQUERY_LOCATION', 'US')
            client.create_dataset(dataset)
            logging.info("Created missing dataset: %s.%s", GCP_PROJECT_ID, BQ_DATASET)

        table_ref = client.dataset(BQ_DATASET).table(TRANSFORMED_BQ_TABLE)

        # Ensure target table exists with expected transformed schema.
        target_schema = [
            bigquery.SchemaField("icao24", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("origin_country", "STRING"),
            bigquery.SchemaField("ingestion_date", "STRING"),
            bigquery.SchemaField("altitude_km", "FLOAT"),
            bigquery.SchemaField("speed_kmh", "FLOAT"),
        ]

        try:
            client.get_table(table_ref)
        except NotFound:
            table = bigquery.Table(table_ref, schema=target_schema)
            table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
            client.create_table(table)
            logging.info("Created missing table: %s.%s.%s", GCP_PROJECT_ID, BQ_DATASET, TRANSFORMED_BQ_TABLE)
        
        staging_table_name = f"{TRANSFORMED_BQ_TABLE}_staging"
        staging_table_ref = client.dataset(BQ_DATASET).table(staging_table_name)

        staging_load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            autodetect=True,
        )
        
        uri = f"gs://{GCS_BUCKET}/flight_metrics/ingestion_date={execution_date}/*.parquet"

        load_job = client.load_table_from_uri(
            uri,
            staging_table_ref,
            job_config=staging_load_config
        )

        result = load_job.result()  # Wait for completion

        merge_query = f"""
            MERGE `{GCP_PROJECT_ID}.{BQ_DATASET}.{TRANSFORMED_BQ_TABLE}` T
            USING (
                SELECT
                    icao24,
                    ARRAY_AGG(
                        STRUCT(
                            origin_country,
                            ingestion_date,
                            altitude_km,
                            speed_kmh
                        )
                        ORDER BY ingestion_date DESC
                        LIMIT 1
                    )[OFFSET(0)] AS latest
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{staging_table_name}`
                WHERE icao24 IS NOT NULL
                GROUP BY icao24
            ) S
            ON T.icao24 = S.icao24
            WHEN MATCHED THEN
              UPDATE SET
                origin_country = S.latest.origin_country,
                ingestion_date = S.latest.ingestion_date,
                altitude_km = S.latest.altitude_km,
                speed_kmh = S.latest.speed_kmh
            WHEN NOT MATCHED THEN
              INSERT (icao24, origin_country, ingestion_date, altitude_km, speed_kmh)
              VALUES (
                S.icao24,
                S.latest.origin_country,
                S.latest.ingestion_date,
                S.latest.altitude_km,
                S.latest.speed_kmh
              )
        """

        merge_job = client.query(merge_query)
        merge_job.result()

        dedupe_query = f"""
            CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{TRANSFORMED_BQ_TABLE}`
            PARTITION BY ingestion_date AS
            SELECT
                icao24,
                origin_country,
                ingestion_date,
                altitude_km,
                speed_kmh
            FROM (
                SELECT
                    icao24,
                    origin_country,
                    ingestion_date,
                    altitude_km,
                    speed_kmh,
                    ROW_NUMBER() OVER (
                        PARTITION BY icao24
                        ORDER BY ingestion_date DESC
                    ) AS rn
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{TRANSFORMED_BQ_TABLE}`
                WHERE icao24 IS NOT NULL
            )
            WHERE rn = 1
        """

        dedupe_job = client.query(dedupe_query)
        dedupe_job.result()
        
        return {
            'rows_loaded': result.output_rows,
            'input_file_bytes': getattr(result, 'input_file_bytes', None),
            'input_files': getattr(result, 'input_files', None),
            'job_id': load_job.job_id,
            'merge_job_id': merge_job.job_id,
            'dedupe_job_id': dedupe_job.job_id,
            'table': f'{GCP_PROJECT_ID}.{BQ_DATASET}.{TRANSFORMED_BQ_TABLE}'
        }
    
    # 5. Create BigQuery analytics views
    @task(task_id='create_analytics_views')
    def create_analytics_views(**context):
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)
        execution_date = resolve_execution_date(context)
        query = f"""
            CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{BQ_DATASET}.country_summary` AS
            SELECT
                origin_country,
                DATE(ingestion_date) as flight_date,
                COUNT(DISTINCT icao24) as unique_aircraft,
                COUNT(*) as total_flights,
                AVG(altitude_km) as avg_altitude_km,
                AVG(speed_kmh) as avg_speed_kmh
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{TRANSFORMED_BQ_TABLE}`
            WHERE ingestion_date = '{execution_date}'
            GROUP BY origin_country, flight_date
        """

        job = client.query(query)
        job.result()
        logging.info("Created/updated analytics view country_summary for %s", execution_date)
        return {"view": f"{GCP_PROJECT_ID}.{BQ_DATASET}.country_summary", "date": execution_date}

    create_analytics_views = create_analytics_views()
    
    # 6. Data quality check
    @task(task_id='data_quality_check')
    def data_quality_check(**context):
        """Basic data quality check"""
        from google.cloud import bigquery
        
        client = bigquery.Client(project=GCP_PROJECT_ID)
        execution_date = resolve_execution_date(context)
        
        query = f"""
            SELECT COUNT(*) as row_count
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{TRANSFORMED_BQ_TABLE}`
            WHERE ingestion_date = '{execution_date}'
        """
        
        result = client.query(query).result()
        row = next(result)
        
        if row.row_count == 0:
            raise ValueError(f"No data found for date {execution_date}")
        
        logging.info(f"✅ Data quality check passed: {row.row_count} rows loaded")
        return {"row_count": row.row_count}
    
    # 7. Send success notification
    @task(
        task_id='send_notification',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    def send_success(**context):
        """Send success notification"""
        execution_date = resolve_execution_date(context)
        logging.info(f"✅ Pipeline completed successfully for {execution_date}")
        return {'status': 'success', 'execution_date': execution_date}
    
    # Build the DAG
    kafka_check = check_kafka_stream()
    raw_data_ready = wait_for_raw_flight_data()
    process_flight_batch = process_flight_batch_task(raw_data_ready)
    raw_bq_load = load_raw_stream_to_bigquery(raw_data_ready)
    transformed_bq_load = load_transformed_to_bigquery()
    quality_check = data_quality_check()
    notify = send_success()

    trigger_data_quality_dag = TriggerDagRunOperator(
        task_id='trigger_data_quality_dag',
        trigger_dag_id=DATA_QUALITY_DAG_ID,
        conf={
            'project_id': GCP_PROJECT_ID,
            'dataset': BQ_DATASET,
            'table_name': TRANSFORMED_BQ_TABLE,
            'execution_date': '{{ ds }}',
        },
        wait_for_completion=False,
        reset_dag_run=False,
    )

    kafka_check >> raw_data_ready
    raw_data_ready >> raw_bq_load
    raw_data_ready >> process_flight_batch >> transformed_bq_load >> create_analytics_views >> quality_check
    raw_bq_load >> notify
    quality_check >> notify >> trigger_data_quality_dag

# Create the DAG
dag = aerostream_pipeline()