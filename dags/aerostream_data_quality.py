"""
Data Quality DAG for AeroStream
Runs quality checks on BigQuery tables
"""

from airflow.decorators import dag, task
import os
from datetime import datetime, timedelta

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

@dag(
    dag_id='aerostream_data_quality',
    default_args=default_args,
    description='Data quality checks for AeroStream',
    schedule=None,  # Triggered by main DAG
    catchup=False,
    tags=['aerostream', 'data-quality', 'airflow3'],
    render_template_as_native_obj=True,
)
def data_quality_dag():
    """Data quality checks for flight data"""

    # Row count check
    @task(task_id='check_row_count')
    def check_row_count(**context):
        """Check that the target table has rows for the given date"""
        from google.cloud import bigquery

        conf = context['dag_run'].conf or {}
        project_id = conf.get('project_id', os.getenv('GCP_PROJECT_ID', 'aerostream-project'))
        dataset = conf.get('dataset', os.getenv('BIGQUERY_DATASET', 'aviation'))
        table_name = conf.get('table_name', 'flight_metrics')
        execution_date = conf.get('execution_date', str(context.get('ds', '')))

        client = bigquery.Client(project=project_id)
        query = f"""
            SELECT COUNT(*) as row_count
            FROM `{project_id}.{dataset}.{table_name}`
            WHERE ingestion_date = '{execution_date}'
        """
        result = client.query(query).result()
        row = next(result)
        if row.row_count == 0:
            raise ValueError(f"No rows found in {project_id}.{dataset}.{table_name} for date {execution_date}")
        return {"status": "passed", "row_count": row.row_count}
    
    # Column null check
    @task(task_id='check_nulls')
    def check_nulls(**context):
        """Check for null values in critical columns"""
        from google.cloud import bigquery

        conf = context['dag_run'].conf or {}
        project_id = conf.get('project_id', os.getenv('GCP_PROJECT_ID', 'aerostream-project'))
        dataset = conf.get('dataset', os.getenv('BIGQUERY_DATASET', 'aviation'))
        table_name = conf.get('table_name', 'flight_metrics')
        execution_date = conf.get('execution_date', str(context.get('ds', '')))

        client = bigquery.Client(project=project_id)
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNTIF(icao24 IS NULL) as null_icao24,
                COUNTIF(longitude IS NULL) as null_longitude,
                COUNTIF(latitude IS NULL) as null_latitude,
                COUNTIF(altitude_km IS NULL) as null_altitude
            FROM `{project_id}.{dataset}.{table_name}`
            WHERE ingestion_date = '{execution_date}'
        """
        
        result = client.query(query).result()
        row = next(result)
        
        thresholds = {
            'null_icao24': 5,
            'null_longitude': 10,
            'null_latitude': 10,
            'null_altitude': 15
        }
        
        issues = []
        for field, threshold in thresholds.items():
            null_count = getattr(row, field)
            if null_count > threshold:
                issues.append(f"{field}: {null_count} nulls (threshold: {threshold})")
        
        if issues:
            raise ValueError(f"Data quality issues found: {', '.join(issues)}")
        
        return {"status": "passed", "checks": issues}
    
    # Schema validation
    @task(task_id='validate_schema')
    def validate_schema(**context):
        """Validate that table schema matches expected structure"""
        from google.cloud import bigquery

        conf = context['dag_run'].conf or {}
        project_id = conf.get('project_id', os.getenv('GCP_PROJECT_ID', 'aerostream-project'))
        dataset = conf.get('dataset', os.getenv('BIGQUERY_DATASET', 'aviation'))
        table_name = conf.get('table_name', 'flight_metrics')

        expected_fields = {
            'icao24': 'STRING',
            'callsign': 'STRING',
            'origin_country': 'STRING',
            'longitude': 'FLOAT',
            'latitude': 'FLOAT',
            'altitude_km': 'FLOAT',
            'speed_kmh': 'FLOAT',
            'ingestion_date': 'DATE'
        }
        
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset}.{table_name}"
        table = client.get_table(table_ref)
        
        actual_fields = {field.name: field.field_type for field in table.schema}
        
        mismatches = []
        for expected_field, expected_type in expected_fields.items():
            if expected_field not in actual_fields:
                mismatches.append(f"Missing field: {expected_field}")
            elif actual_fields[expected_field] != expected_type:
                mismatches.append(f"Field {expected_field} has type {actual_fields[expected_field]}, expected {expected_type}")
        
        if mismatches:
            raise ValueError(f"Schema validation failed: {', '.join(mismatches)}")
        
        return {"status": "passed"}
    
    # Freshness check
    @task(task_id='check_freshness')
    def check_freshness(**context):
        """Check if data is recent enough"""
        from google.cloud import bigquery
        from datetime import datetime, timedelta

        conf = context['dag_run'].conf or {}
        project_id = conf.get('project_id', os.getenv('GCP_PROJECT_ID', 'aerostream-project'))
        dataset = conf.get('dataset', os.getenv('BIGQUERY_DATASET', 'aviation'))
        table_name = conf.get('table_name', 'flight_metrics')

        client = bigquery.Client(project=project_id)
        query = f"""
            SELECT MAX(ingestion_date) as latest_date
            FROM `{project_id}.{dataset}.{table_name}`
        """
        
        result = client.query(query).result()
        row = next(result)
        latest_date = row.latest_date
        
        max_lag = timedelta(days=1)
        current_date = datetime.now().date()
        
        if latest_date < current_date - max_lag:
            raise ValueError(f"Data is stale. Latest date: {latest_date}, expected within last {max_lag}")
        
        return {"latest_date": str(latest_date), "status": "fresh"}
    
    # Run all checks
    row_count_check = check_row_count()
    null_check = check_nulls()
    schema_check = validate_schema()
    freshness_check = check_freshness()

    # Chain dependencies
    row_count_check >> null_check >> schema_check >> freshness_check

dag = data_quality_dag()