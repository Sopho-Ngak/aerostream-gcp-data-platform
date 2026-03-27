"""
Data Quality DAG for AeroStream
Runs quality checks on BigQuery tables
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryValueCheckOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'aerostream',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
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
    check_row_count = BigQueryCheckOperator(
        task_id='check_row_count',
        sql="""
            SELECT COUNT(*) 
            FROM `{{ dag_run.conf['project_id']|default(run_id, true) }}.{{ dag_run.conf['dataset'] }}.{{ dag_run.conf['table_name'] }}`
            WHERE ingestion_date = '{{ dag_run.conf['execution_date'] }}'
            HAVING COUNT(*) > 0
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )
    
    # Column null check
    @task(task_id='check_nulls')
    def check_nulls(**context):
        """Check for null values in critical columns"""
        from google.cloud import bigquery
        
        client = bigquery.Client()
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNTIF(icao24 IS NULL) as null_icao24,
                COUNTIF(longitude IS NULL) as null_longitude,
                COUNTIF(latitude IS NULL) as null_latitude,
                COUNTIF(altitude_km IS NULL) as null_altitude
            FROM `{context['dag_run'].conf['project_id']}.{context['dag_run'].conf['dataset']}.{context['dag_run'].conf['table_name']}`
            WHERE ingestion_date = '{context['dag_run'].conf['execution_date']}'
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
        
        client = bigquery.Client()
        table_ref = f"{context['dag_run'].conf['project_id']}.{context['dag_run'].conf['dataset']}.{context['dag_run'].conf['table_name']}"
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
        
        client = bigquery.Client()
        query = f"""
            SELECT MAX(ingestion_date) as latest_date
            FROM `{context['dag_run'].conf['project_id']}.{context['dag_run'].conf['dataset']}.{context['dag_run'].conf['table_name']}`
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
    null_check = check_nulls()
    schema_check = validate_schema()
    freshness_check = check_freshness()
    
    # Chain dependencies
    check_row_count >> null_check >> schema_check >> freshness_check

dag = data_quality_dag()