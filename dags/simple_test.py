from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

def print_hello():
    """Simple hello function"""
    logging.info("=" * 50)
    logging.info("Hello from Airflow 3!")
    logging.info("=" * 50)
    return "Hello World!"

# Create DAG
dag = DAG(
    'simple_test_v2',
    default_args=default_args,
    description='Simple test DAG',
    schedule_interval='@daily',
    catchup=False,
    tags=['test'],
)

# Create tasks
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

echo_task = BashOperator(
    task_id='echo_task',
    bash_command='echo "Current time: $(date)"',
    dag=dag,
)

date_task = BashOperator(
    task_id='date_task',
    bash_command='date',
    dag=dag,
)

# Set dependencies
hello_task >> echo_task >> date_task