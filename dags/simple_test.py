from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from datetime import datetime, timedelta
import logging

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

def print_hello():
    """Simple hello function"""
    logging.info("=" * 50)
    logging.info("Hello from Airflow 3!")
    logging.info("=" * 50)
    return "Hello World!"

# Create DAG
dag = DAG(
    'simple_test_v2',
    description="A simple test DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["test_v2"],
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