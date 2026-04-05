"""
Simple test DAG to verify Airflow 3 is working
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta

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

@dag(
    dag_id='test_airflow_3',
    default_args=default_args,
    description='Test DAG for Airflow 3',
    schedule='@daily',
    catchup=False,
    tags=['test'],
)
def test_airflow_3():
    
    @task
    def hello_world():
        print("Hello from Airflow 3!")
        return "Hello World!"
    
    @task
    def print_date(**context):
        print(f"Execution date: {context['ds']}")
        return context['ds']
    
    @task
    def success_message():
        print("✅ Airflow 3 is working correctly!")
        return "Success"
    
    hello = hello_world()
    date = print_date()
    success = success_message()
    
    hello >> date >> success

dag = test_airflow_3()