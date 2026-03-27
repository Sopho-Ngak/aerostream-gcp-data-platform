"""
Simple test DAG to verify Airflow 3 is working
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'aerostream',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
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