from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from time import sleep
import pandas as pd


def first_function_execute():
    print('hello world')
    return 'hello world'

with DAG(
    dag_id="first_dag",
    #schedule_interval="@daily", # execute every day
    default_args={
        "owner": "parallels",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "start_date": datetime(2020, 1, 1),
        },
    catchup=False) as f:

    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute)

