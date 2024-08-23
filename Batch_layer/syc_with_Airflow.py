import sys
import os

# Thêm đường dẫn gốc của dự án vào sys.path
sys.path.append('/home/h-user/Predict-Phone-Price')


import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from batch_layer import  batch_layer


with DAG(
    dag_id="daily_data_sync",
    start_date=datetime.datetime(2024, 8, 23),
    schedule_interval="*/1 * * * *",  # Run every 1 minutes
) as dag:
    batch_layer = PythonOperator(
        task_id="batch_layer",
        python_callable=batch_layer
    )

batch_layer
