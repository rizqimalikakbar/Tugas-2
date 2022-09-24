from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2022, 9, 23),
    'end_date': datetime(2023,9,23) 
}

with DAG(
    'belajar_xcom',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False) as dag:
    
    download_data = BashOperator(
        task_id='download_data',
        bash_command='echo "{{ ti.xcom_push(key="key1", value="value from other task") }}"',
        do_xcom_push=True
    )

    format_to_parquet = BashOperator(
        task_id='format_to_parquet',
        bash_command='echo "{{ ti.xcom_pull(key="key1") }}"',
        do_xcom_push=True
    )

    upload_to_gcs = BashOperator(
        task_id='upload_to_gcs',
        bash_command='echo "{{ ti.xcom_pull(key="key1") }}"',
        do_xcom_push=False
    )

download_data >> format_to_parquet >> upload_to_gcs