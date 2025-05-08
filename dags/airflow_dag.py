from airflow import DAG, task
from datetime import datetime, timedelta
from clean import *
from kapka_stream import stream_data_and_log_alert
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Damilola Adeniyi",
    "start_date": datetime(2025, 5, 7),
    "retries": 4,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
        dag_id="bikeshare_dag_v2",
        default_args=default_args,
        schedule="0 10 * * 1",
        catchup=False,
        description='This is my bikeshare dag',
) as dag:
    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
    )
    generate_task = PythonOperator(
        task_id='generate_heatmap',
        python_callable=generate_heatmap,
    )
    partition_task = PythonOperator(
        task_id='partition_by_parquet',
        python_callable=partition_by_parquet,
    )
    streaming_task = PythonOperator(
        task_id='stream_data_and_log_alert',
        python_callable=stream_data_and_log_alert,
    )

    clean_task.set_downstream(generate_task)
    clean_task.set_downstream(partition_task)
    clean_task.set_downstream(streaming_task)
