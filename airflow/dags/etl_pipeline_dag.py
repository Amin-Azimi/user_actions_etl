from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import your ETL functions
from scripts.etl.extract import read_json_logs
from scripts.etl.transform import clean_and_transform
from scripts.etl.load import load_to_postgres
from scripts.utils.quality_checks import run_data_quality_checks

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='etl_pipeline_dag',
    default_args=default_args,
    description='ETL pipeline for ingesting and processing JSON logs',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'json', 'postgres'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=read_json_logs,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=clean_and_transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_to_postgres,
    )

    quality_check_task = PythonOperator(
        task_id='quality_check',
        python_callable=run_data_quality_checks,
    )

    extract_task >> transform_task >> load_task >> quality_check_task
