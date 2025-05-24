import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

log = logging.getLogger(__name__) # <--- ADDED: Initialize logger

# Import your ETL functions
from extract import read_json_log
from transform import clean_logs
from load import load_data
# Assuming utils/quality_checks is accessible via volumes or sys.path
from utils.quality_checks import run_quality_checks

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': lambda context: log.error(f"DAG run failed: {context['dag_run'].run_id}", exc_info=True),
}

with DAG(
    dag_id='etl_pipeline_dag',
    default_args=default_args,
    description='ETL pipeline for ingesting and processing JSON logs',
    # --- IMPORTANT CHANGE FOR AIRFLOW 3.x ---
    # `schedule_interval` is replaced by `schedule`.
    # For simple cron strings like '@daily', you can use it directly.
    schedule='@daily',
    # ----------------------------------------
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'json', 'postgres'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=read_json_log,
        on_success_callback=lambda context: log.info(f"Task {context['task_instance'].task_id} completed successfully."),
        on_failure_callback=lambda context: log.error(f"Task {context['task_instance'].task_id} failed.", exc_info=True),
    )
    pre_transform_quality_check_task = PythonOperator(
        task_id='pre_transform_quality_check',
        python_callable=run_quality_checks,
        op_kwargs={
            # These fields are checked for nulls AND used as keys for duplicates on the RAW data
            'key_fields_for_duplicates': ['user_id', 'timestamp', 'action_type']
            # You might also want a separate list for fields to check for nulls
            # 'fields_for_null_check': ['user_id', 'action_type']
        },
        on_success_callback=lambda context: log.info(f"Task {context['task_instance'].task_id} completed successfully."),
        on_failure_callback=lambda context: log.error(f"Task {context['task_instance'].task_id} failed.", exc_info=True),
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=clean_logs,
        on_success_callback=lambda context: log.info(f"Task {context['task_instance'].task_id} completed successfully."),
        on_failure_callback=lambda context: log.error(f"Task {context['task_instance'].task_id} failed.", exc_info=True),
)

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
        on_success_callback=lambda context: log.info(f"Task {context['task_instance'].task_id} completed successfully."),
        on_failure_callback=lambda context: log.error(f"Task {context['task_instance'].task_id} failed.", exc_info=True),
    )

    log.info("Setting up task dependencies: Extract -> Transform -> Load -> Quality Check.")
    extract_task >> pre_transform_quality_check_task >> transform_task >> load_task

log.info("ETL pipeline DAG definition complete.")
