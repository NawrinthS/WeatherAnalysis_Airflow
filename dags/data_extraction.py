from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from utils.dag_functions import get_conf_date

import constants.constant as CONSTANTS




def process_weather_data(**kwargs):
    """Pulls config from XCom and executes the extraction logic."""
    ti = kwargs["ti"]
    run_date = ti.xcom_pull(
        task_ids="get_conf_date_task", key="run_date"
    )

    print(f"--- Weather Extraction Job Started ---")
    print(f"Processing data for Date: {run_date}")
    # TODO: Add your Open-Meteo API batch extraction logic here


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_extract_data",
    default_args=default_args,
    description="Pipeline with explicit config extraction and XCom passing",
    schedule_interval="0 4 * * *",
    catchup=False,
) as dag:

    
    start = EmptyOperator(task_id="start")

    get_conf_date_task = PythonOperator(
        task_id="get_conf_date_task",
        python_callable=get_conf_date,
    )

    # Trigger checker
    coordinates_trigger_checker = S3KeySensor(
        task_id='coordinates_trigger_checker',
        bucket_name=CONSTANTS.S3_BUCKET,
        bucket_key=CONSTANTS.TRIGGER_CHECKER_PATH_COORDINATES + CONSTANTS.TRIGGER_FILE_NAME,
        aws_conn_id='aws_default',
        poke_interval=60,  # Checks every 60 seconds
        timeout=3600,      # Fails if file doesn't appear within 1 hour
        mode='poke',       # Use 'reschedule' to free up worker slots for long waits
    )

    create_trigger_path = (
        f"{CONSTANTS.INPUT_TRIGGER_CHECKER_PATH}"
        "{{ ti.xcom_pull(task_ids='get_conf_date_task')['date_slash'] }}/"
        f"{CONSTANTS.TRIGGER_FILE_NAME}"
    )

    create_trigger_file = S3CreateObjectOperator(
        task_id="create_triggers_file",
        aws_conn_id="aws_default",          # Your Airflow connection ID for AWS
        s3_bucket="weather-analysis-open-metro",        # The target S3 bucket
        s3_key=create_trigger_path, # The S3 path/key for the file
        data="",                             # Passing an empty string creates a 0-byte file
        replace=True                         # Overwrites the file if it already exists
    )

    extract_weather_task = PythonOperator(
        task_id="extract_weather_task",
        python_callable=process_weather_data,
    )

    end = EmptyOperator(task_id="end")

    # Define the execution flow
    start >> get_conf_date_task >> coordinates_trigger_checker >> extract_weather_task >> create_trigger_file >> end