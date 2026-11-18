from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


def get_conf_date(**kwargs):
    """Extracts config from dag_run, validates it, and pushes to XCom."""
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    
    target_date = conf.get(
        "run_date", datetime.today().strftime("%Y-%m-%d")
    )

    print(f"--- Config Extracted ---")
    print(f"Run Date: {target_date}")

    # Push to XCom so downstream tasks can use them
    ti = kwargs["ti"]
    ti.xcom_push(key="run_date", value=target_date)


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
    dag_id="weather_process_data",
    default_args=default_args,
    description="Pipeline with explicit config extraction and XCom passing",
    schedule_interval=None,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    get_conf_date_task = PythonOperator(
        task_id="get_conf_date_task",
        python_callable=get_conf_date,
    )

    extract_weather_task = PythonOperator(
        task_id="process_weather_task",
        python_callable=process_weather_data,
    )

    # Trigger checker
    wait_for_trigger_file = S3KeySensor(
        task_id='wait_for_trigger_file',
        bucket_name='weather-analysis-open-metro',
        bucket_key='triggers/static-files/coordinates-files/complete.dat',
        aws_conn_id='aws_default',
        poke_interval=60,  # Checks every 60 seconds
        timeout=3600,      # Fails if file doesn't appear within 1 hour
        mode='poke',       # Use 'reschedule' to free up worker slots for long waits
    )

    end = EmptyOperator(task_id="end")

    # Define the execution flow
    start >> get_conf_date_task >> extract_weather_task >> end