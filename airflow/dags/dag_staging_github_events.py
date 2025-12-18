from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from staging.github_events import run_idempotent_etl


default_args = {
    'owner': 'data-lead',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'github_archive_stream_backfill',
    default_args=default_args,
    description='Stream GitHub Archive to Iceberg',
    schedule='@hourly', 
    start_date=datetime(2025, 10, 1),
    catchup=True,
    tags=['ingestion', 'spark', 'iceberg'],
    max_active_runs=1
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_github_hour',
        python_callable=run_idempotent_etl,
        # provide_context=True
    )