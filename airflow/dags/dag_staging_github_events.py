from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)



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
    max_active_runs=2
) as dag:

    submit = SparkKubernetesOperator(
        task_id="ingest_github_hour",
        namespace="spark-operator",
        application_file="staging/github_events.yaml",
        kubernetes_conn_id="kubernetes_default",
        dag=dag
    )