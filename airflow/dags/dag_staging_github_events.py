from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
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
    start_date=datetime(2025, 1, 1),
    catchup=True,
    tags=['ingestion', 'spark', 'iceberg'],
    max_active_runs=1
) as dag:

    submit = SparkKubernetesOperator(
        task_id="ingest_github_hour",
        namespace="spark-operator",
        application_file="staging/github_events.yaml",
        kubernetes_conn_id="kubernetes_default",
        dag=dag,
    )

    submit