from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

# Define the path to the Spark job YAML file
SPARK_STREAMING_JOB_YAML = (
    Path(__file__).parent / "jobs" / "streaming_process_payment.yaml"
)

with DAG(
    dag_id="streaming_payment_processor_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # This DAG is meant to be triggered manually to start the stream
    catchup=False,
    tags=["streaming", "spark", "kubernetes-operator", "kafka"],
    doc_md="""
    ### Streaming Payment Processor DAG

    This DAG launches a long-running Spark Streaming job on Kubernetes.

    The job reads payment events from a Kafka topic, processes them, and writes
    the results to MinIO.

    **Note:** This DAG only *starts* the job. The SparkApplication itself will
    run continuously until it is manually stopped or fails.
    """,
) as dag:
    start_streaming_job = SparkKubernetesOperator(
        task_id="start_spark_streaming_job",
        application_file=SPARK_STREAMING_JOB_YAML.read_text(),
        namespace="spark-jobs",
        do_xcom_push=True,
    )