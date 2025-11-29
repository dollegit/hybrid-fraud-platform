from __future__ import annotations

import pendulum
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# Define the path to the SparkApplication YAML file
SPARK_TEST_YAML_PATH = Path(__file__).parent / "jobs" / "spark-test.yaml"

with DAG(
    dag_id="spark_operator_test_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["spark", "test", "kubernetes-operator"],
    doc_md="""
    ### Spark Operator Test DAG

    This DAG tests the Spark on Kubernetes Operator by submitting a simple print job.
    It uses the `SparkKubernetesOperator` to apply a predefined `SparkApplication` manifest.
    """,
) as dag:
    submit_spark_test_job = SparkKubernetesOperator(
        task_id="submit_simple_print_job",
        # The namespace where the SparkApplication resource will be created.
        # This should match the namespace where your Spark Operator is running.
        namespace="spark-jobs",
        # The application_file should point to the YAML manifest for the SparkApplication.
        # Airflow will read this file and use its content as the body for the API request.
        # application_file=SPARK_TEST_YAML_PATH.as_posix(),
        application_file="jobs/spark-test.yaml",
        # The Kubernetes connection ID configured in Airflow UI.
        # 'kubernetes_default' is the default connection ID created by the Helm chart.
        kubernetes_conn_id="kubernetes_default",
        # Set to 'false' to keep the driver pod for debugging after completion.
        # Set to 'true' for production to clean up automatically.
        delete_on_termination=False,
    )