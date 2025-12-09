# COMMAND ----------
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from pathlib import Path
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# Define the path to the Spark job YAML file for better portability
SPARK_ETL_JOB_YAML = Path(__file__).parent / "jobs" / "spark_etl_job.yaml"

with DAG(
    dag_id="on_prem_payment_processing",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["fraud-platform", "on-prem"],
) as dag:
    process_payments = SparkKubernetesOperator(
        task_id="process_on_prem_payments",
        application_file=SPARK_ETL_JOB_YAML.read_text(),
        namespace="spark-jobs",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        delete_on_termination=True,
        deferrable=False,
    )