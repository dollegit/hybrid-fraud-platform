# COMMAND ----------
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

with DAG(
    dag_id="on_prem_payment_processing",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["fraud-platform", "on-prem"],
) as dag:
    process_payments = SparkKubernetesOperator(
        task_id="process_on_prem_payments",
        application_file="jobs/spark_etl_job.yaml",
        namespace="spark-jobs",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        delete_on_termination=True,
        deferrable=False,
    )