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
        application_file="jobs/spark_etl_job.yaml",  # ✅ Relative path from /opt/airflow/dags/
        namespace="spark-jobs",
        kubernetes_conn_id="kubernetes_default",
        # 👇 Copy the logic from the fix here
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.kubernetes.executor.volumes.configMap.spark-job-script.mount.path": "/opt/spark/work-dir",
            "spark.kubernetes.executor.volumes.configMap.spark-job-script.mount.readOnly": "true",
            "spark.kubernetes.executor.volumes.configMap.spark-job-script.options.name": "on-prem-etl-script"
        },
        do_xcom_push=True,
    )