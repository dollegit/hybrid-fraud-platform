# COMMAND ----------
import pendulum
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# =============================================================================
# CONSTANTS
# =============================================================================
DAG_FOLDER = Path(__file__).parent
PROJECT_ROOT = DAG_FOLDER.parent.parent # Resolves to /opt/airflow/dags/repo
DBT_PROJECT_PATH = PROJECT_ROOT / "5-dbt-project"

# =============================================================================
# DBT CONFIGURATION
# =============================================================================
# Common environment variables for all dbt tasks.
# We pass the database credentials via environment variables, which dbt's `profiles.yml`
# is configured to use. This is more secure than hardcoding them.
# DBT_ENV_VARS = {
#     "DB_HOSTNAME": "{{ conn.postgres_default.host }}",
#     "DB_USER": "{{ conn.postgres_default.login }}",
#     "DB_PASSWORD": "{{ conn.postgres_default.password }}",
#     "DB_NAME": "{{ conn.postgres_default.schema }}",
# }
DBT_ENV_VARS = {
    "DB_HOSTNAME": "airflow-postgresql.airflow.svc.cluster.local",
    "DB_USER": "airflow",
    "DB_PASSWORD": "airflow",
    "DB_NAME": "airflow",
    # optionally:
    "PGPASSWORD": "airflow"
}

DBT_TARGET_PATH = "/tmp/dbt-target"
DBT_LOG_PATH = "/tmp/dbt-logs"

extra_env = {
    "DBT_LOG_FORMAT": "text",
    "DBT_LOG_LEVEL": "debug",
    "DBT_LOG_PATH": DBT_LOG_PATH,
}

# =============================================================================
# DAG DEFINITION
# =============================================================================
with DAG(
    dag_id="fraud_detection_pipeline_dbt_core",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["spark", "dbt", "kubernetes-operator"],
    doc_md="""
    ### Fraud Detection ETL/ELT Pipeline

    This DAG orchestrates a full data pipeline:
    1.  **Generate Data**: A Spark job generates sample CSV files into a shared Persistent Volume.
    2.  **Consolidate Data**: A second Spark job reads the CSVs and loads them into a PostgreSQL staging database.
    3.  **Transform Data**: dbt Core runs transformations on the staging data to create final analytics models.
    """,
) as dag:
    # Task 1: Generate sample data using Spark on Kubernetes
    generate_sample_data = SparkKubernetesOperator(
        task_id="generate_sample_data",
        application_file="jobs/generate_sample_data.yaml",
        namespace="spark-jobs",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,  # 🔥 FIX: Disable XCom push
    )

    # Task 2: Consolidate data from PVC into PostgreSQL staging tables
    consolidate_data_to_staging = SparkKubernetesOperator(
        task_id="consolidate_data_to_staging",
        application_file="jobs/consolidate_data.yaml",
        namespace="spark-jobs",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,  # 🔥 FIX: Disable XCom push
    )

    # Task 3: Run dbt models to transform the staged data
    # We use a BashOperator to run dbt. This assumes `dbt` is installed in the
    # Airflow worker's container image.
    dbt_seed = BashOperator(
        task_id="dbt_seed_models",
        bash_command=f"""
        mkdir -p {DBT_TARGET_PATH} {DBT_LOG_PATH}
        /home/airflow/.local/bin/dbt seed \
        --project-dir {DBT_PROJECT_PATH} \
        --profiles-dir {DBT_PROJECT_PATH} \
        --target prod \
        --target-path {DBT_TARGET_PATH}
        """,
        env={**DBT_ENV_VARS, **extra_env},
    )

    
    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command=f"""
        env  # dump all env vars
        mkdir -p {DBT_TARGET_PATH} {DBT_LOG_PATH}
        /home/airflow/.local/bin/dbt debug \
        --project-dir {DBT_PROJECT_PATH} \
        --profiles-dir {DBT_PROJECT_PATH} \
        --target prod \
        --target-path {DBT_TARGET_PATH}
        /home/airflow/.local/bin/dbt run \
        --project-dir {DBT_PROJECT_PATH} \
        --profiles-dir {DBT_PROJECT_PATH} \
        --target prod \
        --target-path {DBT_TARGET_PATH}
        """,
        env={**DBT_ENV_VARS, **extra_env},
    )


    # Task 4: Test the dbt models to ensure data quality
    dbt_test = BashOperator(
        task_id="dbt_test_models",
        bash_command=f"""
        env  # dump all env vars
        mkdir -p {DBT_TARGET_PATH} {DBT_LOG_PATH}
        /home/airflow/.local/bin/dbt test \
        --project-dir {DBT_PROJECT_PATH} \
        --profiles-dir {DBT_PROJECT_PATH} \
        --target prod \
        --target-path {DBT_TARGET_PATH}
        """,
        env={**DBT_ENV_VARS, **extra_env},
    )

    # Define the task dependencies
    (
        generate_sample_data
        >> consolidate_data_to_staging
        >> dbt_seed
        >> dbt_run
        >> dbt_test
    )