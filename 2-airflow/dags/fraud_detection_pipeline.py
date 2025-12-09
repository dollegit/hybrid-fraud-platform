import pendulum
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# =============================================================================
# CONSTANTS
# =============================================================================
PROJECT_ROOT = Path(__file__).parent.parent
DBT_PROJECT_PATH = PROJECT_ROOT / "5-dbt-project"
DBT_VENV_PATH = PROJECT_ROOT / "dbt_venv/bin/activate"

# =============================================================================
# DBT CONFIGURATION
# =============================================================================
# Common environment variables for all dbt tasks.
# We pass the database credentials via environment variables, which dbt's `profiles.yml`
# is configured to use. This is more secure than hardcoding them.
DBT_ENV_VARS = {
    "DB_HOSTNAME": "{{ conn.postgres_default.host }}",
    "DB_USER": "{{ conn.postgres_default.login }}",
    "DB_PASSWORD": "{{ conn.postgres_default.password }}",
    "DB_NAME": "{{ conn.postgres_default.schema }}",
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
    # We use a BashOperator to activate the virtual environment and run dbt.
    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command=f"""
        set -e
        source {DBT_VENV_PATH}
        dbt run --project-dir {DBT_PROJECT_PATH} --profiles-dir {DBT_PROJECT_PATH} --target prod
        """,
        env=DBT_ENV_VARS,
    )

    # Task 4: Test the dbt models to ensure data quality
    dbt_test = BashOperator(
        task_id="dbt_test_models",
        bash_command=f"""
        set -e
        source {DBT_VENV_PATH}
        dbt test --project-dir {DBT_PROJECT_PATH} --profiles-dir {DBT_PROJECT_PATH} --target prod
        """,
        env=DBT_ENV_VARS,
    )

    # Define the task dependencies
    (
        generate_sample_data
        >> consolidate_data_to_staging
        >> dbt_run
        >> dbt_test
    )