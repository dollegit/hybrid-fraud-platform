from __future__ import annotations

import pendulum
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

# =============================================================================
# PATH DEFINITIONS
# =============================================================================
# Define the project root and dbt project path.
# This makes the DAG portable and independent of the execution environment.
PROJECT_ROOT = Path(__file__).parent.parent.parent
DBT_PROJECT_PATH = PROJECT_ROOT / "5-dbt-project"
DBT_VENV_PATH = PROJECT_ROOT / "dbt_venv/bin/activate" # Path to the dbt virtualenv

# =============================================================================
# DBT CONFIGURATION
# =============================================================================
# Instead of using cosmos's ProfileConfig, we'll construct the dbt command directly.
# We need to tell dbt where to find the project and profiles.
# The `profiles.yml` is located inside the DBT_PROJECT_PATH.
# We'll use the `prod` target as defined in `profiles.yml`.

# Common environment variables for all dbt tasks.
# We pass the database credentials via environment variables, which dbt's `profiles.yml`
# will pick up using the `env_var()` function. This is a secure practice.
# NOTE: You must have Airflow Connections/Variables set up for these (e.g., 'DB_HOSTNAME').
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
    schedule="@daily",
    catchup=False,
    tags=["spark", "dbt", "kubernetes-operator", "pipeline"],
    doc_md="""
    ### Fraud Detection Pipeline (dbt-core)

    **Note on Data Generation**: This pipeline includes a `generate_sample_data` task that creates sample CSV files.
    The subsequent Spark job (`consolidate_data`) reads these files. In a real-world scenario, the initial data
    would likely come from external sources (e.g., Kafka streams processed into a data lake like MinIO),
    and this generation step would be replaced by a task that waits for or triggers that data's arrival.

    This DAG orchestrates the end-to-end fraud detection data pipeline.
    1.  **Consolidate Data**: A Spark job consolidates raw data from various sources into staging tables in PostgreSQL.
    2.  **DBT Run**: dbt models are executed to transform staging data into a unified analytics table.
    3.  **DBT Test**: dbt tests are run to ensure data quality and integrity.
    """,
) as dag:
    # --- Task 1: Consolidate data with Spark ---
    # NOTE: In a real pipeline, this step would be replaced by data arriving from upstream sources.
    # Here, we generate sample data and place it where the consolidate_data job expects it.
    generate_sample_data = SparkKubernetesOperator(
        task_id="generate_sample_data",
        application_file="jobs/generate_sample_data.yaml",
        namespace="spark-jobs",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        delete_on_termination=True,
        deferrable=False,
    )

    consolidate_data = SparkKubernetesOperator(
        task_id="consolidate_data_to_staging",
        application_file="jobs/consolidate_data.yaml",
        namespace="spark-jobs",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        delete_on_termination=True,
        deferrable=False,
        doc_md="""
        #### Spark Data Consolidation
        Submits a Spark application to Kubernetes. This job reads data from MinIO (or other sources),
        processes it, and loads it into `raw_*` tables in the PostgreSQL database, making it ready for dbt.
        """,
    )

    # --- Task 2: Run dbt models ---
    # Replaces the DbtTaskGroup. This uses BashOperator to call dbt directly.
    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command=(
            # Activate the virtual environment, then run dbt
            f"source {DBT_VENV_PATH} && "
            f"dbt run --project-dir {DBT_PROJECT_PATH} --profiles-dir {DBT_PROJECT_PATH} --target prod"
        ),
        env=DBT_ENV_VARS,
    )

    # --- Task 3: Test dbt models ---
    dbt_test = BashOperator(
        task_id="dbt_test_models",
        bash_command=(
            f"source {DBT_VENV_PATH} && "
            f"dbt test --project-dir {DBT_PROJECT_PATH} --profiles-dir {DBT_PROJECT_PATH} --target prod"
        ),
        env=DBT_ENV_VARS,
    )

    # --- Define Task Dependencies ---
    generate_sample_data >> consolidate_data >> dbt_run >> dbt_test