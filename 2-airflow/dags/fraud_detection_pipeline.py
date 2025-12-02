# COMMAND ----------
from __future__ import annotations

import pendulum
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# Cosmos imports
from cosmos.providers.dbt.task_group import DbtTaskGroup, DbtTask
from cosmos.config import ProjectConfig, ProfileConfig
from cosmos.constants import ExecutionMode

# =============================================================================
# CONFIGURATION
# =============================================================================
# In a production environment, these paths and configurations should be managed
# using Airflow Variables, Connections, and a proper Git-sync deployment.

# Define the base path for the project within the Airflow environment.
# This assumes the DAG file is in a 'dags' folder and the project is checked out alongside it.
PROJECT_ROOT = Path(__file__).parent.parent.parent # Corrected: Go up three levels to reach 'hybrid-fraud-platform'

DBT_PROJECT_PATH = PROJECT_ROOT / "5-dbt-project"
DBT_EXECUTABLE_PATH = PROJECT_ROOT / "dbt_venv/bin/dbt"

# Define the profile for dbt to use. This assumes you have a `profiles.yml`
# file in your dbt project directory and an Airflow connection named `dbt_postgres_conn`.
profile_config = ProfileConfig(
    profile_name="fraud_detection_dbt",
    # For production runs, we explicitly set the target to 'prod'.
    target_name="prod",
    profiles_yml_filepath=(DBT_PROJECT_PATH / "profiles.yml"),
)

# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id='fraud_detection_elt_pipeline',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule='@daily',
    catchup=False,
    tags=['fraud', 'dbt', 'spark'],
    doc_md="""
    ### Fraud Detection ELT Pipeline

    This DAG orchestrates the ELT pipeline for the fraud detection platform, following the architecture
    described in the project's README.md.

    1.  **Extract & Load (Spark)**: A Spark job reads raw data from various sources and loads it into
        the data warehouse as staging tables.
    2.  **Transform & Test (dbt)**: A group of dbt tasks transforms the staging data into the final
        `unified_payment_intelligence` table and then runs data quality tests.
    """
) as dag:

    # --- Task 1: Trigger the Spark Job for Data Ingestion ---
    # This task runs the Python script as a Spark job.
    # NOTE: For a true ELT pattern, the `consolidate_data.py` script should be modified
    # to write its output to staging tables in a data warehouse, not to a single CSV file.
    load_staging_data = SparkKubernetesOperator(
        task_id="load_staging_data",
        application_file="jobs/consolidate_data.yaml",
        namespace="spark-jobs",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        delete_on_termination=True,
        deferrable=False,
        doc_md="""
        #### Spark Load Task
        This task extracts data from source systems (simulated by CSVs) and loads them into
        the data warehouse's staging area.
        """
    )

    # --- Task 2: Check Source Freshness ---
    # This task runs `dbt source freshness` to check if the raw data was loaded on time by the Spark job.
    dbt_source_freshness = DbtTask(
        task_id="dbt_source_freshness",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=profile_config,
        command="source freshness",
        operator_args={
            "install_deps": True,
        },
    )

    # --- Task 3: Run dbt snapshot to capture historical changes ---
    # This task runs `dbt snapshot` to update the SCD Type 2 table before the main models run.
    dbt_snapshot = DbtTask(
        task_id="dbt_snapshot",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=profile_config,
        command="snapshot",
        operator_args={
            "install_deps": True, # Ensures dbt deps is run before execution
        },
    )

    # --- Task 4: Trigger dbt transformations and tests using Cosmos ---
    # This DbtTaskGroup will run `dbt build` by default, which includes `dbt run` and `dbt test`.
    dbt_group = DbtTaskGroup(
        group_id="dbt_transformation_and_tests",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args={
            "install_deps": True,
            # Pass variables to dbt models.
            # This allows for dynamic, incremental runs based on the DAG's execution date.
            "vars": {
                "start_date": "{{ ds }}",
                "end_date": "{{ next_ds }}"
            }
        },
    )

    # --- Task 5: Generate dbt documentation ---
    # This task runs `dbt docs generate` to compile the project's documentation.
    # The static files can then be picked up by a separate process and hosted.
    dbt_docs_generate = DbtTask(
        task_id="dbt_docs_generate",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=profile_config,
        command="docs generate",
        operator_args={
            "install_deps": True,
        },
    )

    # --- Define Overall Task Dependencies ---
    # The main transformation group must succeed before we generate docs.
    load_staging_data >> dbt_source_freshness >> dbt_snapshot >> dbt_group
    dbt_group >> dbt_docs_generate