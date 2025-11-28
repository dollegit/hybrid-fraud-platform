# COMMAND ----------
from __future__ import annotations

import pendulum
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# =============================================================================
# CONFIGURATION
# =============================================================================
# In a production environment, these paths and configurations should be managed
# using Airflow Variables, Connections, and a proper Git-sync deployment.

# Define the base path for the project within the Airflow environment.
# This assumes the DAG file is in a 'dags' folder and the project is checked out alongside it.
PROJECT_ROOT = Path(__file__).parent.parent.parent # Corrected: Go up three levels to reach 'hybrid-fraud-platform'

# Adjust paths to match the project's directory structure (e.g., '3-spark-app/src')
SPARK_APP_DIR = PROJECT_ROOT / "3-spark-app" / "src"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project" # Assuming a 'dbt_project' folder exists at the root

# The directory containing the dbt `profiles.yml` file.
# It's a best practice to keep this separate from your project code.
DBT_PROFILES_DIR = DBT_PROJECT_DIR # For simplicity, assuming it's in the dbt project dir for this example

# Airflow Connection ID for the Spark cluster.
SPARK_CONN_ID = "spark_default"

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
        application_file=str(SPARK_APP_DIR / "consolidate_data.yaml"),
        namespace="spark-jobs",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        doc_md="""
        #### Spark Load Task
        This task extracts data from source systems (simulated by CSVs) and loads them into
        the data warehouse's staging area.
        """
    )

    # --- Task Group for dbt ---
    # Using a TaskGroup helps organize the UI for related tasks.
    with TaskGroup(group_id="dbt_transformation_and_tests") as dbt_group:
        # Task 2: Trigger `dbt run`
        transform_with_dbt = BashOperator(
            task_id='transform_with_dbt',
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        )

        # Task 3: Trigger `dbt test`
        test_with_dbt = BashOperator(
            task_id='test_with_dbt',
            bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        )

        # Define dependencies within the dbt TaskGroup
        transform_with_dbt >> test_with_dbt

    # --- Define Overall Task Dependencies ---
    # The Spark job must complete successfully before the dbt tasks can begin.
    load_staging_data >> dbt_group