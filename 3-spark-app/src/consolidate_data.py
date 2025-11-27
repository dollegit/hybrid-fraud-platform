import os
from pyspark.sql import SparkSession

def load_to_staging(spark, source_path, table_name, jdbc_url, connection_properties):
    """
    Reads a CSV file from a source path and loads it into a staging table in a data warehouse.

    Args:
        spark (SparkSession): The active Spark session.
        source_path (str): The file path for the source CSV.
        table_name (str): The name of the target staging table.
        jdbc_url (str): The JDBC URL for the data warehouse.
        connection_properties (dict): A dictionary of JDBC connection properties.
    """
    try:
        print(f"Reading data from {source_path}...")
        df = spark.read.csv(source_path, header=True, inferSchema=True)

        print(f"Loading data into staging table: {table_name}...")
        # Use "overwrite" mode to ensure staging tables are refreshed on each run.
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=connection_properties
        )
        print(f"Successfully loaded data into {table_name}.")
    except FileNotFoundError as e:
        print(f"ERROR: Source file not found at {source_path}. Please run generate_sample_data.py first.")
        raise e

def main():
    """
    Main ETL script for the Extract and Load phases.
    This script reads raw data from CSV files and loads them into staging tables
    in a data warehouse, preparing them for dbt transformation.
    """
    spark = SparkSession.builder.appName("FraudDetection_LoadToStaging").getOrCreate()

    # --- Configuration ---
    # In a real environment, these should be managed via environment variables,
    # Kubernetes secrets, or another secure configuration method.
    jdbc_hostname = os.getenv("DB_HOSTNAME", "localhost")
    jdbc_port = os.getenv("DB_PORT", 5432) # Example for PostgreSQL
    jdbc_database = os.getenv("DB_NAME", "fraud_db")
    jdbc_url = f"jdbc:postgresql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

    connection_properties = {
        "user": os.getenv("DB_USER", "user"),
        "password": os.getenv("DB_PASSWORD", "password"),
        "driver": "org.postgresql.Driver"
    }

    # Define source files and target staging tables
    # Assumes the script is run from the `3-spark-app/src` directory where the CSVs are generated.
    sources_to_tables = {
        "payment_transactions.csv": "stg_payments",
        "account_details.csv": "stg_accounts",
        "external_risk_feed.csv": "stg_risk_feed",
        "historical_fraud_cases.csv": "stg_fraud_cases"
    }

    for source_file, table_name in sources_to_tables.items():
        load_to_staging(spark, source_file, table_name, jdbc_url, connection_properties)

    print("\nAll source data has been successfully loaded into staging tables.")
    spark.stop()

if __name__ == "__main__":
    main()