import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType, DateType

def load_to_staging(spark, source_file, table_name, schema, jdbc_url, connection_properties):
    """Reads a CSV from the shared volume and writes it to a PostgreSQL table."""
    
    # Use an environment variable for the base path, defaulting to /opt/spark/data
    input_dir = os.getenv("DATA_INPUT_PATH", "/opt/spark/data")
    source_path = os.path.join(input_dir, source_file)
    
    print(f"📖 Reading {source_file} from {source_path} -> into table {table_name}")
    
    try:
        # Use the provided schema instead of inferring it.
        # This is more robust and prevents errors with empty files or incorrect types.
        df = (
            spark.read
            .schema(schema)
            .option("header", "true")
            .csv(source_path)
        )
        
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=connection_properties
        )
        print(f"✅ SUCCESS {table_name}: Wrote {df.count()} rows.")
        
    except Exception as e:
        print(f"❌ FAILED {table_name}: {e}")
        # Re-raise the exception to make the Spark job fail
        raise e

def main():
    """
    Main entry point for the data consolidation job.
    Reads CSVs from the shared PVC and loads them into a staging PostgreSQL database.
    """
    spark = SparkSession.builder.appName("ConsolidateData").getOrCreate()

    # Get database connection details from environment variables
    jdbc_url = os.getenv("STAGING_DB_URL")
    db_user = os.getenv("STAGING_DB_USER")
    db_password = os.getenv("STAGING_DB_PASSWORD")

    if not all([jdbc_url, db_user, db_password]):
        raise ValueError("Missing one or more required database environment variables (STAGING_DB_URL, STAGING_DB_USER, STAGING_DB_PASSWORD)")

    connection_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    # Define explicit schemas for each source file to ensure data integrity.
    schemas = {
        "raw_payments": StructType([
            StructField("payment_id", StringType(), True),
            StructField("source_account_id", StringType(), True),
            StructField("destination_account_id", StringType(), True),
            StructField("payment_reference", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("payment_timestamp", TimestampType(), True),
        ]),
        "raw_accounts": StructType([
            StructField("account_id", StringType(), True),
            StructField("opening_date", DateType(), True),
        ]),
        "raw_risk_feed": StructType([
            StructField("account_id", StringType(), True),
            StructField("risk_score", DoubleType(), True),
        ]),
        "raw_fraud_cases": StructType([
            StructField("payment_id", StringType(), True),
            StructField("is_fraud", BooleanType(), True),
            StructField("fraud_type", StringType(), True),
            StructField("fraud_reported_date", DateType(), True),
        ])
    }

    # Define the mapping of source files to destination table names
    files_to_tables = {
        "payment_transactions.csv": "raw_payments",
        "account_details.csv": "raw_accounts",
        "risk_feed.csv": "raw_risk_feed",
        "fraud_cases.csv": "raw_fraud_cases"
    }

    for source_file, table_name in files_to_tables.items():
        load_to_staging(spark, source_file, table_name, schemas[table_name], jdbc_url, connection_properties)

    print("🛑 Stopping SparkSession.")
    spark.stop()

if __name__ == "__main__":
    main()