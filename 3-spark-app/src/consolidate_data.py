# COMMAND ----------
import os
from pyspark.sql import SparkSession

def load_to_staging(spark, source_path, table_name, jdbc_url, connection_properties):
    """
    Reads CSV from container and loads to PostgreSQL staging table.
    """
    try:
        print(f"📖 Reading {source_path} → {table_name}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)

        print(f"💾 Loading to PostgreSQL: {table_name}")
        df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", connection_properties["user"]) \
            .option("password", connection_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .save()
            
        print(f"✅ {table_name}: {df.count()} rows loaded")
    except Exception as e:
        print(f"❌ FAILED {table_name}: {e}")
        raise

def main():
    """
    Load CSV files → PostgreSQL staging → Ready for dbt.
    """
    # ✅ PERFECT - Uses Kubernetes env vars
    jdbc_hostname = os.getenv("DB_HOSTNAME", "postgres.airflow.svc.cluster.local")
    jdbc_port = os.getenv("DB_PORT", "5432")
    jdbc_database = os.getenv("DB_NAME", "fraud_db")
    jdbc_url = f"jdbc:postgresql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

    connection_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    # ✅ Source files in custom image /opt/spark/work-dir/
    sources_to_tables = {
        "payment_transactions.csv": "stg_payments",
        "account_details.csv": "stg_accounts", 
        "external_risk_feed.csv": "stg_risk_feed",
        "historical_fraud_cases.csv": "stg_fraud_cases"
    }

    spark = SparkSession.builder \
        .appName("FraudDetection_LoadToStaging") \
        .getOrCreate()

    for source_file, table_name in sources_to_tables.items():
        load_to_staging(spark, source_file, table_name, jdbc_url, connection_properties)

    print("🎉 All staging tables loaded → Ready for dbt!")
    spark.stop()

if __name__ == "__main__":
    main()
