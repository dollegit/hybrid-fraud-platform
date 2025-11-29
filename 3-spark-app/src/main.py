# COMMAND ----------
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def main():
    """
    Main ETL script for processing payment data from Kafka to MinIO.
    """
    # --- Configuration ---
    kafka_bootstrap_servers = "my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
    kafka_topic = "payment_events"
    minio_endpoint = "http://minio.storage.svc.cluster.local:9000"
    minio_access_key = "minio"
    minio_secret_key = "minio123"
    bronze_path = "s3a://datalake/bronze/payments"
    silver_path = "s3a://datalake/silver/transactions"

    # --- Spark Session Initialization ---
    spark = (
        SparkSession.builder.appName("OnPremPaymentETL")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.streaming.checkpointLocation", "s3a://spark-logs/checkpoints")
        .config("spark.kubernetes.driver.volumes.configMap.spark_job_script.defaultMode", "420")
        .config("spark.kubernetes.executor.volumes.configMap.spark_job_script.defaultMode", "420")
        .getOrCreate()
    )
    print("Spark Session created successfully.")

    # --- Read from Kafka (Bronze Layer) ---
    print(f"Reading from Kafka topic: {kafka_topic}")
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Define schema for the incoming JSON data
    payment_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
    ])

    # Parse the JSON data from the Kafka 'value' column
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), payment_schema).alias("data")
    ).select("data.*")

    # --- Write to Bronze Layer (Raw Data as Parquet) ---
    # Adding a processing timestamp for partitioning
    bronze_df = parsed_df.withColumn("processing_ts", current_timestamp())

    bronze_writer = (
        bronze_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", bronze_path)
        .option("checkpointLocation", "/tmp/checkpoints/bronze")
        .partitionBy("processing_ts")
        .start()
    )
    print(f"Writing raw data to Bronze layer at: {bronze_path}")

    # --- Simple Transformation for Silver Layer ---
    # Example: Add a risk score placeholder
    silver_df = parsed_df.withColumn("risk_score", col("amount") * 0.01)

    silver_writer = (
        silver_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", silver_path)
        .option("checkpointLocation", "/tmp/checkpoints/silver")
        .start()
    )
    print(f"Writing transformed data to Silver layer at: {silver_path}")

    # Wait for streams to terminate
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()