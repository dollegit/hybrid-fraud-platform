# COMMAND ----------
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def main():
    """
    Main ETL script for processing payment data from Kafka to MinIO.
    Uses SparkApplication sparkConf + Kubernetes env vars.
    """
    # --- USE KUBERNETES ENV VARS ---
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092")
    kafka_topic = "payment_events"
    
    # ✅ S3A paths - sparkConf handles endpoint/auth automatically!
    bronze_path = "s3a://datalake/bronze/payments"
    silver_path = "s3a://datalake/silver/transactions"
    bronze_checkpoint_path = "s3a://spark-logs/checkpoints/bronze"
    silver_checkpoint_path = "s3a://spark-logs/checkpoints/silver"

    # --- SPARK SESSION: NO S3A CONFIGS (already in sparkConf) ---
    spark = SparkSession.builder \
        .appName("OnPremPaymentETL") \
        .getOrCreate()
    
    print("✅ Spark Session created (S3A pre-configured by SparkApplication)")
    print(f"✅ Kafka servers: {kafka_bootstrap_servers}")

    # --- Read from Kafka (Bronze Layer) ---
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Schema (unchanged)
    payment_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
    ])

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), payment_schema).alias("data")
    ).select("data.*")

    # --- Bronze Layer (Raw) ---
    bronze_df = parsed_df.withColumn("processing_ts", current_timestamp())
    bronze_writer = (
        bronze_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", bronze_path)  # ✅ S3A works automatically
        .option("checkpointLocation", bronze_checkpoint_path)
        .partitionBy("processing_ts")
        .start()
    )
    print(f"✅ Bronze streaming to: {bronze_path}")

    # --- Silver Layer (Transformed) ---
    silver_df = parsed_df.withColumn("risk_score", col("amount") * 0.01)
    silver_writer = (
        silver_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", silver_path)
        .option("checkpointLocation", silver_checkpoint_path)
        .start()
    )
    print(f"✅ Silver streaming to: {silver_path}")

    # Keep streaming
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
