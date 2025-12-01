# COMMAND ----------
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def process_micro_batch(df: DataFrame, epoch_id: int, bronze_path: str, silver_path: str):
    """Processes one micro-batch: writes to bronze and silver layers."""
    print(f"--- Processing micro-batch {epoch_id} ---")
    # Write to Bronze (with processing timestamp)
    df.withColumn("processing_ts", current_timestamp()).write.format("parquet").mode("append").save(bronze_path)
    # Write to Silver (with risk score)
    df.withColumn("risk_score", col("amount") * 0.01).write.format("parquet").mode("append").save(silver_path)

def main():
    """
    Main ETL script for processing payment data from Kafka to MinIO.
    Uses SparkApplication sparkConf + Kubernetes env vars.
    """
    # --- USE KUBERNETES ENV VARS ---
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "payment-events")

    # ✅ S3A paths - sparkConf handles endpoint/auth automatically!
    bronze_path = "s3a://datalake/bronze/payments"
    silver_path = "s3a://datalake/silver/transactions"
    bronze_checkpoint_path = "s3a://spark-logs/checkpoints/bronze"

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
        StructField("user_name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
    ])

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), payment_schema).alias("data")
    ).select("data.*")

    # --- Process and Write using foreachBatch ---
    # This is the recommended way to write to multiple locations from one stream.
    query = (
        parsed_df.writeStream
        .foreachBatch(lambda df, epoch_id: process_micro_batch(df, epoch_id, bronze_path, silver_path))
        .option("checkpointLocation", bronze_checkpoint_path)
        .trigger(processingTime="1 minute")
        .start()
    )

    print(f"✅ Streaming started. Writing to Bronze ({bronze_path}) and Silver ({silver_path}).")
    query.awaitTermination()

if __name__ == "__main__":
    main()
