# COMMAND ----------
from __future__ import annotations

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

def main():
    """
    Payment Streaming Processor - Uses SparkApplication sparkConf + env vars ONLY.
    """
    # --- ENV VARS ONLY (NO S3A configs) ---
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "paymentevents")
    
    # ✅ S3A paths - sparkConf handles ALL endpoint/auth!
    output_path = os.getenv("OUTPUT_PATH", "s3a://bronze/streaming_payments")
    checkpoint_location = os.getenv("CHECKPOINT_LOCATION", "s3a://bronze/checkpoints/streaming_payments_checkpoint")

    # --- SPARK SESSION: NO S3A CONFIGS ---
    spark = SparkSession.builder \
        .appName("PaymentStreamingProcessor") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("✅ Spark Session created (S3A from sparkConf)")
    print(f"✅ Kafka: {kafka_bootstrap_servers}/{kafka_topic}")
    print(f"✅ Output: {output_path}")

    # --- Schema (unchanged) ---
    payment_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
    ])

    # --- Kafka → Parse → Enrich ---
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), payment_schema).alias("data")
    ).select("data.*")

    enriched_df = parsed_df.withColumn("processing_timestamp", current_timestamp())

    # --- Stream to S3A (sparkConf handles auth) ---
    query = (
        enriched_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)  # ✅ .path() not .path
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="1 minute")
        .start()
    )

    print("🚀 Streaming processor LIVE!")
    query.awaitTermination()

if __name__ == "__main__":
    main()