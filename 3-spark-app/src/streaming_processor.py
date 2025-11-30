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
    Main ETL script for processing payment data from Kafka to MinIO in a streaming fashion.
    """
    # --- Configuration ---
    # In a real environment, these should be managed via environment variables,
    # Kubernetes secrets, or another secure configuration method.
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "payment_events")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio.storage.svc.cluster.local:9000")
    output_path = os.getenv("OUTPUT_PATH", "s3a://bronze/streaming_payments")
    checkpoint_location = os.getenv("CHECKPOINT_LOCATION", "s3a://bronze/checkpoints/streaming_payments_checkpoint")

    # --- Spark Session ---
    spark = SparkSession.builder.appName("PaymentStreamingProcessor").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark Session created. Starting stream processing...")
    print(f"Kafka Topic: {kafka_topic}")
    print(f"Output Path: {output_path}")

    # --- Schema Definition ---
    # Define the schema for the incoming JSON data from Kafka
    payment_schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
        ]
    )

    # --- Read from Kafka ---
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # --- Transformation ---
    # Parse the JSON message from Kafka and apply the schema
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), payment_schema).alias("data")
    ).select("data.*")

    # Add a processing timestamp for auditing
    enriched_df = parsed_df.withColumn("processing_timestamp", current_timestamp())

    # --- Write to MinIO ---
    # Write the stream to MinIO in Parquet format, partitioned by date
    query = (
        enriched_df.writeStream.format("parquet")
        .outputMode("append")
        .path(output_path)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="1 minute")  # Process data every minute
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()