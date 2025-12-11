# COMMAND ----------
from __future__ import annotations

import os
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def process_micro_batch(df: DataFrame, epoch_id: int, output_path: str):
    """
    Processes a single micro-batch: writes the DataFrame to a Parquet file.
    This demonstrates using a standard batch writer within a streaming query.
    """
    print(f"--- Processing micro-batch {epoch_id} ---")
    df.write.format("parquet").mode("append").save(output_path)


def _parse_s3a_bucket(path: str) -> str:
    """
    Extracts the bucket name from an s3a:// URL.
    Example: s3a://bronze/streaming_payments -> bronze
    """
    parsed = urlparse(path)
    if parsed.scheme != "s3a":
        raise ValueError(f"Expected s3a:// URL, got: {path}")
    # parsed.netloc holds the bucket
    return parsed.netloc


def _assert_bucket_exists(spark: SparkSession, s3a_path: str):
    """
    Fail fast with a clear error if the target S3A bucket does not exist.

    Spark/Hadoop S3A will happily create prefixes under an existing bucket,
    but will raise NoSuchBucket if the bucket itself is missing.
    """
    bucket = _parse_s3a_bucket(s3a_path)
    print(f"🔍 Checking S3A bucket existence: {bucket!r} (from {s3a_path})")

    # Try a trivial list operation on the bucket prefix; this will surface
    # NoSuchBucket immediately if the bucket is missing.
    try:
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        path = f"s3a://{bucket}/"
        # Use Java FileSystem to trigger a HEAD/list on the bucket
        jvm = spark._jvm
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.org.apache.hadoop.fs.Path(path).toUri(),
            spark._jsc.hadoopConfiguration()
        )
        fs.listStatus(jvm.org.apache.hadoop.fs.Path(path))
        print(f"✅ S3A bucket '{bucket}' is reachable")
    except Exception as e:
        # This will typically wrap an AmazonS3Exception NoSuchBucket if the bucket is missing
        raise RuntimeError(
            f"S3A bucket '{bucket}' does not exist or is not reachable. "
            f"Create it in MinIO (e.g. `mc mb local/{bucket}`) before running this job."
        ) from e


def main():
    """
    Payment Streaming Processor - Uses SparkApplication sparkConf + env vars ONLY.
    """
    # --- ENV VARS ONLY (NO S3A configs) ---
    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092",
    )
    kafka_topic = os.getenv("KAFKA_TOPIC", "paymentevents")

    # ✅ S3A paths - sparkConf handles ALL endpoint/auth!
    output_path = os.getenv("OUTPUT_PATH", "s3a://bronze/streaming_payments")
    checkpoint_location = os.getenv(
        "CHECKPOINT_LOCATION",
        "s3a://bronze/checkpoints/streaming_payments_checkpoint",
    )

    # --- SPARK SESSION: NO S3A CONFIGS ---
    spark = (
        SparkSession.builder.appName("PaymentStreamingProcessor").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("✅ Spark Session created (S3A from sparkConf)")
    print(f"✅ Kafka: {kafka_bootstrap_servers}/{kafka_topic}")
    print(f"✅ Output: {output_path}")
    print(f"✅ Checkpoint: {checkpoint_location}")

    # 🔍 Validate that the S3A bucket exists, fail fast if not
    _assert_bucket_exists(spark, output_path)
    _assert_bucket_exists(spark, checkpoint_location)

    # --- Schema (unchanged) ---
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

    enriched_df = parsed_df.withColumn(
        "processing_timestamp", current_timestamp()
    )

    # --- Stream to S3A using foreachBatch for micro-batch processing ---
    query = (
        enriched_df.writeStream.outputMode("append")
        .foreachBatch(
            lambda df, epoch_id: process_micro_batch(df, epoch_id, output_path)
        )
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="1 minute")
        .start()
    )

    print("🚀 Streaming processor LIVE!")
    query.awaitTermination()


if __name__ == "__main__":
    main()
