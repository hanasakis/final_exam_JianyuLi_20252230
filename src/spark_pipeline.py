#!/usr/bin/env python3
"""
AeroSense Spark Batch Processing Pipeline.

Reads sensor events from Kafka in batch mode, validates and enriches them,
performs 5-minute windowed aggregation, and writes results to a
three-zone data lake (raw / curated / consumption).

This approach avoids Hadoop native IO issues on Windows while preserving
all the required processing logic (JSON parsing, validation, anomaly
detection, windowed aggregation, partitioned Parquet output).
"""

import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, window, year, month, dayofmonth, hour,
    mean as spark_mean, min as spark_min, max as spark_max,
    count, sum as spark_sum, lit, when, to_timestamp, expr, struct, to_json,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "localhost:9092,localhost:9093,localhost:9094"
)
KAFKA_TOPIC = "sensor-events"
DATA_LAKE_ROOT = os.environ.get("DATA_LAKE_ROOT", "/tmp/datalake")

# JSON schema for sensor events
SENSOR_SCHEMA = StructType([
    StructField("sensor",    StringType(),  True),
    StructField("value",     DoubleType(),  True),
    StructField("unit",      StringType(),  True),
    StructField("timestamp", LongType(),    True),
    StructField("source",    StringType(),  True),
    StructField("anomaly",   BooleanType(), True),
])

# Anomaly threshold expression (independent of producer flag)
ANOMALY_EXPR_STR = (
    "((sensor = 'temperature' AND value > 35.0) OR "
    "(sensor = 'humidity' AND value > 90.0) OR "
    "(sensor = 'pressure' AND (value < 990.0 OR value > 1030.0)))"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _lake_path(*parts: str) -> str:
    return os.path.join(DATA_LAKE_ROOT, *parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    spark = (
        SparkSession.builder
        .appName("AeroSense-IoT-Batch-Pipeline")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("AeroSense Spark Batch Processing Pipeline")
    print(f"  Kafka  : {KAFKA_BOOTSTRAP}")
    print(f"  Topic  : {KAFKA_TOPIC}")
    print(f"  Lake   : {DATA_LAKE_ROOT}")
    print("=" * 60)

    # ---- Read from Kafka in batch mode ------------------------------------
    kafka_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    total_kafka = kafka_df.count()
    print(f"\nKafka messages read: {total_kafka}")

    if total_kafka == 0:
        print("No messages in Kafka. Exiting.")
        spark.stop()
        return

    # ---- Parse JSON with explicit schema ----------------------------------
    parsed = kafka_df.select(
        from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data"),
        col("timestamp").alias("kafka_ts"),
    ).select("data.*", "kafka_ts")

    # Derive event_time from epoch-ms
    parsed = parsed.withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("timestamp") / 1000))
    )

    # ---- Validation -------------------------------------------------------
    validated = parsed.filter(
        ((col("sensor") == "temperature") & col("value").between(-50.0, 100.0)) |
        ((col("sensor") == "humidity")    & col("value").between(0.0, 100.0)) |
        ((col("sensor") == "pressure")    & col("value").between(700.0, 1100.0))
    )

    rejected = parsed.filter(
        ~((col("sensor") == "temperature") & col("value").between(-50.0, 100.0)) &
        ~((col("sensor") == "humidity")    & col("value").between(0.0, 100.0)) &
        ~((col("sensor") == "pressure")    & col("value").between(700.0, 1100.0))
    )
    print(f"Valid records : {validated.count()}")
    print(f"Rejected/invalid: {rejected.count()}")

    # ---- Anomaly detection (independent of producer flag) ------------------
    validated = validated.withColumn(
        "is_anomaly",
        when(expr(ANOMALY_EXPR_STR), lit(True)).otherwise(lit(False)),
    )
    total_anomalies = validated.filter(col("is_anomaly")).count()
    print(f"Anomalies detected: {total_anomalies}")

    # ---- Add partition columns (based on event_time) ----------------------
    with_parts = (
        validated
        .withColumn("ryear",  year("event_time"))
        .withColumn("rmonth", month("event_time"))
        .withColumn("rday",   dayofmonth("event_time"))
        .withColumn("rhour",  hour("event_time"))
    )

    # ---- Write Raw Zone: JSON ---------------------------------------------
    raw_path = _lake_path("raw", "source=kafka", "topic=sensor-events")
    print(f"\nWriting RAW zone → {raw_path}")
    (
        with_parts
        .write
        .mode("append")
        .format("json")
        .partitionBy("ryear", "rmonth", "rday", "rhour")
        .save(raw_path)
    )

    # ---- Write Curated Zone: Parquet (Snappy) -----------------------------
    curated_path = _lake_path("curated", "domain=iot")
    print(f"Writing CURATED zone → {curated_path}")
    (
        with_parts
        .write
        .mode("append")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("sensor", "ryear", "rmonth", "rday")
        .save(curated_path)
    )

    # ---- 5-minute windowed aggregation ------------------------------------
    print("Computing 5-minute windowed aggregates ...")
    windowed_agg = (
        validated
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("sensor").alias("sensor_type"),
        )
        .agg(
            spark_mean("value").alias("mean_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("*").alias("observation_count"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
        .withColumn("cyear",  year(col("window_start")))
        .withColumn("cmonth", month(col("window_start")))
        .drop("window")
    )

    # ---- Write Consumption Zone: Parquet ----------------------------------
    consumption_path = _lake_path("consumption", "use_case=sensor_averages")
    print(f"Writing CONSUMPTION zone → {consumption_path}")
    (
        windowed_agg
        .write
        .mode("append")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("sensor_type", "cyear", "cmonth")
        .save(consumption_path)
    )

    # ---- Summary ----------------------------------------------------------
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print(f"  Total messages ingested : {total_kafka}")
    print(f"  Valid records           : {validated.count()}")
    print(f"  Anomalies detected      : {total_anomalies}")
    print(f"  Window rows             : {windowed_agg.count()}")
    print(f"  Raw zone         → {raw_path}")
    print(f"  Curated zone     → {curated_path}")
    print(f"  Consumption zone → {consumption_path}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
