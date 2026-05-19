#!/usr/bin/env python3
"""
AeroSense Analytical Queries.

Runs Spark SQL queries on the data lake and demonstrates partition pruning.
Results are printed to stdout and written as CSV files in outputs/analytics/.
"""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, count, sum as spark_sum,
    mean as spark_mean, min as spark_min, max as spark_max, stddev,
    when, lit, desc,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_LAKE_ROOT = os.environ.get("DATA_LAKE_ROOT", "/tmp/datalake")
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "outputs", "analytics",
)
os.makedirs(OUTPUT_DIR, exist_ok=True)

CURATED_PATH = os.path.join(DATA_LAKE_ROOT, "curated", "domain=iot")
CONSUMPTION_PATH = os.path.join(DATA_LAKE_ROOT, "consumption", "use_case=sensor_averages")


def _write_csv(df, filename: str) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    print(f"  → CSV saved: {path}\n")


# ---------------------------------------------------------------------------
def main():
    spark = (
        SparkSession.builder
        .appName("AeroSense-Analytics")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    curated = spark.read.parquet(CURATED_PATH)
    consumption = spark.read.parquet(CONSUMPTION_PATH)

    print("=" * 70)
    print("AEROSENSE ANALYTICS REPORT")
    print("=" * 70)

    # ------------------------------------------------------------------
    # Query 1: Top 5 hours with the highest number of anomalies
    # ------------------------------------------------------------------
    print("\n" + "─" * 70)
    print("[Q1] Top 5 hours with the highest number of anomalies")
    print("─" * 70)

    q1 = (
        curated
        .filter(col("is_anomaly") == True)
        .groupBy("year", "month", "day", "hour")
        .agg(count("*").alias("anomaly_count"))
        .orderBy(desc("anomaly_count"))
        .limit(5)
    )
    q1.show(truncate=False)
    _write_csv(q1, "q1_top_anomaly_hours")

    # ------------------------------------------------------------------
    # Query 2: Per sensor type — global mean, min, max, stddev, anomaly rate (%)
    # ------------------------------------------------------------------
    print("─" * 70)
    print("[Q2] Per sensor type: global statistics and anomaly rate")
    print("─" * 70)

    totals = curated.groupBy("sensor").agg(count("*").alias("total_count"))

    q2 = (
        curated
        .groupBy("sensor")
        .agg(
            spark_mean("value").alias("mean_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            stddev("value").alias("stddev_value"),
            spark_sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("anomaly_count"),
        )
        .join(totals, "sensor")
        .withColumn("anomaly_rate_pct",
                     (col("anomaly_count") / col("total_count") * 100).cast("decimal(6,2)"))
        .select("sensor", "mean_value", "min_value", "max_value",
                "stddev_value", "anomaly_count", "total_count", "anomaly_rate_pct")
        .orderBy("sensor")
    )
    q2.show(truncate=False)
    _write_csv(q2, "q2_sensor_statistics")

    # ------------------------------------------------------------------
    # Query 3: Daily evolution of mean value and anomaly count for temperature
    # ------------------------------------------------------------------
    print("─" * 70)
    print("[Q3] Daily evolution: temperature sensor")
    print("─" * 70)

    q3 = (
        curated
        .filter(col("sensor") == "temperature")
        .groupBy("year", "month", "day")
        .agg(
            spark_mean("value").alias("mean_temperature"),
            spark_sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("anomaly_count"),
            count("*").alias("observation_count"),
        )
        .orderBy("year", "month", "day")
    )
    q3.show(30, truncate=False)
    _write_csv(q3, "q3_temperature_daily")

    # ------------------------------------------------------------------
    # Query 4: Partition pruning demonstration
    # ------------------------------------------------------------------
    print("─" * 70)
    print("[Q4] Partition pruning demonstration")
    print("─" * 70)

    # Count all rows (full scan)
    t0 = time.time()
    count_all = curated.count()
    t_all = time.time() - t0
    print(f"  Full scan count: {count_all} rows")
    print(f"  Execution time : {t_all:.4f} s")

    # Count with partition filter on sensor + year
    t0 = time.time()
    count_filtered = curated.filter(
        (col("sensor") == "temperature") &
        (col("year") == col("year"))  # forces no actual filter, just for demo
    ).count()
    t_filtered = time.time() - t0

    # Real pruning: filter on sensor partition column
    t0 = time.time()
    count_pruned = curated.filter(col("sensor") == "temperature").count()
    t_pruned = time.time() - t0
    print(f"  Pruned count (sensor=temperature): {count_pruned} rows")
    print(f"  Execution time : {t_pruned:.4f} s")

    # Filter on both sensor and date partition
    t0 = time.time()
    year_val = curated.select("year").first()[0] if count_all > 0 else 2026
    month_val = curated.select("month").first()[0] if count_all > 0 else 5
    count_multi = curated.filter(
        (col("sensor") == "temperature") &
        (col("year") == year_val) &
        (col("month") == month_val)
    ).count()
    t_multi = time.time() - t0
    print(f"  Multi-pruned count (sensor=temperature, year={year_val}, month={month_val}): {count_multi} rows")
    print(f"  Execution time : {t_multi:.4f} s")

    if t_pruned > 0:
        speedup = t_all / t_pruned
        print(f"\n  Speedup factor (full vs single filter): {speedup:.2f}x")
    if t_multi > 0:
        speedup_multi = t_all / t_multi
        print(f"  Speedup factor (full vs multi filter) : {speedup_multi:.2f}x")

    # Write pruning results to CSV
    pruning_df = spark.createDataFrame(
        [("full_scan", "", count_all, round(t_all, 4)),
         ("sensor_filter", "sensor=temperature", count_pruned, round(t_pruned, 4)),
         ("multi_filter", f"sensor=temperature,year={year_val},month={month_val}", count_multi, round(t_multi, 4))],
        schema=["query_type", "filters", "row_count", "execution_time_s"],
    )
    _write_csv(pruning_df, "q4_partition_pruning")

    print("=" * 70)
    print("Analytics complete. CSV files written to outputs/analytics/")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
