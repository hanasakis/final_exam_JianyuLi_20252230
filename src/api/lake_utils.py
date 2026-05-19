"""
Data lake query helpers for the REST API.
"""

import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

DATA_LAKE_ROOT = os.environ.get("DATA_LAKE_ROOT", "/tmp/datalake")
CURATED_PATH = os.path.join(DATA_LAKE_ROOT, "curated", "domain=iot")
CONSUMPTION_PATH = os.path.join(DATA_LAKE_ROOT, "consumption", "use_case=sensor_averages")

_spark = None


def _get_spark() -> SparkSession:
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder
            .appName("AeroSense-API")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.adaptive.enabled", "true")
            .master("local[2]")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def daily_stats(sensor_type: str, days: int) -> DataFrame | None:
    """Return daily aggregated stats for *sensor_type* over the last *days* days."""
    try:
        df = _get_spark().read.parquet(CURATED_PATH)
    except Exception:
        return None

    filtered = df.filter(col("sensor") == sensor_type)
    if filtered.rdd.isEmpty():
        return None

    from pyspark.sql.functions import (
        mean, min, max, stddev, count, sum as spark_sum, expr, lit,
    )

    result = (
        filtered
        .groupBy("year", "month", "day")
        .agg(
            mean("value").alias("mean_value"),
            min("value").alias("min_value"),
            max("value").alias("max_value"),
            stddev("value").alias("stddev_value"),
            count("*").alias("observation_count"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        .orderBy("year", "month", "day")
    )
    # Apply days limit
    if days > 0:
        result = result.limit(days)
    return result


def recent_anomalies(sensor_type: str | None, limit: int) -> DataFrame | None:
    """Return recent anomaly records, optionally filtered by sensor type."""
    try:
        df = _get_spark().read.parquet(CURATED_PATH)
    except Exception:
        return None

    result = df.filter(col("is_anomaly") == True)
    if sensor_type:
        result = result.filter(col("sensor") == sensor_type)

    return result.orderBy(col("event_time").desc()).limit(limit)
