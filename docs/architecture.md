# Architecture Documentation

## Platform Overview

The AeroSense IoT Data Platform ingests high-frequency sensor readings (temperature, humidity, pressure) from industrial sites, processes them through a streaming pipeline, stores them in a three-zone data lake, and exposes results via a REST API.

## Pipeline Diagram

```
+------------------+       +---------------------------+       +----------------------------+
|  Python Producer | ----> |  Kafka Cluster (3 br.)    | ----> |  Spark Structured Streaming|
|  (producer.py)   |       |  topic: sensor-events     |       |  (spark_pipeline.py)       |
|                  |       |  3 partitions, RF=3       |       |                            |
+------------------+       +---------------------------+       +-------------+--------------+
                                                                             |
                                                                     +-------+-------+
                                                                     |  Data Lake    |
                                                                     |  (Parquet)    |
                                                                     +-------+-------+
                                                                             |
                                              +------------------------------+---+
                                              |              |                   |
                                        Raw Zone      Curated Zone      Consumption Zone
                                        (JSON)        (Parquet)         (Parquet)
                                              |              |                   |
                                              +--------------+-------------------+
                                                             |
                                                    +--------+--------+
                                                    |  Spark SQL      |
                                                    |  (analytics.py) |
                                                    +--------+--------+
                                                             |
                                                    +--------+--------+
                                                    |  Flask REST API |
                                                    |  (app.py)       |
                                                    +-----------------+
```

## Component Description

### 1. Kafka Cluster (Infrastructure)
- **3 brokers** in KRaft mode (no ZooKeeper dependency).
- **Replication factor 3**, **min.insync.replicas 2** for fault tolerance.
- **Kafka UI** on port 8080 for monitoring.
- Topic `sensor-events` with 3 partitions, keyed by sensor type.

### 2. Python Producer
- Generates realistic sensor data within known physical ranges.
- ~12% of readings are deliberately out-of-threshold for anomaly testing.
- Configured with `acks=all`, `retries=5`, idempotent ordering.
- Key-based partitioning ensures per-sensor-type ordering.

### 3. Spark Structured Streaming Pipeline
- Reads from Kafka in streaming mode.
- Parses JSON with an explicit schema.
- Validates readings against physical plausibility ranges.
- Detects anomalies independently of the producer flag.
- Computes 5-minute windowed aggregates with a 2-minute watermark.
- Writes to three data lake zones (raw, curated, consumption).

### 4. Data Lake (Three Zones)
| Zone        | Format | Partitioning                          | Purpose                        |
|-------------|--------|---------------------------------------|--------------------------------|
| Raw         | JSON   | year/month/day/hour                   | Immutable ingestion record     |
| Curated     | Parquet| sensor/year/month/day                  | Cleaned, validated, queryable  |
| Consumption | Parquet| sensor_type/year/month                | Pre-aggregated for dashboards  |

### 5. Analytics (Spark SQL)
- Batch queries on the curated and consumption zones.
- Top-N anomaly hours, per-sensor statistics, daily evolution.
- Partition pruning benchmark with quantified speedup.

### 6. REST API (Flask)
- Six endpoints (health, sensors, latest, stats, anomalies, POST reading).
- Consistent JSON responses with proper HTTP status codes.
- Input validation with 400/422 distinction.
- Integration with both Kafka (live reads/writes) and Parquet (historical stats).
