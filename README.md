# AeroSense IoT Data Engineering Platform

**Student:** JianyuLi  
**Student ID:** 20252230  
**Date:** 2026-05-19  
**Course:** Data Engineering — XICS404  
**Institution:** EFREI Paris, School of Engineering and Computer Science

---

## 1. Overview

### Objectives

Design and implement an end-to-end Big Data platform for industrial IoT sensor monitoring. The platform covers the full data lifecycle: **Generation → Ingestion (Kafka) → Processing (Spark Structured Streaming) → Storage (Data Lake) → Exposure (REST API) → Consumption (Analytics)**.

### Scope

AeroSense deploys IoT sensors across industrial sites to collect three physical measurements: **temperature** (°C), **humidity** (%), and **atmospheric pressure** (hPa). Each sensor emits readings at high frequency. The platform provides:

- Reliable, fault-tolerant real-time ingestion via a 3-broker Kafka cluster.
- Streaming processing with anomaly detection and windowed aggregation.
- A three-zone data lake (raw / curated / consumption) on local storage.
- Analytical queries via Spark SQL with partition pruning.
- A REST API exposing sensor data for consuming applications.

### Technologies Used

| Component          | Technology                     |
|--------------------|--------------------------------|
| Message Broker     | Apache Kafka 3.5 (Confluent 7.5) |
| Cluster Mode       | KRaft (no ZooKeeper)           |
| Streaming Engine   | PySpark 3.5.3 (Structured Streaming) |
| Storage Format     | Parquet (Snappy), JSON         |
| API Framework      | Flask 3.1                      |
| Kafka Client       | kafka-python-ng 2.2.3          |
| Container Runtime  | Docker Compose v2              |
| Language           | Python 3.9+                    |

---

## 2. Architecture

```
                       +---------------------------+
                       |   Docker Compose Cluster  |
                       |   kafka1  kafka2  kafka3  |
                       |   (KRaft mode, RF=3)      |
                       +------------+--------------+
                                    |
                    Kafka Topic: sensor-events
                    (3 partitions, key=sensor_type)
                                    |
        +---------------------------+---------------------------+
        |                           |                           |
  [producer.py]            [spark_pipeline.py]           [Kafka UI :8080]
   Python Producer          Spark Structured Streaming
   - random sensor data     - parse JSON
   - ~12% anomalies         - validate & detect anomalies
   - acks=all, retries=5    - 5-min windowed aggregation
   - key-based partition    - 2-min watermark
                            - 3-zone data lake write
                                    |
                    +---------------+---------------+---------------+
                    |               |               |
                 Raw Zone     Curated Zone    Consumption Zone
                 /tmp/datalake/raw/  /curated/  /consumption/
                 (JSON)        (Parquet)       (Parquet)
                    |               |               |
                    +-------+-------+-------+
                            |
                    [analytics.py]
                     Spark SQL batch queries
                     - Top-N anomaly hours
                     - Sensor statistics
                     - Partition pruning demo
                            |
                    [Flask REST API :5000]
                     GET /health, /sensors, /latest, /stats
                     GET /anomalies
                     POST /readings
```

### Component Details

See [docs/architecture.md](docs/architecture.md) for a detailed description of each component.

---

## 3. Instructions

### 3.1 Prerequisites

- **Docker** 20.10+ with Docker Compose v2
- **Python** 3.9+ with pip
- **Java** 8/11 (for PySpark)
- **8 GB** free RAM (Docker + Spark)
- **Git** (for submission)

### 3.2 Installation

```bash
# 1. Clone the repository
git clone git@github.com:hanasakis/final_exam_JianyuLi_20252230.git
cd final_exam_JianyuLi_20252230

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Start the Kafka cluster
docker compose up -d

# 4. Wait for all brokers to be healthy (~30-60 seconds)
docker ps  # verify kafka1, kafka2, kafka3, kafka-ui are running

# 5. Create the sensor-events topic
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create \
  --topic sensor-events \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2

# 6. Verify the topic
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

### 3.3 Step-by-Step Execution

```bash
# --- Step 1: Run the producer (generate 1000 events at 50/s) ---
python src/producer.py --count 1000 --rate 50 --source site-A-rack-12

# --- Step 2: Start the Spark streaming pipeline (in a separate terminal) ---
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  src/spark_pipeline.py

# Let the pipeline run for at least 2-3 minutes to process data.
# Press Ctrl+C to stop.

# --- Step 3: Run the analytical queries ---
spark-submit src/analytics.py

# --- Step 4: Start the REST API (in a separate terminal) ---
python src/api/app.py

# --- Step 5: Test the API endpoints ---
bash tests/test_curl_commands.sh
```

### 3.4 Quick Verification

| Check | Command |
|-------|---------|
| Kafka UI | Open http://localhost:8080 |
| Cluster health | `docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --list` |
| Topic description | `docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events` |
| API health | `curl -s http://localhost:5000/api/v1/health` |
| Data lake | `ls -R /tmp/datalake/` |

---

## 4. Technical Choices

### 4.1 Partitioning Strategy for the Curated Zone

**Choice:** Partition by `sensor / year / month / day` based on event time.

**Why:** Sensor type is the primary query filter (most analytical queries target a specific sensor). Date-based partitioning enables efficient time-range scans and partition pruning. Day granularity balances partition count (not too many small files, not too few large ones). Event time (rather than ingestion time) ensures data lands in the correct partition regardless of when it was processed, which is critical for late-arriving data from unreliable IoT networks.

**Alternatives considered:** Hive-style partitioning with ingestion time would be simpler but breaks correctness for late data. Partitioning only by sensor would create massive partitions that grow unbounded, hurting query performance.

### 4.2 Spark Structured Streaming `outputMode`

**Choice:** `append` for raw and curated zones, `update` for consumption zone.

**Why:** The raw and curated sinks only ever insert new records — they never modify existing ones — so `append` is semantically correct and most efficient. The consumption zone uses `update` because windowed aggregations with a watermark can produce revised results for the most recent window as late events arrive; `update` overwrites the previous result for that window rather than appending a duplicate. `complete` mode would require retaining and re-emitting the full result table on every trigger, which is unnecessary and would grow unbounded.

### 4.3 Replication Factor and min.insync.replicas

**Choice:** `replication.factor=3`, `min.insync.replicas=2`.

**Why:** With 3 brokers, RF=3 means every partition is fully replicated across the cluster, tolerating up to 2 broker failures without data loss. Setting `min.insync.replicas=2` means a producer with `acks=all` requires acknowledgment from at least 2 replicas, including the leader. This provides a strong durability guarantee (data is on at least 2 disks) while tolerating 1 broker failure without blocking writes. The trade-off is slightly higher write latency vs. RF=2, but the durability gain justifies it for industrial monitoring where data loss is unacceptable.

**Alternatives considered:** RF=2 with min.isr=1 would reduce storage and write latency by 33% but would not survive a single disk failure.

### 4.4 Event Time vs. Ingestion Time Across Zones

**Choice:** Event time for curated and consumption zones; both event time and ingestion time available in raw zone.

**Why:** Event time (the timestamp embedded in the sensor reading) reflects when the measurement actually occurred, which is the relevant dimension for analysis, trend detection, and windowing. Ingestion time (when Kafka received the message) is preserved as `kafka_ts` in all zones for operational debugging. The raw zone keeps both timestamps for traceability. Using event time for windowing ensures that even if messages are delayed in transit (network issues, batch uploads from disconnected sensors), they are grouped into the correct time window.

### 4.5 End-to-End Delivery Semantics

**Choice:** At-least-once delivery with idempotent consumers.

**Why:** True exactly-once across Kafka + Spark + file sinks requires a transactional Kafka producer and `_spark_metadata` commit coordination, which adds latency and complexity. The platform achieves effectively exactly-once through:
1. **Kafka level:** `acks=all` + `retries=5` + `max_in_flight=1` ensures no lost writes and ordered delivery per partition.
2. **Spark level:** Checkpointing ensures the pipeline resumes from the last committed offset after a failure.
3. **Sink level:** The `append` output mode prevents in-place updates. Duplicates from replay (after failure) are bounded by the checkpoint interval.

**Limitation:** During a crash-recovery cycle, a small number of duplicate records may appear in the raw zone (one micro-batch window). The curated zone benefits from the `_spark_metadata` directory for idempotent writes with the FileStreamSink. For production use with strict exactly-once requirements, enabling Kafka transactions and using `foreachBatch` with a transactional Parquet writer would close this gap.

---

## 5. Results

### 5.1 Analytical Query Samples

Results are written to `outputs/analytics/` as CSV files. Expected outputs are described in [docs/analytics.md](docs/analytics.md).

### 5.2 Kafka UI Screenshots

Screenshots of Kafka UI (cluster overview, topic metrics, consumer lag) should be placed in `outputs/screenshots/`.

### 5.3 API Response Samples

```json
// GET /api/v1/health → 200
{"data":{"healthy":true,"service":"AeroSense IoT Data Platform API","timestamp":1716163200000,"version":"1.0.0"},"status":"success"}

// GET /api/v1/sensors → 200
{"data":{"count":3,"sensor_types":["humidity","pressure","temperature"]},"status":"success"}

// POST /api/v1/readings → 201
{"data":{"published":{"anomaly":false,"sensor":"temperature","source":"site-F-rack-9","timestamp":1716163200000,"unit":"C","value":23.7}},"status":"success"}

// GET /api/v1/sensors/co2/latest → 404
{"error":{"code":404,"message":"Unknown sensor type 'co2'. Allowed: ['humidity', 'pressure', 'temperature']","type":"not_found"},"status":"error"}
```

---

## 6. Limitations and Improvements

### Known Limitations

1. **Local-only execution:** The platform runs on a single machine. In production, Spark would run on a cluster (YARN/Kubernetes) and the data lake would use HDFS or S3.
2. **No schema registry:** The JSON schema is hard-coded rather than managed through Confluent Schema Registry, which would enable schema evolution.
3. **Simple anomaly detection:** Current rules are static thresholds. A production system would use ML-based anomaly detection (e.g., moving average with dynamic thresholds, isolation forest).
4. **No authentication:** The REST API has no auth layer. Production would require API keys or OAuth2.
5. **Single producer instance:** The Python producer is single-threaded and cannot reach production-scale throughput (>1K msg/s).

### What I Would Do with Two Extra Days

1. **Add a Schema Registry** and switch to Avro serialization for type safety and schema evolution.
2. **Implement exactly-once semantics** using Kafka transactions and idempotent Parquet writes.
3. **Add a Grafana dashboard** consuming the consumption zone for real-time visualization.
4. **Containerize the Spark job and API** as Docker services in the compose file for one-command startup.
5. **Add unit and integration tests** with pytest and a Kafka test container.
6. **Implement a dead-letter queue** for invalid/malformed messages, with a quarantine UI for manual inspection.

---

## Repository Structure

```
final_exam_JianyuLi_20252230/
├── README.md                  # This file
├── docker-compose.yml         # 3-broker Kafka cluster + UI
├── requirements.txt           # Python dependencies (pinned)
├── docs/
│   ├── architecture.md        # Component descriptions and diagram
│   ├── fault_tolerance.md     # Fault tolerance test documentation
│   ├── analytics.md           # Analytical query explanations
│   └── reflection.md          # Reflection questions (bonus)
├── src/
│   ├── producer.py            # IoT sensor event producer
│   ├── spark_pipeline.py      # Spark Structured Streaming pipeline
│   ├── analytics.py           # Spark SQL analytical queries
│   └── api/
│       ├── __init__.py
│       ├── app.py             # Flask REST API (6 endpoints)
│       ├── kafka_utils.py     # Kafka consumer/producer helpers
│       └── lake_utils.py      # Data lake query helpers
├── outputs/
│   ├── analytics/             # CSV outputs from queries
│   └── screenshots/           # Kafka UI and curl screenshots
└── tests/
    └── test_curl_commands.sh  # API integration test script
```

---

*EFREI Paris — Academic Year 2024–2025 — XICS404 Final Exam*
