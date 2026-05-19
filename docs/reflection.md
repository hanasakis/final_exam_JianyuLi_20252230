# Reflection Questions

## Q1: Pipeline Crash Between Raw and Curated Zones

**Impact:** If the pipeline crashes after writing to the raw zone but before completing the curated zone write, the data in the raw zone is safe (JSON files already persisted). The curated and consumption zones would miss those records for that micro-batch. On restart, Spark Structured Streaming reads from the checkpoint offset and re-processes the same Kafka messages. Since the raw sink uses `outputMode=append`, re-processing would create duplicate raw records. The curated sink would catch up without gaps.

**Prevention strategy:** Checkpointing is the key mechanism. Each sink has its own checkpoint directory. Spark records the Kafka offset and the micro-batch progress atomically. On restart, the pipeline resumes from the last committed offset. To achieve exactly-once semantics, the curated and consumption sinks can use the `_spark_metadata` directory with idempotent writes. The combination of Kafka source replay and checkpointed sink progress prevents permanent data loss.

## Q2: Scaling to 50,000 Messages/Second

**Likely bottlenecks at 50k msg/s:**

1. **Kafka broker I/O:** Each broker handles ~17k msg/s. With replication factor 3 and SSDs, this is manageable. The first bottleneck would likely be network bandwidth between brokers for replication traffic.
2. **Spark processing:** JSON parsing is CPU-intensive. At 50k msg/s, a single Spark executor may saturate. Solution: increase `spark.sql.shuffle.partitions` and add more executor cores.
3. **Parquet sink writes:** Many small Parquet files per micro-batch cause write amplification. Solution: increase `maxOffsetsPerTrigger` to batch more records per trigger, or tune `spark.sql.streaming.minBatchesToRetain`.
4. **Python producer:** A single-threaded Python producer cannot reach 50k msg/s. Solution: use multi-threading or switch to multiple producer instances.

**Fixes:** Horizontal scaling of brokers, multi-threaded producers, larger Spark clusters, and optimized Parquet file sizing.

## Q3: Kafka vs Parquet as Source of Truth

**Kafka as source of truth:**
- Advantages: Real-time access, ordered events, replay capability via offsets, exactly-once semantics with transactions.
- Drawbacks: Retention-limited (disk-based, finite storage), expensive long-term storage, not optimized for analytical queries, higher cost per GB.

**Parquet data lake as source of truth:**
- Advantages: Columnar format enables fast analytical queries, efficient compression, partition pruning, low-cost object storage, schema evolution support.
- Drawbacks: Not real-time, batch-oriented, no built-in ordering guarantees, requires separate indexing for point lookups.

**When to prefer each:**
- Kafka: Real-time alerting, operational dashboards requiring sub-second latency, event-driven microservices.
- Parquet: Historical analysis, machine learning training, regulatory archiving, ad-hoc SQL queries spanning months of data.

**The recommended pattern** (used in this platform) is a hybrid: Kafka for the hot path (streaming) and Parquet for the cold path (analytics).

## Q4: Aberrant Sensor Values Detection and Isolation

**Detection:** The Spark pipeline's validation layer already filters physically implausible values (e.g., temperature outside [-50, 100]). However, a sensor could emit values within the plausibility range but statistically aberrant (e.g., a temperature sensor at a constant 40°C for 2 hours). The anomaly detection rules (temperature >35°C) would flag these as anomalies, triggering alerts. Additionally, the windowed aggregation would show an elevated mean and anomaly count for that sensor_type, which the analytics queries would surface.

**Isolation without deletion:**
1. Add an `aberration_flag` column during validation based on statistical outlier detection (z-score or IQR over a sliding window).
2. Write aberrated records to a separate quarantine partition in the curated zone (`curated/domain=iot/sensor_type=.../quarantine=true/`).
3. Use a `data_quality` metadata column marking the record as "suspect" rather than deleting it.
4. The consumption zone aggregates exclude quarantined records, but raw and curated zones preserve the original data for forensic analysis.

## Q5: Adding a New Sensor Type (CO2)

### Files to modify:

1. **`src/producer.py`:**
   - Add `"co2"` to `SENSOR_CONFIG` with `unit: "ppm"`, realistic min/max (e.g., 350–2000 ppm), and anomaly thresholds.
   - The random generation logic reuses the existing `_generate_value()` function — no structural changes needed.

2. **`src/spark_pipeline.py`:**
   - Add `"co2"` to the validation filter (e.g., `co2.between(300, 5000)`).
   - Extend `ANOMALY_EXPR` with a CO2 rule (e.g., `co2 > 1500`).
   - The schema remains compatible since `sensor` is a free-text field.

3. **`src/api/kafka_utils.py`:**
   - Add `"co2"` to `ALLOWED_SENSORS` and `"co2": "ppm"` to `ALLOWED_UNITS`.

4. **`src/api/app.py`:**
   - No changes required — validation is driven by `ALLOWED_SENSORS`.

5. **`src/api/lake_utils.py`:**
   - No changes required — queries are parameterized by sensor_type.

6. **`docs/architecture.md`:**
   - Update component descriptions to mention the new sensor type.

**Total changes:** 3 files with actual logic changes, 1 documentation file. The pipeline's parameterized design means no schema redeployment is needed — the new sensor type reuses the exact same JSON schema.
