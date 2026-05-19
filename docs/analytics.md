# Analytical Query Results

## Query 1: Top 5 Hours with Highest Anomaly Count

This query aggregates anomalies across all sensor types by hour and ranks the top 5 hours with the most anomalies.

**SQL logic:** Filter `is_anomaly == True`, group by (year, month, day, hour), count, order by count descending, limit 5.

**Sample output (when data is present):**

| year | month | day | hour | anomaly_count |
|------|-------|-----|------|---------------|
| 2026 | 5     | 19  | 9    | 47            |
| 2026 | 5     | 19  | 8    | 35            |
| 2026 | 5     | 19  | 10   | 28            |
| 2026 | 5     | 19  | 7    | 19            |
| 2026 | 5     | 19  | 6    | 15            |

*Note: Actual values depend on the volume of data produced and the ~12% anomaly rate.*

## Query 2: Per-Sensor Type Global Statistics

Computes mean, min, max, standard deviation, anomaly count, total count, and anomaly rate as a percentage for each sensor type.

**Expected anomaly rate:** ~9-15% due to the anomaly detection rules in the Spark pipeline (temperature >35°C, humidity >90%, pressure <990 or >1030 hPa).

## Query 3: Daily Evolution of Temperature

Shows the daily mean temperature and anomaly count over time, enabling trend analysis.

## Query 4: Partition Pruning Demonstration

### Test Methodology

1. **Full scan:** `curated.count()` with no filter — reads all Parquet files.
2. **Partition filter:** `curated.filter(col("sensor") == "temperature").count()` — Spark reads only the `sensor=temperature` directories.
3. **Multi-column filter:** Filter on sensor + year + month — reads only the matching subdirectory.

### Results

| Query Type       | Filters Applied          | Row Count | Time (s) | Speedup vs Full |
|-----------------|--------------------------|-----------|----------|-----------------|
| Full scan       | None                     | N         | T1       | 1.0x            |
| Sensor filter   | sensor=temperature       | N_temp    | T2       | T1/T2           |
| Multi filter    | sensor + year + month    | N_sub     | T3       | T1/T3           |

*Speedup is most pronounced with large datasets where partition pruning avoids scanning irrelevant directories. With small datasets, the overhead of listing files may reduce the observed speedup.*

### Why Partition Pruning Works

The curated zone is partitioned by `sensor/year/month/day` on disk:
```
curated/domain=iot/sensor=temperature/year=2026/month=5/day=19/
curated/domain=iot/sensor=humidity/year=2026/month=5/day=19/
curated/domain=iot/sensor=pressure/year=2026/month=5/day=19/
```

When Spark's query planner sees a filter on `sensor`, it reads the partition metadata and skips entire directories that don't match, avoiding both I/O and deserialization costs.
