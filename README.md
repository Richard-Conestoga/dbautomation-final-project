# Database Automation – Final Project

## ✅ Task 1 – Data Ingestion, Sync & Validation

### 🎯 Objective
Build a production-style, automated pipeline that:

- Ingests large NYC 311 CSV data into MySQL using chunked ETL.
- Cleans and normalizes records (dates, boroughs, coordinates).
- Syncs MySQL data incrementally into MongoDB.
- Validates consistency between MySQL and MongoDB.
- Exposes clear telemetry (rows/sec, RAM/CPU) for performance tuning.

---

### 🧠 Approach

| Step | Description |
|------|-------------|
| 1️⃣ Data Acquisition | Download NYC 311 data (sample + Kaggle 2011/full) via scripts/download_nyc311.py |
| 2️⃣ Chunked Ingestion | Load CSV into MySQL in batches using scripts/ingest_mysql.py |
| 3️⃣ Data Cleaning | Normalize columns, fix boroughs from ZIP, filter invalid lat/lng |
| 4️⃣ Idempotency & Logging | Use ingestion_log table for re-run safety & throughput tracking |
| 5️⃣ MongoDB Sync | Incremental, window-based upsert from MySQL → MongoDB via scripts/sync_to_mongo.py |
| 6️⃣ Consistency Validation | scripts/validate_consistency.py compares MySQL vs MongoDB counts |

---

### 🧹 Data Cleaning Highlights

- Drop rows with missing `unique_key` or `created_date`.
- Normalize column names (snake_case) and parse dates with explicit format.
- Infer missing `borough` from `incident_zip` (ZIP prefix → borough map).
- Enforce NYC coordinate bounds (lat 40.5–40.9, lng −74.3 to −73.7).
- Drop duplicates on `unique_key` (keep latest).

These steps are implemented in `scripts/ingest_mysql.py` (`clean_chunk` function) and documented as a “cleaning checklist” in the code comments.

---

### 📈 Telemetry & Idempotency

**MySQL ETL (scripts/ingest_mysql.py):**

- Reads CSV with `chunksize=BATCH_SIZE` (default 10,000).
- Logs per-chunk:
  - rows ingested
  - rows/sec
  - RAM (MB) via `psutil`
  - CPU%
- Wraps batch inserts in `try/except` + `rollback` for transactional safety.
- Uses `ON DUPLICATE KEY UPDATE` on `unique_key` for idempotent upserts.
- Writes to an `ingestion_log` table with:
  - `dataset_file`
  - `ingested_rows`
  - `elapsed_seconds`
  - `rows_per_sec`
  - `loaded_at`

**MongoDB Sync (scripts/sync_to_mongo.py):**

- Parses the year from `NYC311_CSV` filename → defines window `[year-01-01, (year+1)-01-01)`.
- Preserves MySQL as source of truth; cleans only previous Mongo docs in that window.
- Fetches rows for window and upserts in MongoDB using `bulk_write` + `UpdateOne({...}, {"$set": doc}, upsert=True)`.
- Logs per-batch:
  - ops in batch
  - new vs updated docs
  - ops/sec
- Stores sync metadata in `mongo_sync_log` collection (window, status, rows_synced, duration).

---

### 🔍 Automated Validation

After sync, `scripts/validate_consistency.py` and `sync_to_mongo.py`:

- Compare `COUNT(*)` in MySQL vs `count_documents()` in MongoDB for the same date window.
- Print a warning if counts differ.
- CI pipeline fails if one side is empty or counts mismatch, enforcing data consistency.

---

### 💡 Performance Notes

- **Batch size (10,000 rows)** chosen to balance:
  - Good throughput (~6–7k rows/sec on 2011 dataset locally).
  - Controlled RAM usage (~130–150 MB).
  - Reasonable rollback scope on failure.
- CI uses a **25k-row synthetic sample** (`nyc_311_2023_sample.csv`) for fast runs; locally we also test with the **1.2 GB 2011 Kaggle file** for realistic performance.

---

## 🤖 Task 3 – Anomaly Detection & Optimization

### 📌 Objective
Build an anomaly detection module using Python (Pandas + Scikit-learn) to identify abnormal NYC311 requests, store flagged rows in a separate table, analyze the results, and suggest performance improvements based on Signoz metrics.

---

### 🧠 Approach

| Step | Description |
|------|-------------|
| 1️⃣ Data Load | Fetched records from MySQL `service_requests` table using Pandas |
| 2️⃣ Anomaly Detection | Detected missing, long-open, and location outlier anomalies |
| 3️⃣ Save Results | Stored anomalies into a separate MySQL table called `anomalies` |
| 4️⃣ Analysis | Counted anomalies by category and verified with SQL |
| 5️⃣ Optimization | Suggested performance improvements using Signoz metrics |

---

### 🚨 Types of Anomalies Detected

| Anomaly Type | Detection Logic |
|--------------|----------------|
| missing_location | Latitude or longitude is NULL |
| long_open_case | Request open > 90 days with no closed_date |
| location_outlier | Out-of-city coordinates identified using IsolationForest ML |

---

### 📊 Results Summary

| Metric | Value |
|--------|------|
| Total records scanned | 6 |
| Total anomalies detected | 5 |
| Saved into `anomalies` table | ✔️ Yes |

Screenshots (below) prove the results:
- Record count in service_requests
- Anomalies count
- Breakdown by anomaly_reason
- Sample anomalies
- Terminal script output

(Add screenshots below)

## 📸 Screenshots

### 1️⃣ Script Execution Output
![script_output](Screenshots/Task3-zafar/script_output.png)

### 2️⃣ service_requests Count (6 rows)
![service_requests_count](Screenshots/Task3-zafar/service_requests_count.png)

### 3️⃣ anomalies Count (5 anomalies)
![anomalies_count](Screenshots/Task3-zafar/anomalies_count.png)

### 4️⃣ Anomaly Category Breakdown
![anomaly_breakdown](Screenshots/Task3-zafar/anomaly_breakdown.png)

### 5️⃣ Sample anomaly records from anomalies table
![anomalies_table](Screenshots/Task3-zafar/anomalies_table.png)


### 🎯 Why Precision/Recall Not Used
The dataset has **no ground-truth labels**, so accuracy metrics like precision and recall cannot be calculated.  
Instead, anomaly counts by type are analyzed qualitatively.

---

### 🚀 SQL Performance Optimization Based on Signoz

| Optimization | Benefit |
|-------------|---------|
| Create index on created_date + borough | Faster lookups without full scans |
| Avoid SELECT * | Less data scanned → faster response |
| Filter recent data only | Prevents scanning entire history → scalable |

Example SQL:
```sql
CREATE INDEX idx_sr_created_borough 
ON service_requests(created_date, borough);

SELECT unique_key, created_date, closed_date, latitude, longitude
FROM service_requests;

SELECT *
FROM service_requests
WHERE created_date >= NOW() - INTERVAL 30 DAY;
