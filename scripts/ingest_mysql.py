import os
import time
import psutil
import pymysql
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

load_dotenv()

HOST = os.getenv("MYSQL_HOST", "localhost")
PORT = int(os.getenv("MYSQL_PORT", 3306))
USER = os.getenv("MYSQL_USER", "root")
PWD = os.getenv("MYSQL_PASSWORD", "")
DB = os.getenv("MYSQL_DB", "nyc311")

CSV_FILENAME = os.getenv("NYC311_CSV", "./data/nyc_311_2023_sample.csv")     #Change here to use sample dataset or full dataset
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))  # Tuned: balances memory (100MB/chunk) vs commit overhead

# NYC 311 standard datetime format [web:21][web:23]
DATETIME_FORMAT = "%m/%d/%Y %I:%M:%S %p"

# Explicit columns to read (schema contract, prevents drift)
REQUIRED_COLS = [
    "unique_key", "created_date", "closed_date", "agency",
    "complaint_type", "descriptor", "borough", "incident_zip",
    "latitude", "longitude"
]


def parse_date_range_from_filename(filename: str) -> tuple[str, str]:
    """
    Extract year from filename and return date range for cleanup.
    
    Example: "311_Service_Requests_from_2011.csv" → ('2011-01-01', '2012-01-01')
    """
    import re
    year_match = re.search(r'(\d{4})', filename)
    if not year_match:
        raise ValueError(f"Could not parse year from filename: {filename}")
    
    year = year_match.group(1)
    start_date = f"{year}-01-01"
    end_date = f"{int(year) + 1}-01-01"
    
    print(f"[🔍] Parsed {filename} → date range {start_date} to {end_date}")
    return start_date, end_date


def create_ingestion_log_table(conn):
    """Create idempotency tracking table."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_log (
                dataset_file VARCHAR(255) PRIMARY KEY,
                ingested_rows BIGINT NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                elapsed_seconds DECIMAL(10,2),
                rows_per_sec DECIMAL(10,2)
            )
        """)
    conn.commit()


def log_ingestion_start(conn, filename: str):
    """Check if file already ingested; skip if present."""
    with conn.cursor() as cur:
        cur.execute("SELECT ingested_rows FROM ingestion_log WHERE dataset_file = %s", (filename,))
        result = cur.fetchone()
        if result:
            print(f"[⏭] Dataset {filename} already ingested ({result[0]:,} rows). Skipping.")
            return True
    return False


def cleanup_previous_data(conn, filename: str):
    """Delete previous data from the target ingestion window."""
    try:
        start_date, end_date = parse_date_range_from_filename(filename)
    except ValueError as e:
        print(f"[⚠] Skipping cleanup for {filename}: {e}")
        return
    
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM service_requests
            WHERE created_date >= %s AND created_date < %s
        """, (start_date, end_date))
        deleted = cur.rowcount
        print(f"[🗑️] Cleaned {deleted:,} previous rows ({start_date} to {end_date})")
    conn.commit()


def infer_borough_from_zip(zipcode: str) -> str | None:
    """Infer NYC borough from ZIP code prefix."""
    if not isinstance(zipcode, str) or len(zipcode) < 3:
        return None
    prefix = zipcode[:3]
    borough_map = {
        "100": "MANHATTAN", "101": "MANHATTAN", "102": "MANHATTAN",
        "103": "STATEN ISLAND", "104": "BRONX",
        "111": "QUEENS", "112": "BROOKLYN", "113": "QUEENS", 
        "114": "QUEENS", "116": "QUEENS"
    }
    return borough_map.get(prefix)


def clean_chunk(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Clean NYC 311 chunk with validation and telemetry.
    
    Returns: (cleaned_df, cleaning_stats)
    """
    df = df.copy()
    original_count = len(df)
    
    # Normalize column names
    df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]
    
    # Schema mapping (only keep expected columns)
    colmap = {
        "unique_key": "unique_key", "created_date": "created_date", 
        "closed_date": "closed_date", "agency": "agency",
        "complaint_type": "complaint_type", "descriptor": "descriptor",
        "borough": "borough", "incident_zip": "incident_zip",
        "latitude": "latitude", "longitude": "longitude"
    }
    df = df.rename(columns=colmap)
    
    # Parse datetimes with explicit format (no warnings, faster)
    for col in ["created_date", "closed_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=DATETIME_FORMAT, errors="coerce")
    
    # Coerce numerics
    for c in ["latitude", "longitude"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    
    # Data quality filters (edge cases)
    initial_rows = len(df)
    
    # 1. Drop rows missing required fields
    df = df.dropna(subset=["unique_key", "created_date"])
    
    # 2. Remove duplicates
    df = df.drop_duplicates(subset=["unique_key"], keep="last")
    
    # 3. Infer missing boroughs
    if "borough" in df.columns and "incident_zip" in df.columns:
        mask = df["borough"].isna()
        df.loc[mask, "borough"] = df.loc[mask, "incident_zip"].astype(str).map(infer_borough_from_zip)
    
    # 4. Filter invalid lat/lng (NYC bounds)
    if "latitude" in df.columns and "longitude" in df.columns:
        nyc_mask = (
            (df["latitude"].between(40.5, 40.9)) & 
            (df["longitude"].between(-74.3, -73.7))
        )
        df = df[nyc_mask]
    
    # Final column selection
    final_cols = ["unique_key", "created_date", "closed_date", "agency",
                 "complaint_type", "descriptor", "borough", "latitude", "longitude"]
    df = df[final_cols]
    
    cleaned_count = len(df)
    
    # Cleaning telemetry
    stats = {
        "original": original_count,
        "cleaned": cleaned_count,
        "dropped_required": initial_rows - len(df.dropna(subset=["unique_key", "created_date"])),
        "dropped_dupes": len(df) - cleaned_count,
        "nyc_bounds_filtered": sum(~nyc_mask) if "latitude" in locals() else 0
    }
    
    print(f"[🧹] Chunk: {stats['cleaned']:,}/{stats['original']:,} rows "
          f"(dropped: {stats['dropped_required']:,} req + {stats['dropped_dupes']:,} dupes)")
    
    return df, stats


def insert_batch(conn, df: pd.DataFrame) -> None:
    """Transactional batch insert with rollback safety."""
    if df.empty:
        return
    
    df = df.replace({np.nan: None, pd.NaT: None})
    data = [tuple(row) for row in df.to_numpy()]
    placeholders = ",".join(["%s"] * len(df.columns))
    cols = ",".join(df.columns)
    
    sql = f"""
        INSERT INTO service_requests ({cols})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
          created_date=VALUES(created_date),
          closed_date=VALUES(closed_date),
          agency=VALUES(agency),
          complaint_type=VALUES(complaint_type),
          descriptor=VALUES(descriptor),
          borough=VALUES(borough),
          latitude=VALUES(latitude),
          longitude=VALUES(longitude)
    """
    
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[❌] Batch insert failed (rolled back): {e}")
        raise


def run_data_quality_checks(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) as total FROM service_requests")
        total = cur.fetchone()[0]
        
        cur.execute("""
            SELECT 
                MIN(created_date), MAX(created_date),
                MIN(latitude), MAX(latitude),
                MIN(longitude), MAX(longitude)
            FROM service_requests
        """)
        ranges = cur.fetchone()
        
        cur.execute("""
            SELECT HOUR(created_date) as hour, COUNT(*) as complaints
            FROM service_requests 
            GROUP BY HOUR(created_date) 
            ORDER BY hour
            LIMIT 5
        """)
        hourly_sample = cur.fetchall()
    
    # Safe formatting: show "N/A" if None
    def fmt(dt):
        return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "N/A"
    def fmt_num(num):
        return f"{num:.4f}" if num is not None else "N/A"
    
    print(f"[📊] VALIDATION: {total:,} total rows")
    print(f"[📊] Date range: {fmt(ranges[0])} → {fmt(ranges[1])}")
    print(f"[📊] Lat/Lng bounds: {fmt_num(ranges[2])}/{fmt_num(ranges[3])}, {fmt_num(ranges[4])}/{fmt_num(ranges[5])}")
    print("[📊] Hourly complaints sample:", hourly_sample[:3])


def ingest_mysql() -> None:
    """Production-grade NYC 311 ETL with full telemetry."""
    print(f"[🚀] Starting ETL: {CSV_FILENAME} (BATCH_SIZE={BATCH_SIZE:,})")
    overall_start = time.time()
    
    conn = pymysql.connect(
        host=HOST, port=PORT, user=USER, password=PWD,
        database=DB, charset="utf8mb4", autocommit=False
    )
    
    try:
        # STEP 1: Ensure logging infrastructure exists
        create_ingestion_log_table(conn)
        
        # STEP 2: Idempotency check
        if log_ingestion_start(conn, os.path.basename(CSV_FILENAME)):
            return
        
        # STEP 3: Cleanup previous data for this file
        cleanup_previous_data(conn, os.path.basename(CSV_FILENAME))
        
        total_rows = 0
        total_chunks = 0
        cleaning_stats = []
        
        # Per-chunk telemetry loop
        for chunk_num, chunk in enumerate(pd.read_csv(CSV_FILENAME, chunksize=BATCH_SIZE)):
            chunk_start = time.time()
            
            df, stats = clean_chunk(chunk)
            cleaning_stats.append(stats)
            
            if df.empty:
                continue
            
            insert_batch(conn, df)
            total_rows += len(df)
            total_chunks += 1
            
            # Per-chunk telemetry
            chunk_time = time.time() - chunk_start
            rows_per_sec = len(df) / chunk_time if chunk_time > 0 else 0
            mem_mb = psutil.Process().memory_info().rss / 1024**2
            cpu_pct = psutil.cpu_percent()
            
            print(f"[📈] Chunk #{chunk_num+1}: {len(df):,} rows, "
                  f"{rows_per_sec:.0f} r/s, MEM: {mem_mb:.1f}MB, CPU: {cpu_pct:.1f}%")
        
        # Final telemetry
        elapsed = time.time() - overall_start
        overall_rps = total_rows / elapsed if elapsed > 0 else 0
        final_mem = psutil.Process().memory_info().rss / 1024**2
        
        print(f"\n[✅] ETL COMPLETE:")
        print(f"   {total_rows:,} rows in {total_chunks} chunks, {elapsed:.1f}s ({overall_rps:.0f} r/s)")
        print(f"   Peak RAM: {final_mem:.1f} MB")
        
        # Log to ingestion_log table
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ingestion_log (dataset_file, ingested_rows, elapsed_seconds, rows_per_sec)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    ingested_rows=VALUES(ingested_rows),
                    elapsed_seconds=VALUES(elapsed_seconds),
                    rows_per_sec=VALUES(rows_per_sec)
            """, (os.path.basename(CSV_FILENAME), total_rows, elapsed, overall_rps))
        
        # Data quality validation
        run_data_quality_checks(conn)
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"[💥] ETL FAILED (full rollback): {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    ingest_mysql()
