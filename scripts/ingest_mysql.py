import os
import time
import psutil
import pymysql
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("MYSQL_HOST", "localhost")
PORT = int(os.getenv("MYSQL_PORT", 3306))
USER = os.getenv("MYSQL_USER", "root")
PWD = os.getenv("MYSQL_PASSWORD", "")
DB = os.getenv("MYSQL_DB", "nyc311")

CSV_FILENAME = os.getenv("NYC311_CSV", "./data/nyc_311_2023_sample.csv")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))


def infer_borough_from_zip(zipcode: str) -> str | None:
    if not isinstance(zipcode, str) or len(zipcode) < 3:
        return None
    prefix = zipcode[:3]
    borough_map = {
        "100": "MANHATTAN",
        "101": "MANHATTAN",
        "102": "MANHATTAN",
        "103": "STATEN ISLAND",
        "104": "BRONX",
        "111": "QUEENS",
        "112": "BROOKLYN",
        "113": "QUEENS",
        "114": "QUEENS",
        "116": "QUEENS",
    }
    return borough_map.get(prefix)


def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    original_count = len(df)
    df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]

    colmap = {
        "unique_key": "unique_key",
        "created_date": "created_date",
        "closed_date": "closed_date",
        "agency": "agency",
        "complaint_type": "complaint_type",
        "descriptor": "descriptor",
        "borough": "borough",
        "incident_zip": "incident_zip",
        "latitude": "latitude",
        "longitude": "longitude",
    }
    df = df.rename(columns=colmap)

    for col in ["created_date", "closed_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for c in ["latitude", "longitude"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["unique_key", "created_date"])
    df = df.drop_duplicates(subset=["unique_key"], keep="last")

    if "borough" in df.columns and "incident_zip" in df.columns:
        mask = df["borough"].isna()
        df.loc[mask, "borough"] = df.loc[mask, "incident_zip"].astype(str).map(
            infer_borough_from_zip
        )

    df = df[
        [
            "unique_key",
            "created_date",
            "closed_date",
            "agency",
            "complaint_type",
            "descriptor",
            "borough",
            "latitude",
            "longitude",
        ]
    ]

    cleaned_count = len(df)
    skipped = original_count - cleaned_count
    print(f"[🧹] Cleaned chunk: {cleaned_count:,} valid rows, {skipped:,} skipped.")
    return df


def insert_batch(conn, df: pd.DataFrame) -> None:
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

    with conn.cursor() as cur:
        cur.executemany(sql, data)
    conn.commit()


def ingest_mysql() -> None:
    print(f"[🚀] Starting ingestion into MySQL: {CSV_FILENAME}")
    start_time = time.time()

    conn = pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PWD,
        database=DB,
        charset="utf8mb4",
        autocommit=False,
    )

    total_rows = 0
    for chunk in pd.read_csv(CSV_FILENAME, chunksize=BATCH_SIZE):
        df = clean_chunk(chunk)
        if df.empty:
            continue
        insert_batch(conn, df)
        total_rows += len(df)

    elapsed = time.time() - start_time
    mem = psutil.Process().memory_info().rss / 1024**2
    print(
        f"[✅] {total_rows:,} rows ingested in {elapsed:.2f}s "
        f"({total_rows / elapsed:.2f} rows/s), RAM: {mem:.1f} MB"
    )

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM service_requests;")
        count, = cur.fetchone()
        print(f"[📊] Total rows in DB: {count:,}")

    conn.close()


if __name__ == "__main__":
    ingest_mysql()
