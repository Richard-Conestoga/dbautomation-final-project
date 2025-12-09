import os
import re
import time
from datetime import datetime

import pymysql
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import pymongo
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "nyc311")

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGO_DB", "nyc311")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "service_requests")
MONGO_SYNC_LOG = "mongo_sync_log"


def parse_date_range_from_filename(filename: str) -> tuple[str, str]:
    """Extract year from filename for incremental sync window."""
    year_match = re.search(r"(\d{4})", filename)
    if not year_match:
        year = datetime.now().strftime("%Y")
        print(f"[⚠️] No year in filename {filename}, using {year}")
    else:
        year = year_match.group(1)

    start_date = f"{year}-01-01"
    end_date = f"{int(year) + 1}-01-01"
    print(f"[🔍] Parsed sync window: {start_date} → {end_date}")
    return start_date, end_date


def fetch_mysql_rows(conn, start_date: str, end_date: str, limit: int = None):
    """Fetch rows from specific date range (incremental)."""
    with conn.cursor() as cur:
        if limit:
            cur.execute(
                """
                SELECT unique_key, created_date, closed_date, agency,
                       complaint_type, descriptor, borough,
                       latitude, longitude
                FROM service_requests
                WHERE created_date >= %s AND created_date < %s
                LIMIT %s
                """,
                (start_date, end_date, limit),
            )
        else:
            cur.execute(
                """
                SELECT unique_key, created_date, closed_date, agency,
                       complaint_type, descriptor, borough,
                       latitude, longitude
                FROM service_requests
                WHERE created_date >= %s AND created_date < %s
                """,
                (start_date, end_date),
            )

        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row_tuple)) for row_tuple in cur.fetchall()]

    # Convert Decimal to float for MongoDB
    for row in rows:
        if row.get("latitude") is not None:
            row["latitude"] = float(row["latitude"])
        if row.get("longitude") is not None:
            row["longitude"] = float(row["longitude"])

    return rows


def cleanup_previous_mongo(client_mongo, start_date: str, end_date: str):
    """Delete previous MongoDB data only for this sync window."""
    db = client_mongo[MONGO_DB]
    coll = db[MONGO_COLLECTION]
    result = coll.delete_many(
        {"created_date": {"$gte": start_date, "$lt": end_date}}
    )
    print(f"[🗑️] MongoDB: {result.deleted_count:,} docs removed for window")


def log_sync_start(db, start_date: str, end_date: str):
    """Check and log sync window idempotency."""
    log_coll = db[MONGO_SYNC_LOG]
    existing = log_coll.find_one(
        {"window_start": start_date, "window_end": end_date},
        sort=[("synced_at", pymongo.DESCENDING)],
    )
    if existing:
        print(
            f"[ℹ️] Previous sync found for {start_date}→{end_date} "
            f"({existing.get('rows_synced', 0):,} docs at {existing['synced_at']})"
        )
    log_id = log_coll.insert_one(
        {
            "window_start": start_date,
            "window_end": end_date,
            "status": "running",
            "rows_synced": 0,
            "started_at": datetime.utcnow(),
        }
    ).inserted_id
    return log_coll, log_id


def log_sync_end(log_coll, log_id, rows_synced: int, duration: float, status: str):
    log_coll.update_one(
        {"_id": log_id},
        {
            "$set": {
                "status": status,
                "rows_synced": rows_synced,
                "duration_seconds": round(duration, 2),
                "synced_at": datetime.utcnow(),
            }
        },
    )


def validate_counts(mysql_conn, mongo_client, start_date: str, end_date: str):
    """Post-sync validation: compare MySQL vs Mongo counts for window."""
    with mysql_conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM service_requests
            WHERE created_date >= %s AND created_date < %s
            """,
            (start_date, end_date),
        )
        mysql_count = cur.fetchone()[0]

    db = mongo_client[MONGO_DB]
    coll = db[MONGO_COLLECTION]
    mongo_count = coll.count_documents(
        {"created_date": {"$gte": start_date, "$lt": end_date}}
    )

    print(
        f"[🔍] Validation counts → MySQL: {mysql_count:,}, MongoDB: {mongo_count:,}"
    )
    if mysql_count != mongo_count:
        print("[⚠️] Count mismatch between MySQL and MongoDB for this window")


def sync_to_mongo(filename: str = None, limit: int = None):
    """Incremental sync: cleanup Mongo data → fetch MySQL → upsert to MongoDB."""
    if not MONGO_URI:
        raise RuntimeError("MONGODB_URI is not set")

    mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=True,
    )

    use_ssl = "mongodb+srv" in MONGO_URI or "mongodb.net" in MONGO_URI
    mongo_client = MongoClient(
        MONGO_URI, ssl=use_ssl, serverSelectionTimeoutMS=60000
    )
    db = mongo_client[MONGO_DB]

    if not filename:
        filename = os.getenv("NYC311_CSV", "unknown")

    start_date, end_date = parse_date_range_from_filename(filename)

    try:
        # Idempotency log
        log_coll, log_id = log_sync_start(db, start_date, end_date)

        # Cleanup target only
        print("[ℹ️] Preserving MySQL as source of truth")
        cleanup_previous_mongo(mongo_client, start_date, end_date)

        # Fetch from MySQL
        rows = fetch_mysql_rows(mysql_conn, start_date, end_date, limit)
        print(f"[📊] Fetched {len(rows):,} rows from MySQL for sync window")

        if not rows:
            print("[ℹ] No new rows to sync")
            log_sync_end(log_coll, log_id, 0, 0.0, "no_data")
            return

        # Transform docs
        docs = []
        for r in rows:
            doc = dict(r)
            doc["_id"] = r["unique_key"]
            docs.append(doc)

        coll = db[MONGO_COLLECTION]
        batch_size = 1000
        total_upserted = 0
        start_time = time.time()

        for i in range(0, len(docs), batch_size):
            batch = docs[i : i + batch_size]
            ops = [
                UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True)
                for d in batch
            ]

            batch_start = time.time()
            try:
                result = coll.bulk_write(ops, ordered=False)
            except BulkWriteError as e:
                print(f"[❌] Batch {i//batch_size + 1} failed: {e.details}")
                raise
            except Exception as e:
                print(f"[❌] Batch {i//batch_size + 1} unexpected error: {e}")
                raise

            elapsed = time.time() - batch_start
            upserted = result.upserted_count
            modified = result.modified_count
            total_upserted += upserted + modified
            rps = len(batch) / elapsed if elapsed > 0 else 0

            print(
                f"[📥] Batch {i//batch_size + 1}: {len(batch)} ops, "
                f"new={upserted}, updated={modified}, {rps:.0f} ops/s"
            )

        total_elapsed = time.time() - start_time
        print(
            f"[✅] Incremental sync complete: {total_upserted:,} docs upserted "
            f"({start_date} → {end_date}) in {total_elapsed:.1f}s"
        )

        # Log result and validate
        log_sync_end(log_coll, log_id, total_upserted, total_elapsed, "success")
        validate_counts(mysql_conn, mongo_client, start_date, end_date)

    finally:
        mysql_conn.close()
        mongo_client.close()


if __name__ == "__main__":
    filename = os.getenv(
        "NYC311_CSV", "./data/nyc_311_2023_sample.csv"
    )
    limit_str = os.getenv("MONGO_SYNC_LIMIT", "")
    limit = int(limit_str) if limit_str and limit_str.isdigit() else None

    print(f"[ℹ] Incremental sync: {os.path.basename(filename)}")
    if limit:
        print(f"[ℹ] Max rows: {limit:,}")

    sync_to_mongo(filename=filename, limit=limit)
