import os
import pymysql
from pymongo import MongoClient
from dotenv import load_dotenv
import re
from datetime import datetime

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "nyc311")

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGO_DB", "nyc311")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "service_requests")


def parse_date_range_from_filename(filename: str) -> tuple[str, str]:
    """Extract year from filename for incremental sync window."""
    year_match = re.search(r'(\d{4})', filename)
    if not year_match:
        # Fallback: use current year
        year = datetime.now().strftime("%Y")
        print(f"[⚠️] No year in filename {filename}, using {year}")
    else:
        year = year_match.group(1)
    
    start_date = f"{year}-01-01"
    end_date = f"{int(year) + 1}-01-01"
    print(f"[🔍] Parsed sync window: {start_date} → {end_date}")
    return start_date, end_date


def cleanup_previous_sync(conn_mysql, client_mongo, filename: str):
    """Delete previous data for this sync window from both MySQL and MongoDB."""
    start_date, end_date = parse_date_range_from_filename(filename)
    
    # MySQL cleanup
    with conn_mysql.cursor() as cur:
        cur.execute("""
            DELETE FROM service_requests
            WHERE created_date >= %s AND created_date < %s
        """, (start_date, end_date))
        mysql_deleted = cur.rowcount
    
    # MongoDB cleanup (same date range)
    db = client_mongo[MONGO_DB]
    coll = db[MONGO_COLLECTION]
    mongo_deleted = coll.delete_many({
        "created_date": {
            "$gte": start_date,
            "$lt": end_date
        }
    }).deleted_count
    
    print(f"[🗑️] Cleaned MySQL: {mysql_deleted:,} rows, MongoDB: {mongo_deleted:,} docs")
    conn_mysql.commit()


def fetch_mysql_rows(conn, start_date: str, end_date: str, limit: int = None):
    """Fetch rows from specific date range (incremental)."""
    with conn.cursor(cursorclass=pymysql.cursors.DictCursor) as cur:
        if limit:
            cur.execute("""
                SELECT unique_key, created_date, closed_date, agency,
                       complaint_type, descriptor, borough,
                       latitude, longitude
                FROM service_requests
                WHERE created_date >= %s AND created_date < %s
                LIMIT %s
            """, (start_date, end_date, limit))
        else:
            cur.execute("""
                SELECT unique_key, created_date, closed_date, agency,
                       complaint_type, descriptor, borough,
                       latitude, longitude
                FROM service_requests
                WHERE created_date >= %s AND created_date < %s
            """, (start_date, end_date))
        
        rows = cur.fetchall()
    
    # Convert Decimal to float for MongoDB
    for row in rows:
        if row.get('latitude') is not None:
            row['latitude'] = float(row['latitude'])
        if row.get('longitude') is not None:
            row['longitude'] = float(row['longitude'])
    
    return rows


def sync_to_mongo(filename: str = None, limit: int = None):
    """Incremental sync: cleanup old data → fetch new → upsert to MongoDB."""
    if not MONGO_URI:
        raise RuntimeError("MONGODB_URI is not set")
    
    # MySQL connection (autocommit=True for cleanup)
    mysql_conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DB, charset="utf8mb4", autocommit=True
    )
    
    # MongoDB connection
    use_ssl = "mongodb+srv" in MONGO_URI or "mongodb.net" in MONGO_URI
    mongo_client = MongoClient(MONGO_URI, ssl=use_ssl, serverSelectionTimeoutMS=60000)
    
    try:
        # 1. Incremental cleanup (both databases)
        cleanup_previous_sync(mysql_conn, mongo_client, filename or "unknown")
        
        # 2. Fetch rows for this sync window
        if not filename:
            filename = os.getenv("NYC311_CSV", "unknown")
        
        start_date, end_date = parse_date_range_from_filename(filename)
        rows = fetch_mysql_rows(mysql_conn, start_date, end_date, limit)
        
        print(f"[📊] Fetched {len(rows):,} rows from MySQL for sync window")
        
        if not rows:
            print("[ℹ] No new rows to sync")
            return
        
        # 3. Transform to MongoDB documents
        docs = []
        for r in rows:
            doc = dict(r)
            doc["_id"] = r["unique_key"]
            docs.append(doc)
        
        # 4. Batch upsert (incremental, handles duplicates gracefully)
        db = mongo_client[MONGO_DB]
        coll = db[MONGO_COLLECTION]
        
        batch_size = 1000
        total_upserted = 0
        
        for i in range(0, len(docs), batch_size):
            batch = docs[i:i + batch_size]
            try:
                # Upsert: insert new, update existing (idempotent)
                result = coll.bulk_write([
                    pymongo.ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
                    for doc in batch
                ])
                total_upserted += result.upserted_count + result.modified_count
                print(f"[📥] Batch {i//batch_size + 1}: {len(batch)} docs upserted "
                      f"(new: {result.upserted_count}, updated: {result.modified_count})")
            except Exception as e:
                print(f"[❌] Batch {i//batch_size + 1} failed: {e}")
                raise
        
        print(f"[✅] Incremental sync complete: {total_upserted:,} docs upserted "
              f"({start_date} → {end_date})")
    
    finally:
        mysql_conn.close()
        mongo_client.close()


if __name__ == "__main__":
    filename = os.getenv("NYC311_CSV", "./data/311_Service_Requests_from_2011.csv")
    limit_str = os.getenv("MONGO_SYNC_LIMIT", "")
    limit = int(limit_str) if limit_str and limit_str.isdigit() else None
    
    print(f"[ℹ] Incremental sync: {os.path.basename(filename)}")
    if limit:
        print(f"[ℹ] Max rows: {limit:,}")
    
    sync_to_mongo(filename=filename, limit=limit)
