import os
import pymysql
from pymongo import MongoClient
from dotenv import load_dotenv
import ssl

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "nyc311")

MONGO_URI = os.getenv("MONGODB_URI")  # Atlas connection string [web:44]
MONGO_DB = os.getenv("MONGO_DB", "nyc311")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "service_requests")


def fetch_mysql_rows(limit: int = None):
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    with conn.cursor() as cur:
        if limit and limit > 0:
            cur.execute(
                """
                SELECT unique_key, created_date, closed_date, agency,
                       complaint_type, descriptor, borough,
                       latitude, longitude
                FROM service_requests
                LIMIT %s
                """,
                (limit,),
            )
        else:
            # No limit - sync all rows (for local development)
            cur.execute(
                """
                SELECT unique_key, created_date, closed_date, agency,
                       complaint_type, descriptor, borough,
                       latitude, longitude
                FROM service_requests
                """
            )
        rows = cur.fetchall()
    
    # Convert Decimal to float for MongoDB compatibility
    for row in rows:
        if 'latitude' in row and row['latitude'] is not None:
            row['latitude'] = float(row['latitude'])
        if 'longitude' in row and row['longitude'] is not None:
            row['longitude'] = float(row['longitude'])
    
    conn.close()
    return rows


def sync_to_mongo(limit: int = 50000):
    if not MONGO_URI:
        raise RuntimeError("MONGODB_URI is not set")

    rows = fetch_mysql_rows(limit=limit)
    # Use SSL only for Atlas (mongodb+srv://), not for local MongoDB
    use_ssl = "mongodb+srv" in MONGO_URI or "mongodb.net" in MONGO_URI
    client = MongoClient(MONGO_URI, ssl=use_ssl, serverSelectionTimeoutMS=60000)
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]

    docs = []
    for r in rows:
        doc = dict(r)
        doc["_id"] = r["unique_key"]
        docs.append(doc)

    if docs:
        coll.insert_many(docs, ordered=False)
        print(f"[📥] Inserted {len(docs)} docs into MongoDB collection '{MONGO_COLLECTION}'")
    else:
        print("[ℹ] No rows to sync")

    client.close()


if __name__ == "__main__":
    limit_str = os.getenv("MONGO_SYNC_LIMIT", "0")
    limit = int(limit_str) if limit_str and limit_str != "0" else None
    if limit:
        print(f"[ℹ️] Syncing up to {limit:,} rows from MySQL to MongoDB")
    else:
        print("[ℹ️] Syncing ALL rows from MySQL to MongoDB (no limit)")
    sync_to_mongo(limit=limit)
