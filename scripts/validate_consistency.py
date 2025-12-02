import os
import pymysql
from pymongo import MongoClient
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


def validate_counts():
    mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=True,
    )
    with mysql_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM service_requests;")
        mysql_count, = cur.fetchone()
    mysql_conn.close()

    client = MongoClient(MONGO_URI)
    coll = client[MONGO_DB][MONGO_COLLECTION]
    mongo_count = coll.count_documents({})
    client.close()

    print(f"[MySQL] service_requests rows: {mysql_count}")
    print(f"[Mongo] service_requests docs: {mongo_count}")

    if mongo_count == 0 or mysql_count == 0:
        raise AssertionError("One of the databases is empty")

    if abs(mysql_count - mongo_count) > 1000:
        raise AssertionError("Count difference between MySQL and MongoDB is too large")

    print("[✅] Basic count validation passed")


if __name__ == "__main__":
    if not MONGO_URI:
        raise RuntimeError("MONGODB_URI is not set for validation")
    validate_counts()
