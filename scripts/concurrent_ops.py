import os
import random
import threading
import time
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


def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=True,
    )


def get_mongo_coll():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB][MONGO_COLLECTION]


def worker_mysql_updates(n: int = 50):
    conn = get_mysql_conn()
    with conn.cursor() as cur:
        for _ in range(n):
            rand_borough = random.choice(["MANHATTAN", "BROOKLYN", "QUEENS"])
            cur.execute(
                """
                UPDATE service_requests
                SET borough = borough
                WHERE borough = %s
                LIMIT 10
                """,
                (rand_borough,),
            )
            time.sleep(0.05)
    conn.close()
    print("[MySQL] Updates thread done")


def worker_mysql_queries(n: int = 50):
    conn = get_mysql_conn()
    with conn.cursor() as cur:
        for _ in range(n):
            cur.execute(
                """
                SELECT borough, COUNT(*) 
                FROM service_requests
                GROUP BY borough
                LIMIT 5
                """
            )
            _ = cur.fetchall()
            time.sleep(0.05)
    conn.close()
    print("[MySQL] Queries thread done")


def worker_mongo_queries(n: int = 50):
    coll = get_mongo_coll()
    for _ in range(n):
        pipeline = [
            {"$group": {"_id": "$borough", "count": {"$sum": 1}}},
            {"$limit": 5}
        ]
        list(coll.aggregate(pipeline))
        time.sleep(0.05)
    print("[Mongo] Queries thread done")


def run_concurrent_ops():
    if not MONGO_URI:
        raise RuntimeError("MONGODB_URI is not set for concurrent_ops")

    threads = [
        threading.Thread(target=worker_mysql_updates, args=(30,)),
        threading.Thread(target=worker_mysql_queries, args=(30,)),
        threading.Thread(target=worker_mongo_queries, args=(30,)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("[✅] Concurrent MySQL/MongoDB operations completed")


if __name__ == "__main__":
    run_concurrent_ops()
