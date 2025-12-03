import os
import pymysql
from pymongo import MongoClient
from dotenv import load_dotenv
from prometheus_client import Gauge, start_http_server, REGISTRY
import time

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "nyc311")

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGO_DB", "nyc311")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "service_requests")

# Prometheus metrics for Grafana dashboard
mysql_row_count = Gauge('nyc311_mysql_row_count', 'Total rows in MySQL service_requests table')
mongo_doc_count = Gauge('nyc311_mongo_doc_count', 'Total documents in MongoDB service_requests collection')
sync_mismatch_count = Gauge('nyc311_sync_mismatch_count', 'Difference between MySQL and MongoDB counts')
sync_status = Gauge('nyc311_sync_status', 'Sync status: 1=in sync, 0=out of sync')
last_check_timestamp = Gauge('nyc311_last_check_timestamp', 'Unix timestamp of last consistency check')


def validate_counts(update_metrics=False):
    """
    Validate row counts between MySQL and MongoDB.

    Args:
        update_metrics: If True, update Prometheus metrics for Grafana

    Returns:
        tuple: (mysql_count, mongo_count, mismatch)
    """
    # Connect to MySQL
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

    # Connect to MongoDB - use SSL only for Atlas, not for local
    use_ssl = "mongodb+srv" in MONGO_URI or "mongodb.net" in MONGO_URI
    client = MongoClient(MONGO_URI, ssl=use_ssl, serverSelectionTimeoutMS=60000)
    coll = client[MONGO_DB][MONGO_COLLECTION]
    mongo_count = coll.count_documents({})
    client.close()

    # Calculate mismatch
    mismatch = abs(mysql_count - mongo_count)
    is_in_sync = mismatch == 0

    print(f"[MySQL] service_requests rows: {mysql_count:,}")
    print(f"[Mongo] service_requests docs: {mongo_count:,}")
    print(f"[📊] Mismatch: {mismatch:,} records")

    # Update Prometheus metrics if requested
    if update_metrics:
        mysql_row_count.set(mysql_count)
        mongo_doc_count.set(mongo_count)
        sync_mismatch_count.set(mismatch)
        sync_status.set(1 if is_in_sync else 0)
        last_check_timestamp.set(time.time())
        print(f"[📈] Prometheus metrics updated")

    # Validation checks
    if mongo_count == 0 or mysql_count == 0:
        print("[⚠️] Warning: One of the databases is empty")
        if update_metrics:
            return mysql_count, mongo_count, mismatch
        raise AssertionError("One of the databases is empty")

    if mismatch > 1000:
        print(f"[❌] Error: Count difference ({mismatch:,}) exceeds threshold (1000)")
        if update_metrics:
            return mysql_count, mongo_count, mismatch
        raise AssertionError("Count difference between MySQL and MongoDB is too large")

    if is_in_sync:
        print("[✅] Perfect sync: MySQL and MongoDB counts match exactly")
    else:
        print(f"[⚠️] Sync mismatch: {mismatch:,} records difference (within acceptable range)")

    return mysql_count, mongo_count, mismatch


def run_metrics_server(port=8000, interval=30):
    """
    Run a Prometheus metrics server that continuously monitors sync status.

    Args:
        port: Port to expose metrics on (default: 8000)
        interval: Check interval in seconds (default: 30)
    """
    print(f"[🚀] Starting Prometheus metrics server on port {port}")
    print(f"[🔄] Checking sync status every {interval} seconds")
    print(f"[📊] Metrics available at http://localhost:{port}/metrics")

    start_http_server(port)

    try:
        while True:
            try:
                validate_counts(update_metrics=True)
                print(f"[⏰] Next check in {interval} seconds...\n")
            except Exception as e:
                print(f"[❌] Error during validation: {e}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[👋] Metrics server stopped")


if __name__ == "__main__":
    import sys

    if not MONGO_URI:
        raise RuntimeError("MONGODB_URI is not set for validation")

    # Check if running in metrics server mode
    if "--metrics-server" in sys.argv:
        port = int(os.getenv("METRICS_PORT", "8000"))
        interval = int(os.getenv("METRICS_CHECK_INTERVAL", "30"))
        run_metrics_server(port=port, interval=interval)
    else:
        # One-time validation check
        validate_counts()
