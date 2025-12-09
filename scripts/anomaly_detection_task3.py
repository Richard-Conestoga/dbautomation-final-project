import math
import pandas as pd
import pymysql
from sklearn.ensemble import IsolationForest


# 1️⃣ MySQL connection (Docker)
def get_connection():
    return pymysql.connect(
        host="127.0.0.1",
        port=5510,
        user="appuser",
        password="Life8574!",
        database="nyc311"
    )


# 2️⃣ Load NYC311 data
def load_data():
    conn = get_connection()
    query = "SELECT * FROM service_requests;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# 3️⃣ Detect anomalies
def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    anomalies_list = []

    # (a) Missing latitude/longitude → missing_location
    if "latitude" in df.columns and "longitude" in df.columns:
        missing_loc = df[df[["latitude", "longitude"]].isna().any(axis=1)].copy()
        if not missing_loc.empty:
            missing_loc["anomaly_reason"] = "missing_location"
            anomalies_list.append(missing_loc)

    # (b) Long open cases (created_date long ago, closed_date is NULL) → long_open_case
    if "created_date" in df.columns and "closed_date" in df.columns:
        # Ensure datetime
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
        df["closed_date"] = pd.to_datetime(df["closed_date"], errors="coerce")

        now = pd.Timestamp.now()
        mask = df["created_date"].notna() & df["closed_date"].isna()
        long_open = df[mask & ((now - df["created_date"]).dt.days > 90)].copy()
        if not long_open.empty:
            long_open["anomaly_reason"] = "long_open_case"
            anomalies_list.append(long_open)

    # (c) Location outliers using IsolationForest → location_outlier
    numeric_cols = [c for c in ["latitude", "longitude"] if c in df.columns]
    if numeric_cols:
        df_num = df[numeric_cols].copy()
        df_num = df_num.fillna(0.0)

        try:
            model = IsolationForest(contamination=0.3, random_state=42)
            labels = model.fit_predict(df_num)
            ml_outliers = df[labels == -1].copy()
            if not ml_outliers.empty:
                ml_outliers["anomaly_reason"] = "location_outlier"
                anomalies_list.append(ml_outliers)
        except Exception as e:
            print(f"[WARN] IsolationForest failed: {e}")

    if not anomalies_list:
        print("No anomalies detected.")
        return pd.DataFrame()

    # Combine, deduplicate by unique_key if exists
    combined = pd.concat(anomalies_list, ignore_index=True)

    if "unique_key" in combined.columns:
        combined = combined.drop_duplicates(subset=["unique_key"])
    else:
        combined = combined.drop_duplicates()

    return combined


# Helper: convert NaN / NaT to None so MySQL gets NULL
def to_null(value):
    # pandas + float NaN
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except Exception:
        pass

    # pandas isna (covers NaT, etc.)
    if pd.isna(value):
        return None

    return value


# 4️⃣ Save anomalies to MySQL
def save_anomalies(anomalies_df: pd.DataFrame):
    if anomalies_df.empty:
        print("No anomalies to save.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    # Create table if needed
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS anomalies (
            unique_key BIGINT PRIMARY KEY,
            anomaly_reason VARCHAR(255),
            created_date DATETIME,
            closed_date DATETIME,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6)
        );
        """
    )
    conn.commit()

    insert_sql = """
        INSERT INTO anomalies
        (unique_key, anomaly_reason, created_date, closed_date, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            anomaly_reason = VALUES(anomaly_reason),
            created_date = VALUES(created_date),
            closed_date = VALUES(closed_date),
            latitude = VALUES(latitude),
            longitude = VALUES(longitude);
    """

    rows = []
    for _, r in anomalies_df.iterrows():
        rows.append(
            (
                int(r["unique_key"]),
                to_null(r.get("anomaly_reason")),
                to_null(r.get("created_date")),
                to_null(r.get("closed_date")),
                to_null(r.get("latitude")),
                to_null(r.get("longitude")),
            )
        )

    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"🚨 {len(rows)} anomalies saved into 'anomalies' table!")


# 5️⃣ Main
def main():
    df = load_data()
    print(f"Loaded {len(df)} rows from service_requests")

    anomalies = detect_anomalies(df)
    print(f"Detected {len(anomalies)} anomalies")

    save_anomalies(anomalies)
    print("✨ Task 3 anomaly detection completed!")


if __name__ == "__main__":
    main()
