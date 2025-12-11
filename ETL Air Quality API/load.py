from __future__ import annotations
import pandas as pd
import numpy as np
import time
from pathlib import Path
from typing import List, Dict
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
TRANSFORMED_CSV = Path("data/staged/air_quality_transformed.csv")
TABLE_NAME = "air_quality_data"
BATCH_SIZE = 200
MAX_RETRIES = 2

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    time TIMESTAMP,
    pm10 FLOAT,
    pm2_5 FLOAT,
    carbon_monoxide FLOAT,
    nitrogen_dioxide FLOAT,
    sulphur_dioxide FLOAT,
    ozone FLOAT,
    uv_index FLOAT,
    aqi_category TEXT,
    severity_score FLOAT,
    risk_flag TEXT,
    hour INTEGER
);
"""
def ensure_table():
    try:
        supabase.postgrest.rpc("execute_sql", {"query": CREATE_TABLE_SQL}).execute()
        print(f"🛠️ Ensured table exists: {TABLE_NAME}")
    except Exception as e:
        print("⚠️ Could not run CREATE TABLE via RPC, table may already exist.")
        print(e)

def prepare_records(df: pd.DataFrame) -> List[Dict]:
    records = df.replace({np.nan: None}).to_dict(orient="records")
    for r in records:
        if isinstance(r["time"], pd.Timestamp):
            r["time"] = r["time"].isoformat()
        r["aqi_category"] = r.pop("AQI")
        r["severity_score"] = r.pop("severity")
        r["risk_flag"] = r.pop("risk")
    return records

def insert_batch(batch: List[Dict], batch_index: int) -> int:
    attempt = 0
    while attempt <= MAX_RETRIES:
        try:
            res = supabase.table(TABLE_NAME).insert(batch).execute()
            print(f"✅ Batch {batch_index}: inserted {len(batch)} rows")
            return len(batch)
        except Exception as e:
            attempt += 1
            print(f"⚠️ Batch {batch_index} failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            time.sleep(1.5)
    print(f"❌ Batch {batch_index} failed permanently.")
    return 0

def load_to_supabase(csv_path, batch_size=200):
    if not TRANSFORMED_CSV.exists():
        print(f"❌ No transformed CSV found at {TRANSFORMED_CSV}")
        return
    df = pd.read_csv(csv_path)
    ensure_table()
    records = prepare_records(df)
    total_inserted = 0
    batches: List[List[Dict]] = [records[i:i + BATCH_SIZE] for i in range(0, len(records), batch_size)]
    for idx, batch in enumerate(batches, start=1):
        total_inserted += insert_batch(batch, idx)
    print("\n📌 Load Summary")
    print(f"Total rows in CSV     : {len(df)}")
    print(f"Total rows inserted   : {total_inserted}")
    print(f"Rows skipped / failed : {len(df) - total_inserted}")

if __name__ == "__main__":
    print("🚀 Starting Supabase load process...")
    load_to_supabase(TRANSFORMED_CSV, batch_size=200)
