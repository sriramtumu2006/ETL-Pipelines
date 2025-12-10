import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("❌ Missing Supabase credentials")
    return create_client(url, key)

def load_to_supabase(staged_path, table_name="churn_data"):
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), staged_path)
        )
    print(f"🔍 Loading file: {staged_path}")
    if not os.path.exists(staged_path):
        print("❌ Staged file not found")
        return
    supabase = get_supabase_client()
    df = pd.read_csv(staged_path)
    df.columns = [c.strip().lower() for c in df.columns] # Normalize column names: lowercase + strip spaces
    df = df.where(pd.notnull(df), None)     # Replace NaN with None for Supabase
    batch_size = 50
    total_rows = len(df)
    print(f"📊 Inserting {total_rows} rows into '{table_name}'")
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size]
        records = batch.to_dict("records")
        supabase.table(table_name).insert(records).execute()
        print(f"✅ Inserted rows {i+1}-{min(i+batch_size, total_rows)}")
    print("🎯 Load completed successfully")

if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "churn_staged.csv")
    load_to_supabase(staged_csv_path)
