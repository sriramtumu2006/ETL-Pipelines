import time
from extract import fetch_all_cities
from transform import transform_raw_to_df
from load import load_to_supabase
from etl_analysis import run_analysis
from pathlib import Path

RAW_DIR = Path("data/raw")
STAGED_DIR = Path("data/staged")
OUTPUT_CSV = STAGED_DIR / "air_quality_transformed.csv"

def run_full_pipeline():
    print("\n===== STEP 1: EXTRACT =====")
    results = fetch_all_cities()  
    time.sleep(1)

    print("\n===== STEP 2: TRANSFORM =====")
    df = transform_raw_to_df(RAW_DIR)
    if df.empty:
        print("❌ No data to load. Exiting pipeline.")
        return
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Transformed CSV saved: {OUTPUT_CSV}")
    time.sleep(1)

    print("\n===== STEP 3: LOAD =====")
    load_to_supabase(OUTPUT_CSV, batch_size=200)
    time.sleep(1)

    print("\n===== STEP 4: ANALYSIS =====")
    run_analysis()

    print("\n✅ ETL Pipeline Completed Successfully!")

if __name__ == "__main__":
    run_full_pipeline()
