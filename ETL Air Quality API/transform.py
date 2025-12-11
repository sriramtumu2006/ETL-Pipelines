from __future__ import annotations
import json
from pathlib import Path
from typing import List, Dict
import pandas as pd

RAW_DIR = Path("data/raw")
STAGED_DIR = Path("data/staged")
STAGED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV = STAGED_DIR / "air_quality_transformed.csv"
REQUIRED_COLUMNS = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index"]

def compute_aqi(pm2_5: float) -> str:
    if pd.isna(pm2_5):
        return None
    if pm2_5 <= 50:
        return "Good"
    elif pm2_5 <= 100:
        return "Moderate"
    elif pm2_5 <= 200:
        return "Unhealthy"
    elif pm2_5 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

def compute_severity(row: pd.Series) -> float:
    return (
        row.get("pm2_5", 0) * 5 +
        row.get("pm10", 0) * 3 +
        row.get("nitrogen_dioxide", 0) * 4 +
        row.get("sulphur_dioxide", 0) * 4 +
        row.get("carbon_monoxide", 0) * 2 +
        row.get("ozone", 0) * 3
    )

def compute_risk(severity: float) -> str:
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"

def transform_raw_to_df(raw_dir: Path) -> pd.DataFrame:
    all_records: List[Dict] = []
    for file in raw_dir.glob("*_raw_*.json"):
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"⚠️ Could not read {file}: {e}")
            continue
        city_name = file.stem.split("_raw_")[0]
        hourly = data.get("hourly")
        if not hourly:
            print(f"⚠️ No 'hourly' data in {file}")
            continue
        timestamps = hourly.get("time", [])
        for i, ts in enumerate(timestamps):
            record = {"city": city_name, "time": pd.to_datetime(ts)}
            for col in REQUIRED_COLUMNS:
                values = hourly.get(col, [])
                record[col] = values[i] if i < len(values) else None

            all_records.append(record)
    df = pd.DataFrame(all_records)
    if df.empty:
        return df
    for col in REQUIRED_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=REQUIRED_COLUMNS, how="all")
    df["AQI"] = df["pm2_5"].apply(compute_aqi)
    df["severity"] = df.apply(compute_severity, axis=1)
    df["risk"] = df["severity"].apply(compute_risk)
    df["hour"] = df["time"].dt.hour
    return df

if __name__ == "__main__":
    print("Transforming raw JSON files into tabular CSV with feature engineering")
    df = transform_raw_to_df(RAW_DIR)
    if df.empty:
        print("❌ No data found to transform.")
    else:
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Transformation complete. CSV saved to: {OUTPUT_CSV}")
        print(df.head())