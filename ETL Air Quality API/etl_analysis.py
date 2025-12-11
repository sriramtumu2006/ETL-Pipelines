from __future__ import annotations
import os
from pathlib import Path
from typing import Tuple
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = os.getenv("SUPABASE_TABLE", "air_quality_data")
PROCESSED_DIR = Path("data/processed")
PLOTS_DIR = Path("data/processed/plots")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_table(table_name: str) -> pd.DataFrame:
    """Fetch entire table from Supabase and return a DataFrame."""
    print(f"Fetching table: {table_name} from Supabase...")
    resp = supabase.table(table_name).select("*").execute()
    data = resp.data if hasattr(resp, "data") else resp
    df = pd.DataFrame(data)
    if df.empty:
        print("Warning: fetched dataframe is empty")
        return df
    df.columns = [c.lower() for c in df.columns]
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df

def compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """Compute required KPI metrics and return as a single-row DataFrame (for easy CSV saving)."""
    numeric_cols = ["pm2_5", "pm10", "severity_score"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    kpis = {}
    if "pm2_5" in df.columns:
        avg_pm25 = df.groupby("city")["pm2_5"].mean().dropna()
        if not avg_pm25.empty:
            top_city_pm25 = avg_pm25.idxmax()
            kpis["city_highest_avg_pm2_5"] = top_city_pm25
            kpis["city_highest_avg_pm2_5_value"] = avg_pm25.max()
        else:
            kpis["city_highest_avg_pm2_5"] = None
            kpis["city_highest_avg_pm2_5_value"] = None
    else:
        kpis["city_highest_avg_pm2_5"] = None
        kpis["city_highest_avg_pm2_5_value"] = None
    if "severity_score" in df.columns:
        avg_sev = df.groupby("city")["severity_score"].mean().dropna()
        if not avg_sev.empty:
            top_city_sev = avg_sev.idxmax()
            kpis["city_highest_avg_severity"] = top_city_sev
            kpis["city_highest_avg_severity_value"] = avg_sev.max()
        else:
            kpis["city_highest_avg_severity"] = None
            kpis["city_highest_avg_severity_value"] = None
    else:
        kpis["city_highest_avg_severity"] = None
        kpis["city_highest_avg_severity_value"] = None
    if "risk_flag" in df.columns:
        risk_counts = df["risk_flag"].fillna("Unknown").value_counts()
        total = risk_counts.sum()
        kpis.update({f"pct_{r.lower().replace(' ','_')}": (risk_counts.get(r, 0) / total) * 100 for r in ["High Risk", "Moderate Risk", "Low Risk"]})
    else:
        kpis["pct_high_risk"] = kpis["pct_moderate_risk"] = kpis["pct_low_risk"] = None

    # Hour of day with worst AQI (use avg pm2_5 as proxy)
    if "time" in df.columns and "pm2_5" in df.columns:
        df = df.dropna(subset=["time", "pm2_5"])  # ensure valid values
        df["hour"] = df["time"].dt.hour
        hour_avg = df.groupby("hour")["pm2_5"].mean()
        worst_hour = int(hour_avg.idxmax()) if not hour_avg.empty else None
        kpis["hour_of_day_worst_aqi"] = worst_hour
        kpis["hour_of_day_worst_aqi_value"] = float(hour_avg.max()) if not hour_avg.empty else None
    else:
        kpis["hour_of_day_worst_aqi"] = None
        kpis["hour_of_day_worst_aqi_value"] = None

    return pd.DataFrame([kpis])


# ----------------------
# City pollution trend report
# ----------------------

def create_pollution_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Return a tidy DataFrame with columns: city, time, pm2_5, pm10, ozone"""
    cols = [c for c in ["city", "time", "pm2_5", "pm10", "ozone"] if c in df.columns]
    trends = df.loc[:, cols].copy()
    trends = trends.dropna(subset=[c for c in ["time"] if c in trends.columns])
    trends = trends.sort_values(["city", "time"]) if "city" in trends.columns and "time" in trends.columns else trends
    return trends


# ----------------------
# Export helpers
# ----------------------

def export_csvs(kpi_df: pd.DataFrame, risk_dist: pd.DataFrame, trends: pd.DataFrame) -> None:
    kpi_path = PROCESSED_DIR / "summary_metrics.csv"
    risk_path = PROCESSED_DIR / "city_risk_distribution.csv"
    trends_path = PROCESSED_DIR / "pollution_trends.csv"

    kpi_df.to_csv(kpi_path, index=False)
    risk_dist.to_csv(risk_path, index=False)
    trends.to_csv(trends_path, index=False)

    print(f"Saved summary metrics -> {kpi_path}")
    print(f"Saved city risk distribution -> {risk_path}")
    print(f"Saved pollution trends -> {trends_path}")


# ----------------------
# Visualizations
# ----------------------

def plot_histogram_pm25(df: pd.DataFrame) -> Path:
    path = PLOTS_DIR / "histogram_pm2_5.png"
    if "pm2_5" not in df.columns:
        print("pm2_5 column missing; skipping histogram")
        return path
    plt.figure()
    df["pm2_5"].dropna().plot(kind="hist", bins=40)
    plt.title("Histogram of PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"Saved histogram -> {path}")
    return path


def plot_bar_risk_per_city(df: pd.DataFrame) -> Path:
    path = PLOTS_DIR / "bar_risk_per_city.png"
    if "city" not in df.columns or "risk_flag" not in df.columns:
        print("city or risk_flag missing; skipping bar chart")
        return path
    pivot = df.groupby(["city", "risk_flag"]).size().unstack(fill_value=0)
    plt.figure()
    pivot.plot(kind="bar", stacked=False)
    plt.title("Risk Flags per City")
    plt.ylabel("Count")
    plt.xlabel("City")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"Saved bar chart -> {path}")
    return path


def plot_line_hourly_pm25(df: pd.DataFrame) -> Path:
    path = PLOTS_DIR / "line_hourly_pm2_5_trends.png"
    if "time" not in df.columns or "pm2_5" not in df.columns:
        print("Missing columns for hourly PM2.5 line chart; skipping")
        return path
    df_clean = df.dropna(subset=["time", "pm2_5"]).copy()
    df_clean["time"] = pd.to_datetime(df_clean["time"])
    # Resample to hourly across all cities combined (or average per hour)
    df_clean.set_index("time", inplace=True)
    hourly = df_clean["pm2_5"].resample("H").mean()
    plt.figure()
    hourly.plot()
    plt.title("Hourly PM2.5 (global average)")
    plt.xlabel("Time")
    plt.ylabel("PM2.5")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"Saved line chart -> {path}")
    return path


def plot_scatter_severity_vs_pm25(df: pd.DataFrame) -> Path:
    path = PLOTS_DIR / "scatter_severity_vs_pm2_5.png"
    if "severity_score" not in df.columns or "pm2_5" not in df.columns:
        print("Missing columns for scatter plot; skipping")
        return path
    plt.figure()
    plt.scatter(df["pm2_5"], df["severity_score"], alpha=0.6)
    plt.title("Severity Score vs PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Severity Score")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"Saved scatter plot -> {path}")
    return path


# ----------------------
# Main analysis function
# ----------------------

def run_analysis():
    df = fetch_table(TABLE_NAME)
    if df.empty:
        print("No data available to analyze. Exiting.")
        return

    # Compute KPIs
    kpi_df = compute_kpis(df)

    # City risk distribution (counts + percent)
    if "risk_flag" in df.columns and "city" in df.columns:
        risk_counts = df.groupby(["city", "risk_flag"]).size().reset_index(name="count")
        total_by_city = df.groupby("city").size().reset_index(name="total")
        risk_dist = risk_counts.merge(total_by_city, on="city")
        risk_dist["percent"] = risk_dist["count"] / risk_dist["total"] * 100
    else:
        risk_dist = pd.DataFrame()

    # Pollution trends
    trends = create_pollution_trends(df)

    # Export CSVs
    export_csvs(kpi_df, risk_dist, trends)

    # Visualizations
    plot_histogram_pm25(df)
    plot_bar_risk_per_city(df)
    plot_line_hourly_pm25(df)
    plot_scatter_severity_vs_pm25(df)

    print("Analysis complete.")


if __name__ == "__main__":
    run_analysis()
