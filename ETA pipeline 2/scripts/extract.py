import os
import pandas as pd

def extract_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    source_path = os.path.join(
        base_dir,
        "WA_Fn-UseC_-Telco-Customer-Churn.csv"
    )
    if not os.path.exists(source_path):
        raise FileNotFoundError("❌ Churn dataset not found")
    df = pd.read_csv(source_path)
    raw_path = os.path.join(raw_dir, "churn_raw.csv")
    df.to_csv(raw_path, index=False)
    print(f"✅ Data extracted to: {raw_path}")
    return raw_path

if __name__ == "__main__":
    extract_data()
