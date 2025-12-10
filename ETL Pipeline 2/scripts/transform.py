import os
import pandas as pd
import numpy as np

def transform_data(raw_path):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
    df = pd.read_csv(raw_path)
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")
    df["TotalCharges"]=df["TotalCharges"].fillna(df["TotalCharges"].median())
    bins = [0, 12, 36, 60, float("inf")]
    labels = ["New", "Regular", "Loyal", "Champion"]
    df["tenure_group"] = pd.cut(df["tenure"], bins=bins, labels=labels)
    conditions = [
        df["MonthlyCharges"] < 30,
        df["MonthlyCharges"].between(30, 70),
        df["MonthlyCharges"] > 70
    ]
    choices = ["Low", "Medium", "High"]
    df["MonthlyCharges_group"] = np.select(
        conditions, choices, default="Unknown"
    )
    df["has_internet_service"] = df["InternetService"].map(
        {"DSL": 1, "Fiber optic": 1, "No": 0}
    )
    df["is_multi_line_user"] = np.where(
        df["MultipleLines"] == "Yes", 1, 0
    )
    df["contract_type_code"] = df["Contract"].map(
        {"Month-to-month": 0, "One year": 1, "Two year": 2}
    )
    staged_path = os.path.join(staged_dir, "churn_staged.csv")
    df.to_csv(staged_path, index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path

if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_data(raw_path)
