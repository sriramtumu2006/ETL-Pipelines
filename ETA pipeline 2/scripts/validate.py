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

def validate_load(csv_path, table_name="churn_data"):
    if not os.path.exists(csv_path):
        print("❌ CSV file not found:", csv_path)
        return
    df_csv = pd.read_csv(csv_path)
    supabase = get_supabase_client()
    res = supabase.table(table_name).select("*").execute()
    df_db = pd.DataFrame(res.data)
    print("\n🔹 Validation Summary 🔹")

    # 1️⃣ Check for missing values
    missing_columns = ["tenure", "MonthlyCharges", "TotalCharges"]
    for col in missing_columns:
        col_lower = col.lower()
        if col_lower in df_db.columns:
            missing_count = df_db[col_lower].isnull().sum()
            print(f"{col}: Missing values in table = {missing_count}")
        else:
            print(f"⚠️ Column {col} not found in DB")

    # 2️⃣ Check unique row count matches CSV
    csv_rows = len(df_csv)
    db_rows = len(df_db)
    print(f"Original CSV rows: {csv_rows}")
    print(f"Supabase table rows: {db_rows}")
    print("Row count match:", "✅" if csv_rows == db_rows else "❌")

    # 3️⃣ Check all tenure_group segments exist
    expected_tenure_groups = {"New", "Regular", "Loyal", "Champion"}
    if "tenure_group" in df_db.columns:
        db_tenure_groups = set(df_db["tenure_group"].dropna().unique())
        missing_groups = expected_tenure_groups - db_tenure_groups
        print("Tenure groups present:", db_tenure_groups)
        print("Missing tenure groups:", missing_groups if missing_groups else "None")
    else:
        print("⚠️ tenure_group column not found")

    # 4️⃣ Check all MonthlyCharges_group segments exist
    expected_charge_groups = {"Low", "Medium", "High"}
    if "monthlycharges_group" in df_db.columns:
        db_charge_groups = set(df_db["monthlycharges_group"].dropna().unique())
        missing_charge_groups = expected_charge_groups - db_charge_groups
        print("MonthlyCharges groups present:", db_charge_groups)
        print("Missing MonthlyCharges groups:", missing_charge_groups if missing_charge_groups else "None")
    else:
        print("⚠️ monthlycharges_group column not found")

    # 5️⃣ Check contract_type_code values
    expected_contract_codes = {0,1,2}
    if "contract_type_code" in df_db.columns:
        db_codes = set(df_db["contract_type_code"].dropna().astype(int).unique())
        invalid_codes = db_codes - expected_contract_codes
        print("Contract codes present:", db_codes)
        print("Invalid contract codes:", invalid_codes if invalid_codes else "None")
    else:
        print("⚠️ contract_type_code column not found")
    print("\n🎯 Validation completed")


if __name__ == "__main__":
    staged_csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),"data","staged","churn_staged.csv")
    validate_load(staged_csv_path)
