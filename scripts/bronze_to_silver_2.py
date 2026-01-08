# BRONZE TO SILVER TRANSFORMATION
# -------------------------------
# GOAL: Clean and standardize raw "Bronze" data into a trusted "Silver" layer.
# ANALYSIS:
#   1. Standardize column names to snake_case (e.g., "Driver Name" -> "driver_name").
#   2. Initial data type handling and value imputation (e.g., Qualifying times).
# OUTPUT: .parquet files in `data/silver/` ready for joining.

# imports
import pandas as pd
from pathlib import Path
from config import BRONZE_PATH, SILVER_PATH

# 1. SETUP
# --------
# Ensure Silver directory exists
Path(SILVER_PATH).mkdir(parents=True, exist_ok=True)

# Loading Bronze Data
def load_bronze(name):
    """Load parquet (preferred) or CSV from bronze layer."""
    try:
        return pd.read_parquet(f"{BRONZE_PATH}/{name}.parquet")
    except (ImportError, FileNotFoundError):
        print(f"Parquet not available, loading {name} from CSV.")
        return pd.read_csv(f"{BRONZE_PATH}/{name}.csv")

# 2. STANDARDIZATION
# ------------------
def standardize_columns(df):
    """Clean column names: lowercase, strip space, replace space with underscore."""
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

print("Loading and Standardizing Bronze Data...")
constructor_standings = standardize_columns(load_bronze("constructor_standings_2024"))
driver_standings = standardize_columns(load_bronze("driver_standings_2024"))
race_results = standardize_columns(load_bronze("race_results_2024"))
circuits_metadata = standardize_columns(load_bronze("circuits_metadata"))
historical_drivers = standardize_columns(load_bronze("historical_drivers"))
qualifying_results = standardize_columns(load_bronze("qualifying_results_2024"))

# Creating a iteratable dataset dictionary
silver_datasets = {
    "constructor_standings": constructor_standings,
    "driver_standings": driver_standings,
    "race_results": race_results,
    "circuits_metadata": circuits_metadata,
    "historical_drivers": historical_drivers,
    "qualifying_results": qualifying_results
}

# 3. SPECIFIC CLEANING RULES
# --------------------------
# Handle missing qualifying times (drivers who didn't reach Q2 or Q3)
print("Applying cleaning rules...")
qualifying_results['q2_time'] = qualifying_results['q2_time'].fillna("00:00.000")
qualifying_results['q3_time'] = qualifying_results['q3_time'].fillna("00:00.000")

# Checking columns standardization 
# for name, df in silver_datasets.items():
#     print(f"Dataset: {name}")
#     print(df.columns)
#     print()

# 4. SAVE TO SILVER LAYER
# -----------------------
print(f"Saving {len(silver_datasets)} tables to Silver Layer ({SILVER_PATH})...")

for name, df in silver_datasets.items():
    try:
        df.to_parquet(f"{SILVER_PATH}/{name}.parquet", index=False)
        print(f"-> Saved {name} to silver layer.")
    except ImportError:
        print(f"Parquet engine missing, saving {name} to silver layer as CSV instead.")
        df.to_csv(f"{SILVER_PATH}/{name}.csv", index=False)

print("Silver layer tables saved successfully.")
print("\nSample Preview (Race Results):")
print(race_results.head(3))