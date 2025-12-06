# imports
import pandas as pd
from pathlib import Path
from config import BRONZE_PATH, SILVER_PATH

# Cleaning, Standardizing Bronze Tables to Silver Layer

Path(SILVER_PATH).mkdir(parents=True, exist_ok=True)

# Loading Bronze Data
def load_bronze(name):
    try:
        return pd.read_parquet(f"{BRONZE_PATH}/{name}.parquet")
    except (ImportError, FileNotFoundError):
        print(f"Parquet not available, loading {name} from CSV.")
        return pd.read_csv(f"{BRONZE_PATH}/{name}.csv")

# Standardizing_columns to snake_case 
def standardize_columns(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

constructor_standings = standardize_columns(load_bronze("constructor_standings_2024"))
driver_standings = standardize_columns(load_bronze("driver_standings_2024"))
race_results = standardize_columns(load_bronze("race_results_2024"))
circuits_metadata = standardize_columns(load_bronze("circuits_metadata"))
historical_drivers = standardize_columns(load_bronze("historical_drivers"))
qualifying_results = standardize_columns(load_bronze("qualifying_results_2024"))

# Creating a iteratable dataset
silver_datasets = {
    "constructor_standings": constructor_standings,
    "driver_standings": driver_standings,
    "race_results": race_results,
    "circuits_metadata": circuits_metadata,
    "historical_drivers": historical_drivers,
    "qualifying_results": qualifying_results
}

# --- Handle might be missing qualifying times ---
qualifying_results['q2_time'] = qualifying_results['q2_time'].fillna("00:00.000")
qualifying_results['q3_time'] = qualifying_results['q3_time'].fillna("00:00.000")

# Checking columns standardization 
# for name, df in silver_datasets.items():
#     print(f"Dataset: {name}")
#     print(df.columns)
#     print()


for name, df in silver_datasets.items():
    try:
        df.to_parquet(f"{SILVER_PATH}/{name}.parquet", index=False)
        print(f"Saved {name} to silver layer as Parquet.")
    except ImportError:
        print(f"Parquet engine missing, saving {name} to silver layer as CSV instead.")
        df.to_csv(f"{SILVER_PATH}/{name}.csv", index=False)

print("Silver layer tables saved successfully.")
print(race_results.head(3))