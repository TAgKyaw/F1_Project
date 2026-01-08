# BRONZE LAYER INGESTION
# ----------------------
# GOAL: Load raw CSV data from the source directory and convert it to Parquet format for efficient storage.
# ANALYSIS: No transformation is performed here; this is a pure data dump to the "Bronze" (Raw) layer.
# OUTPUT: .parquet files in `data/bronze/` corresponding to each source CSV.

# imports 
import pandas as pd
from pathlib import Path
from config import BRONZE_PATH, F1_PATH

# 1. READ RAW DATA
# ----------------
# Loading the 2024 season datasets and historical context files.
print("Loading raw CSV files...")
constructor_standings_2024 = pd.read_csv(f'{F1_PATH}/f1_2024_constructor_standings.csv')
driver_standings_2024 = pd.read_csv(f'{F1_PATH}/f1_2024_driver_standings.csv')
race_results_2024 = pd.read_csv(f'{F1_PATH}/f1_2024_race_results.csv')
circuits_metadata = pd.read_csv(f'{F1_PATH}/f1_circuits_metadata.csv')
historical_drivers = pd.read_csv(f'{F1_PATH}/f1_historical_drivers.csv')
qualifying_results_2024 = pd.read_csv(f'{F1_PATH}/f1_qualifying_results_2024.csv')

# Dataset dictionary for iteration
datasets = {
    "constructor_standings_2024": constructor_standings_2024,
    "driver_standings_2024": driver_standings_2024,
    "race_results_2024": race_results_2024,
    "circuits_metadata": circuits_metadata,
    "historical_drivers": historical_drivers,
    "qualifying_results_2024": qualifying_results_2024
}

# 2. SAVE TO BRONZE LAYER
# -----------------------
# Ensure the bronze directory exists
Path(BRONZE_PATH).mkdir(parents=True, exist_ok=True)
   
print(f"Saving {len(datasets)} files to Bronze Layer ({BRONZE_PATH})...")
for name, df in datasets.items():
    try:
        # Prefer Parquet for performance (faster reads, type preservation)
        df.to_parquet(f"{BRONZE_PATH}/{name}.parquet", index=False)
        print(f"-> Saved {name}.parquet")
    except ImportError:
        # Fallback if pyarrow/fastparquet is missing
        print(f"Parquet engine missing, saving {name} as CSV instead.")
        df.to_csv(f"{BRONZE_PATH}/{name}.csv", index=False)

print("Bronze data saving successful!")

