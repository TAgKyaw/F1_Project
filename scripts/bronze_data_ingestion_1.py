# imports 
import pandas as pd
from pathlib import Path
from config import BRONZE_PATH, F1_PATH

constructor_standings_2024 = pd.read_csv(f'{F1_PATH}/f1_2024_constructor_standings.csv')
driver_standings_2024 = pd.read_csv(f'{F1_PATH}/f1_2024_driver_standings.csv')
race_results_2024 = pd.read_csv(f'{F1_PATH}/f1_2024_race_results.csv')
circuits_metadata = pd.read_csv(f'{F1_PATH}/f1_circuits_metadata.csv')
historical_drivers = pd.read_csv(f'{F1_PATH}/f1_historical_drivers.csv')
qualifying_results_2024 = pd.read_csv(f'{F1_PATH}/f1_qualifying_results_2024.csv')

# dataset dictionary

datasets = {
    "constructor_standings_2024": constructor_standings_2024,
    "driver_standings_2024": driver_standings_2024,
    "race_results_2024": race_results_2024,
    "circuits_metadata": circuits_metadata,
    "historical_drivers": historical_drivers,
    "qualifying_results_2024": qualifying_results_2024
}

# for name,df in datasets.items():
#     print(name)
#     df.info()
#     print()

Path(BRONZE_PATH).mkdir(parents=True, exist_ok=True)
   
for name, df in datasets.items():
    try:
        df.to_parquet(f"{BRONZE_PATH}/{name}.parquet", index=False)
        print(f"Saved {name} as Parquet.")
    except ImportError:
        print(f"Parquet engine missing, saving {name} as CSV instead.")
        df.to_csv(f"{BRONZE_PATH}/{name}.csv", index=False)
print("Bronze data saving successful!")

