# DATA PROCESSING

# Datasets from the silver layer will now be merged for data engineering 
# and save in the gold layer.

# Imports and Setups
import pandas as pd
from pathlib import Path
from config import SILVER_PATH, GOLD_PATH

# Create gold directory at project root if it doesn't exist
Path(GOLD_PATH).mkdir(parents=True, exist_ok=True)

# Load Silver datasets (Parquet with CSV Callback)
def load_silver(name):
    try:
        return pd.read_parquet(f"{SILVER_PATH}/{name}.parquet")
    except (ImportError, FileNotFoundError):
        print(f"Parquet not available for {name}, loading CSV.")
        return pd.read_csv(f"{SILVER_PATH}/{name}.csv")

# SAVING TO GOLD LAYER
def save_to_gold(master_table):
    try:
        master_table.to_parquet(f"{GOLD_PATH}/master_table.parquet", index=False)
        print("✅ Master table saved as Parquet to data/gold/")
    except ImportError:
        print("Parquet not available. Saving as CSV instead.")
        master_table.to_csv(f"{GOLD_PATH}/master_table.csv", index=False)

# Row validation for explosions
def df_shapes(main, merged):
    # Final Row Validation
    # Checking if any one-to-many explosions
    # Checking if any accidental Cartesian joins
    expected_rows = main.shape[0] # race_results - > the main backbone table
    actual_rows = merged.shape[0] # master_table - > Merged with all tables
    print("Expected Rows:", expected_rows)
    print("Actual Rows After All Joins:", actual_rows)

    if expected_rows != actual_rows:
        print("⚠️ WARNING: Row count mismatch! Possible join explosion.")
    else:
        print("✅ Row count is correct. No duplication detected.")

# All Functions ended ============================================

race_results = load_silver("race_results")
qualifying_results = load_silver("qualifying_results")
driver_standings = load_silver("driver_standings")
constructor_standings = load_silver("constructor_standings")
circuits_metadata = load_silver("circuits_metadata")
historical_drivers = load_silver("historical_drivers")

# Sanity Check Before Any merging
print("Race Results Shape:", race_results.shape)
print("Qualifying Shape:", qualifying_results.shape)
print("Driver Standings Shape:", driver_standings.shape)
print("Constructor Standings Shape:", constructor_standings.shape)
print("Circuits Metadata Shape:", circuits_metadata.shape)
print("Historical Drivers Shape:", historical_drivers.shape)
# Confirming expected row counts before joins
# Race Results Shape: (480, 16)
# Qualifying Shape: (480, 14)
# Driver Standings Shape: (480, 13)
# Constructor Standings Shape: (240, 13)
# Circuits Metadata Shape: (24, 14)
# Historical Drivers Shape: (30, 16)

# MASTER MERGE FUNCTION BEGINS HERE =================================

# Base Table (Race Results = Backbone)
# Check in documentation why it is a backbone table
master_table = race_results.copy()
print("Base Master Table Shape:", master_table.shape)
# One row = one driver in one race
# Output: Base Master Table Shape: (480, 16)

# Merging with Qualifying results
master_table = master_table.merge(
    qualifying_results,
    on=["race_id", "driver_name"],
    how="left",
    suffixes=("", "_qual")
)

print("After Qualifying Merge:", master_table.shape)
# Adds: qualifying_position, q1_time, q2_time, q3_time, best_time, gap_to_pole
# Output: After Qualifying Merge: (480, 28)

# Merging with Circuits Metadata
master_table = master_table.merge(
    circuits_metadata,
    left_on="circuit",
    right_on="circuit_name",
    how="left"
)

print("After Circuit Metadata Merge:", master_table.shape)

# Adds: track length, turns, elevation, DRS zones, lap record info
# Output: After Circuit Metadata Merge: (480, 42)

# Merging with Driver Standings
master_table['race_round'] = master_table['race_id']

master_table = master_table.merge(
    driver_standings,
    on=["race_round", "driver_name"],
    how="left",
    suffixes=("", "_driver_standings")
)

print("After Driver Standings Merge:", master_table.shape)

# Adds: season momentum, total points so far, wins, podiums, DNFs, etc.
# Time-safe. No leakage.
# Output: After Driver Standings Merge: (480, 54)

# Merging with Constructor Standings
master_table = master_table.merge(
    constructor_standings,
    left_on=["race_round", "team"],
    right_on=["race_round", "constructor"],
    how="left",
    suffixes=("", "_constructor")
)

print("After Constructor Standings Merge:", master_table.shape)

# Adds: team, team wins, DNFs, podium power
# Output: After Constructor Standings Merge: (480, 66)

# Merging with Historical Drivers data

master_table = master_table.merge(
    historical_drivers,
    on="driver_name",
    how="left",
    suffixes=("", "_career")
)

print("After Historical Drivers Merge:", master_table.shape)
# Adds: experience, career win rate, championship count
# Static background info → safe global merge
# Output: After Historical Drivers Merge: (480, 81)

# All MERGING COMPLETED ====================================

# Adding is_podium for Feature Engineering and Gold EDA
master_table["is_podium"] = (master_table["position"] <= 3).astype(int)
print("Is_Podium")
print(master_table["is_podium"].head(3))

# ======== POST-MERGE VALIDATION:ROW CHECKS =============================

# Remove rows with missing "position"
master_table = master_table.dropna(subset=["position"])
df_shapes(race_results, master_table)
# Output:
    # Expected Rows: 480
    # Actual Rows After All Joins: 480
    # ✅ Row count is correct. No duplication detected.

# ======== FEATURE REVIEW ===============================================

# Quick Column Review
print(master_table.columns.tolist())
# All of the Race features, Qualifying features, Circuit features, Driver season features,
# Constructor features, and Career features

# Saving the final validated masters data to gold folder for EDA and Feature Engineering
save_to_gold(master_table)

# -------------------------------------------------------------

#### WHAT We HAVE IN MASTER TABLE (!!)

# ✅ A true Gold-level analytical dataset; 
# One row = one driver in one race With:
# 1. Qualifying context
# 2. Circuit difficulty
# 3. Season momentum
# 4. Team strength
# 5. Career experience

## ✅ Master Data Model is now validated and operational.
### This Model is available for:
# # TRUE EDA, # Feature Engineering, and # ML 
### ----------------------------------------- ###
#### NEXT STAGE ####
#### EDA
# 1. Distributions
# 2. Correlations
# 3. Consistency analysis
# 4. Qualifying vs finish
# 5. Circuit difficulty
# ------------------------------------------------ #

#### FEATURE ENGINEERING
# 1. Win-rate per driver
# 2. Rolling averages
# 3. DNF ratios
# 4. Qualifying delta features