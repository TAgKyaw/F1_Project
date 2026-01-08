# FEATURE ENGINEERING
# GOAL: Create advanced metrics for Driver Performance & Consistency Analysis

import pandas as pd
import numpy as np
from pathlib import Path
from config import GOLD_PATH

# 1. Load Validated Master Table
MASTER_PATH = f"{GOLD_PATH}/master_table.parquet"
OUTPUT_PATH = f"{GOLD_PATH}/master_features.parquet"

print("Loading Master Data...")
df = pd.read_parquet(MASTER_PATH)
print("Initial Shape:", df.shape)

# Sort by Date/Race Order to ensure rolling calculations are correct
# NOTE: Dataset is confirmed to be 2024. Adding year for consistency.
df["year"] = 2024
df = df.sort_values(by=["year", "race_round"])

# ==============================================================================
# 2. FEATURE: ROLLING AVERAGES (Performance Trend)
# ==============================================================================
# OBJECTIVE: Smooth out race-by-race noise to see the underlying form of a driver.
# LOGIC: Calculate average finishing position over the last 5 races.
# OUTPUT CHECK: 
#   - New Column: 'rolling_avg_5_races'
#   - Should not encompass future races (shift operations might be needed if not using closed windows properly).
#   - First 4 races for a driver should be NaN or partial averages.

print("\n--- Generating Rolling Averages ---")
df["rolling_avg_5_races"] = (
    df.groupby("driver_name")["position"]
    .transform(lambda x: x.rolling(window=5, min_periods=1).mean())
)

print(df[["driver_name", "year", "race_round", "position", "rolling_avg_5_races"]].head(10))
# Check: Is the 5th value roughly the average of the first 5? Yes.


# ==============================================================================
# 3. FEATURE: CUMULATIVE WIN & PODIUM RATES (Season Level)
# ==============================================================================
# OBJECTIVE: Measure dominance throughout a season.
# LOGIC: (Cumulative Wins / Races Participating so far)
# OUTPUT CHECK:
#   - New Columns: 'wins_cumulative', 'races_cumulative', 'season_win_rate'
#   - Win rate should be between 0 and 1.
#   - Should reset every season.

print("\n--- Generating Season Win/Podium Rates ---")
# Helper for cumulative count
df["is_win"] = (df["position"] == 1).astype(int)

# Group by Driver AND Year (Season-level stats)
season_grp = df.groupby(["driver_name", "year"])

df["races_season_cumulative"] = season_grp.cumcount() + 1
df["wins_season_cumulative"] = season_grp["is_win"].cumsum()
df["podiums_season_cumulative"] = season_grp["is_podium"].cumsum()

# Calculate Rates
df["season_win_rate"] = df["wins_season_cumulative"] / df["races_season_cumulative"]
df["season_podium_rate"] = df["podiums_season_cumulative"] / df["races_season_cumulative"]

print(df[["driver_name", "year", "is_win", "season_win_rate"]].tail())
# Check: Does a driver with 0 wins have 0 rate? Does a driver with all wins have 1.0?


# ==============================================================================
# 4. FEATURE: DNF ANALYSIS (Reliability Score)
# ==============================================================================
# OBJECTIVE: Quantify how reliable a car/driver pair is.
# LOGIC: Identify 'Did Not Finish'. We don't have a direct 'status' column in master yet, 
#        but we can infer from positionText or points if available, or assume 'position' 
#        is valid only for finishers.
#        *NOTE*: In the master merge, we did `dropna(subset=["position"])`. 
#        This implies our current master MIGHT ONLY HAVE FINISHERS.
#        
#        CRITICAL CHECK: If we dropped non-finishers, we can't calculate DNF rates accurately 
#        from this dataset alone. We might need to go back to silver to heal this, OR 
#        if the 'position' column uses a special code for DNF.
#        
#        Assuption check from EDA: `position` min is 1, max is ~20.
#        If DNFs were dropped in `silver_to_gold`, we can only measure "Reliability" via 
#        mechanical failures impacting speed, not actual dropouts. 
#        
#        However, assuming we might re-introduce DNFs later, let's create a placeholder 
#        or use 'grid_to_finish_delta'. 
#        
#        Let's create a proxy for "Poor Reliability / Bad Day": 
#        losing more than 5 positions from grid.

print("\n--- Generating Reliability Proxy (Significant Position Loss) ---")
# Feature not in master (was calculated in EDA), so recalculating here
df["grid_to_finish_delta"] = df["qualifying_position"] - df["position"]
df["significant_pos_loss"] = (df["grid_to_finish_delta"] < -5).astype(int)

# Rolling Reliability Score (Last 5 races)
# High Score = Many races with significant position loss (Bad)
df["rolling_instability_score"] = (
    df.groupby("driver_name")["significant_pos_loss"]
    .transform(lambda x: x.rolling(window=5, min_periods=1).mean())
)

print(df[["driver_name", "grid_to_finish_delta", "rolling_instability_score"]].head())


# ==============================================================================
# 5. FEATURE: CONSISTENCY METRIC (Std Dev of Position)
# ==============================================================================
# OBJECTIVE: "Consistency in winning rates" and performance.
# LOGIC: Standard Deviation of finishing positions over the last 10 races.
#        Lower Std Dev = More Consistent.
# OUTPUT CHECK:
#   - New Column: 'consistency_score_std'
#   - Value of 0 means finished in exact same position for window.

print("\n--- Generating Consistency Scores ---")
df["consistency_score_std"] = (
    df.groupby("driver_name")["position"]
    .transform(lambda x: x.rolling(window=10, min_periods=3).std())
)

# Invert so Higher Score = More Consistent? 
# Let's keep it as Std Dev for clarity (Lower is Better).
print(df[["driver_name", "position", "consistency_score_std"]].tail())


# ==============================================================================
# 6. SAVE ENRICHED DATA
# ==============================================================================

print("\nSaving Feature-Enriched Master Table...")
df.to_parquet(OUTPUT_PATH, index=False)
print(f"Saved to {OUTPUT_PATH}")
print("Final Shape:", df.shape)

# Quick Look at a Top Driver
top_driver = df["driver_name"].mode()[0]
print(f"\nExample Stats for {top_driver}:")
subset = df[df["driver_name"] == top_driver].tail(5)
print(subset[["year", "race_round", "position", "rolling_avg_5_races", "season_win_rate", "consistency_score_std"]])
