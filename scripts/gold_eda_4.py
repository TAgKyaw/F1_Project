# MASTER DATA EDA
# EDA WTIH CONTEXT - ENGINEERING EDA
# NOT TRUE EDA YET FOR DATA ANALYSIS

from config import GOLD_PATH
import pandas as pd

MASTER_PATH = f"{GOLD_PATH}/master_table.parquet"

# Display settings
pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 200)

#Load Master data
master = pd.read_parquet(MASTER_PATH)

print("Master Table Shape:", master.shape) 
# Output: (480,81)
# print(master.head())
print(master.info())

# EDA for Data Health Check =========================================

# Duplicate Check
print(master.duplicated().sum())

# Missing Value Report

missing = (master.isna().sum() / len(master)) * 100
missing = missing.sort_values(ascending=False)

print(missing[missing > 0])
# Some features have more than 30 missing values
# Will be cleaned properly in cleaning stage

# Primary Key Checks
print(master[["race_id", "driver_name"]].nunique())
# Should be one record only
print(master.groupby(["race_id", "driver_name"]).size().value_counts())

# Domain Sanity Checks
# Position Range (Should between 1- 20)
print({master["position"].min()}, {master["position"].max()})

# Qualifying position Range Check (1-20)
print(master["qualifying_position"].min(), master["qualifying_position"].max())

# Stats Check
#  Max should match F1 scoring (approx. 26 with fastest lap)
print(master["points"].describe())

# Categorical Cardinality (Leakage Detection)

categorical_cols = master.select_dtypes(include="object").columns

print(master[categorical_cols].nunique().sort_values(ascending=False))
# team should be ~ 10, driver name should be around 20, circuit_names around 24
# Any column with nearly 480 unique values --> leakage Risk.
# Some values with leakge detected.
# Will be dropped in later data cleaning stage.

### Target Variable Creation (Podium Classification)
# Preparation for ML, Not Trained Yet.
# Podium Data features for domain-driven analyses
master["is_podium"] = (master["position"] <= 3).astype(int)

print(master["is_podium"].value_counts(normalize=True) * 100)
# Should see ~15% podiums, ~85% non-podiums

### Qualifying vs Race Delta (Core Performance Signal)
master["grid_to_finish_delta"] = master["qualifying_position"] - master["position"]
master["grid_to_finish_delta"].describe()
#  Positive → positions gained
#  Negative → positions 

#### Driver Consistency Overview
# Season consistency, Elite vs midfield vs backmarkers
driver_summary = (
    master.groupby("driver_name")
    .agg(
        avg_finish=("position", "mean"),
        avg_qualifying=("qualifying_position", "mean"),
        podium_rate=("is_podium", "mean"),
        avg_points=("points", "mean"),
        races=("race_id", "count")
    )
    .sort_values("avg_finish")
)

# print(driver_summary.head(10))

# Constructor Strength Overview
constructor_summary = (
    master.groupby("team")
    .agg(
        avg_finish=("position", "mean"),
        avg_points=("points", "mean"),
        podium_rate=("is_podium", "mean")
    )
    .sort_values("avg_points", ascending=False)
)

# print(constructor_summary)

# Circuit Difficulty Overview
# Idenifies: Easy overtaking tracks, and Processional Circuits

circuit_summary = (
    master.groupby("circuit")
    .agg(
        avg_finish=("position", "mean"),
        avg_overtakes=("grid_to_finish_delta", "mean"),
        races=("race_id", "count")
    )
    .sort_values("avg_overtakes", ascending=False)
)

# print(circuit_summary)

#### Correlation Scan (Numerical Only)

# Searching for: 
#  Strong negative correlation with skill features
#  Strong positive correlation with mistakes/features
# Checking if it has erred data points with low calculations
# Computing pairwise Pearson correlation - only correlations with the target "position" for future ML predictions
numeric_df = master.select_dtypes(include=["int64", "float64"])
corr = numeric_df.corr()

# print(corr["position"].sort_values())

# Target Distribution Check 

print("Target distribution:")
print(master["is_podium"].value_counts())
print("\nPercentages:")
print(master["is_podium"].value_counts(normalize=True) * 100)

numeric_cols = master.select_dtypes(include=["int64", "float64"]).columns
corr = master[numeric_cols].corr()["is_podium"].sort_values(ascending=False)

print("Correlation with is_podium:\n")
print(corr)

from sklearn.feature_selection import mutual_info_classif

X = master[numeric_cols].drop(columns=["is_podium"], errors="ignore")
y = master["is_podium"]

mi = mutual_info_classif(X.fillna(0), y, discrete_features=False)
mi_scores = pd.Series(mi, index=X.columns).sort_values(ascending=False)

print("\nMutual Information scores:")
print(mi_scores)

