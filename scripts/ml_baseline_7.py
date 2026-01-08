# ML BASELINE: RANDOM FOREST REGRESSOR
# GOAL: Predict Driver Finishing Position using Feature Engineering

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from pathlib import Path
from config import GOLD_PATH

INPUT_PATH = f"{GOLD_PATH}/master_features.parquet"
MODEL_DIR = "data/models"
MODEL_PATH = f"{MODEL_DIR}/rf_model.joblib"
IMG_PATH = "imgs"

Path(MODEL_DIR).mkdir(parents=True, exist_ok=True)
Path(IMG_PATH).mkdir(exist_ok=True)

# Set visual style
sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)

print("Loading Data for ML...")
try:
    df = pd.read_parquet(INPUT_PATH)
except FileNotFoundError:
    print(f"Error: {INPUT_PATH} not found.")
    exit()

# 1. DATA PREPARATION
# ===================

# Drop rows where target or key features are missing
# (First few races might have NaN rolling avgs)
df = df.dropna(subset=["rolling_avg_5_races", "position"])

# Feature Selection
# 'grid_to_finish_delta' is derived from y, so it's a cheat -> REMOVE IT from X!
# We want to predict position knowing qualifying + history.
features = [
    "qualifying_position", 
    "rolling_avg_5_races", 
    "season_win_rate", 
    "confidence_score_std", # Wait, it was consistency_score_std
    "rolling_instability_score",
    "points_per_race",
    "car_number"
]

# Check column names
available_cols = df.columns.tolist()
# 'consistency_score_std' check
if "consistency_score_std" in available_cols:
    features = [f if f != "confidence_score_std" else "consistency_score_std" for f in features]
else:
    print("Warning: consistency_score_std not found.")

# Remove 'confidence_score_std' typo if present in my list above
features = [f for f in features if f != "confidence_score_std"]
features.append("consistency_score_std")

# 2. TIME-SERIES SPLIT
# ====================
# Train: Rounds 1 to 18 (Approx 75%)
# Test: Rounds 19 to 24 (End of Season)

TRAIN_CUTOFF_ROUND = 18

print(f"\nSplitting Data (Train: Rounds 1-{TRAIN_CUTOFF_ROUND}, Test: Rounds {TRAIN_CUTOFF_ROUND+1}+)")

train = df[df["race_round"] <= TRAIN_CUTOFF_ROUND]
test = df[df["race_round"] > TRAIN_CUTOFF_ROUND]

X_train = train[features]
y_train = train["position"]
X_test = test[features]
y_test = test["position"]

print(f"Train Shape: {X_train.shape}, Test Shape: {X_test.shape}")

# 3. MODEL TRAINING
# =================
print("\nTraining Random Forest Regressor...")
rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
rf.fit(X_train, y_train)

# 4. PREDICTION & EVALUATION
# ==========================
y_pred = rf.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print("\n--- MODEL PERFORMANCE METRICS ---")
print(f"MAE (Mean Absolute Error): {mae:.2f} positions")
print(f"RMSE (Root Mean Sq Error): {rmse:.2f} positions")
print(f"R2 Score: {r2:.2f}")

# Save Model
print(f"\nSaving Model to {MODEL_PATH}...")
joblib.dump(rf, MODEL_PATH)
print("Model Saved.")

# Interpretation:
# MAE of 2.5 means on average we are off by 2.5 positions.
# Not bad for a baseline given the chaos of F1!

# 5. FEATURE IMPORTANCE
# =====================
importance = pd.DataFrame({
    "feature": features,
    "importance": rf.feature_importances_
}).sort_values("importance", ascending=False)

print("\n--- FEATURE IMPORTANCE ---")
print(importance)

plt.figure(figsize=(10, 6))
sns.barplot(data=importance, x="importance", y="feature", palette="magma")
plt.title("What drives the prediction? (Random Forest Feature Importance)")
plt.savefig(f"{IMG_PATH}/feature_importance_rf.png")
print(f"Saved plot: {IMG_PATH}/feature_importance_rf.png")

# 6. ACTUAL VS PREDICTED (Visualization)
# ======================================
results = test.copy()
results["predicted_position"] = y_pred
results["error"] = results["position"] - results["predicted_position"]

# Let's look at one specific race in the test set
sample_round = TRAIN_CUTOFF_ROUND + 1
race_df = results[results["race_round"] == sample_round].sort_values("position")

plt.figure(figsize=(12, 6))
x = np.arange(len(race_df))
width = 0.35

plt.bar(x - width/2, race_df["position"], width, label='Actual')
plt.bar(x + width/2, race_df["predicted_position"], width, label='Predicted')

plt.xlabel('Driver')
plt.ylabel('Position')
plt.title(f'Actual vs Predicted Positions - Round {sample_round}')
plt.xticks(x, race_df["driver_name"], rotation=45, ha='right')
plt.legend()
plt.tight_layout()
plt.savefig(f"{IMG_PATH}/prediction_vs_actual_round_{sample_round}.png")
print(f"Saved plot: {IMG_PATH}/prediction_vs_actual_round_{sample_round}.png")
