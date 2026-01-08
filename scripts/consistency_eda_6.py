# CONSISTENCY & RELIABILITY EDA
# GOAL: Analyze the features generated in Stage 5 to understand Driver Forms.

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from config import GOLD_PATH

INPUT_PATH = f"{GOLD_PATH}/master_features.parquet"
IMG_PATH = "imgs"
Path(IMG_PATH).mkdir(exist_ok=True)

# Set visual style
sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)

print("Loading Enriched Data...")
try:
    df = pd.read_parquet(INPUT_PATH)
except FileNotFoundError:
    print(f"Error: {INPUT_PATH} not found. Please run feature_engineering_5.py first.")
    exit()

print("Initial Shape:", df.shape)

# Filter for 2024 season (or latest available) to get current form
LATEST_YEAR = df["year"].max()
print(f"\n--- Analysis Context: Season {LATEST_YEAR} ---")
current_season = df[df["year"] == LATEST_YEAR].copy()

# ==============================================================================
# 1. DRIVER CONSISTENCY RANKING
# ==============================================================================
# Use limits: Drivers with at least 5 races
stats = current_season.groupby("driver_name").agg(
    avg_finish=("position", "mean"),
    avg_consistency=("consistency_score_std", "mean"),
    races=("race_round", "count")
).reset_index()

stats = stats[stats["races"] >= 5].sort_values("avg_consistency")

print("\nTOP 5 MOST CONSISTENT DRIVERS (Lower StdDev = Better):")
print(stats[["driver_name", "avg_consistency", "avg_finish"]].head(5))

print("\nLEAST CONSISTENT DRIVERS (Higher StdDev = More Volatile):")
print(stats[["driver_name", "avg_consistency", "avg_finish"]].tail(5))

# Visualization: Consistency Ranking
plt.figure(figsize=(12, 8))
sns.barplot(
    data=stats.sort_values("avg_consistency"),
    y="driver_name",
    x="avg_consistency",
    palette="viridis"
)
plt.title(f"Driver Consistency (Std Dev of Position) - Season {LATEST_YEAR}")
plt.xlabel("Standard Deviation (Lower is Better)")
plt.ylabel("Driver")
plt.tight_layout()
plt.savefig(f"{IMG_PATH}/driver_consistency_{LATEST_YEAR}.png")
print(f"Saved plot: {IMG_PATH}/driver_consistency_{LATEST_YEAR}.png")

# Insight: High inconsistency might mean High highs and Low lows (Podium or DNF)

# ==============================================================================
# 2. PERFORMANCE vs. RELIABILITY
# ==============================================================================
# Does being unstable (significant pos loss) correlate with lower points?

reliability_stats = current_season.groupby("driver_name").agg(
    total_instability_score=("significant_pos_loss", "sum"),
    total_points=("points", "sum"),
    avg_finish=("position", "mean")
).sort_values("total_instability_score", ascending=False)

print("\nDRIVERS WITH MOST 'BAD DAYS' (Significant Position Loss > 5):")
print(reliability_stats.head(5))

# Correlation Check
corr = reliability_stats["total_instability_score"].corr(reliability_stats["total_points"])
print(f"\nCorrelation between Instability (Pos Loss) and Points: {corr:.2f}")

# Visualization: Instability vs Points
plt.figure(figsize=(10, 6))
sns.scatterplot(
    data=reliability_stats,
    x="total_instability_score",
    y="total_points",
    hue="total_points",
    size="total_points",
    palette="coolwarm",
    legend=False
)
# Add labels for top outliers
for i, row in reliability_stats.head(5).iterrows():
    plt.text(
        row["total_instability_score"] + 0.1, 
        row["total_points"], 
        i, 
        fontsize=9
    )

plt.title(f"Impact of Reliability on Points - Season {LATEST_YEAR}")
plt.xlabel("Total Instability Score (Significant Position Losses)")
plt.ylabel("Total Points")
plt.grid(True)
plt.savefig(f"{IMG_PATH}/reliability_vs_points_{LATEST_YEAR}.png")
print(f"Saved plot: {IMG_PATH}/reliability_vs_points_{LATEST_YEAR}.png")

# Expected: Negative correlation (More instability = Fewer points)

# ==============================================================================
# 3. WIN RATE STABILITY
# ==============================================================================
# Who maintains a high win rate?

winners = current_season[current_season["is_win"] == 1]["driver_name"].unique()
winner_stats = current_season[current_season["driver_name"].isin(winners)].groupby("driver_name").agg(
    final_win_rate=("season_win_rate", "last"),
    win_count=("is_win", "sum")
).sort_values("final_win_rate", ascending=False)

print("\nWINNER EFFICIENCY (Win Rate %):")
print(winner_stats)

# Visualization: Win Share Pie Chart
plt.figure(figsize=(8, 8))
# Only top winners
pie_data = winner_stats[winner_stats["win_count"] > 0]
plt.pie(
    pie_data["win_count"],
    labels=pie_data.index,
    autopct='%1.1f%%',
    startangle=140,
    colors=sns.color_palette("pastel")
)
plt.title(f"Win Share Distribution - Season {LATEST_YEAR}")
plt.savefig(f"{IMG_PATH}/win_share_{LATEST_YEAR}.png")
print(f"Saved plot: {IMG_PATH}/win_share_{LATEST_YEAR}.png")

# ==============================================================================
# 4. SUMMARY INSIGHTS
# ==============================================================================
print("\n--- SUMMARY INSIGHTS ---")
best_consistency = stats.iloc[0]["driver_name"]
most_unstable = reliability_stats.index[0]

print(f"1. The most consistent driver is {best_consistency}.")
print(f"2. The driver with the most 'bad days' (significant drops) is {most_unstable}.")
if corr < -0.3:
    print("3. There is a notable negative link between instability and scoring points.")
else:
    print("3. Instability does not strongly penalize points (perhaps recovery drives are common).")
