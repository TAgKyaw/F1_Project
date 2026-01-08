# F1 DATA COMPANION (CHATBOT)
# GOAL: Interactive interface for querying F1 Insights & ML Predictions

import pandas as pd
import numpy as np
import joblib
import difflib
import sys
from config import GOLD_PATH

# Configuration
DATA_PATH = f"{GOLD_PATH}/master_features.parquet"
MODEL_PATH = "data/models/rf_model.joblib"

class F1Bot:
    def __init__(self):
        print("Initializing F1 Data Companion...")
        try:
            self.df = pd.read_parquet(DATA_PATH)
            # Ensure 2024 (or latest) focus
            self.latest_year = self.df["year"].max()
            self.current_season = self.df[self.df["year"] == self.latest_year].copy()
            print(f"-> Local Data Loaded (Season {self.latest_year} Focus)")
        except FileNotFoundError:
            print("Error: Data file not found. Run pipeline first.")
            sys.exit(1)

        try:
            self.model = joblib.load(MODEL_PATH)
            print("-> ML Prediction Model Loaded")
        except FileNotFoundError:
            print("Warning: ML Model not found. Prediction capabilities disabled.")
            self.model = None

        self.drivers = self.current_season["driver_name"].unique().tolist()
    
    def normalize_driver_name(self, name_query):
        """Fuzzy match driver name."""
        matches = difflib.get_close_matches(name_query, self.drivers, n=1, cutoff=0.4)
        return matches[0] if matches else None

    def handle_query(self, query):
        query = query.lower()

        # INTENT: EXIT
        if query in ["exit", "quit", "bye"]:
            print("F1Bot: Goodbye! Enjoy the race.")
            return False

        
        # INTENT: CONSISTENCY LEADERBOARD (Check BEFORE driver name to avoid "consistent" -> "Sargeant" mismatch)
        if "consistency" in query or "consistent" in query:
            self.response_consistency()
            return True
            
        # INTENT: RELIABILITY / CRASHES
        if "reliability" in query or "crash" in query or "dnf" in query:
            self.response_reliability()
            return True
            
        # INTENT: WIN RATE
        if "win" in query or "winner" in query:
            self.response_winners()
            return True

        # INTENT: DRIVER STATS / PREDICTION
        # Logic: Check if a driver name is mentioned
        found_driver = None
        for driver in self.drivers:
            # Check full name or last name
            if driver.lower() in query or (len(query.split()) > 1 and driver.split()[-1].lower() in query):
                found_driver = driver
                break
        
        # If not direct match, try fuzzy logic on LAST WORD only if query is short
        if not found_driver:
            # heuristic: Only try fuzzy match if query length is small (likely just a name)
            # or explicitly asks "stats for [name]"
            words = query.split()
            potential_name = words[-1]
            # Increase cutoff to 0.6 to be safer
            matches = difflib.get_close_matches(potential_name, self.drivers, n=1, cutoff=0.6)
            if matches:
                 found_driver = matches[0]

        if "predict" in query and found_driver:
            self.response_predict_driver(found_driver)
            return True
        
        if found_driver and ("stats" in query or "how is" in query or "who is" in query):
            self.response_driver_stats(found_driver)
            return True



        # FALLBACK
        print("F1Bot: I didn't quite catch that. Try asking about:")
        print("       - 'Who is the most consistent?'")
        print("       - 'Stats for [Driver Name]'")
        print("       - 'Predict position for [Driver Name]'")
        return True

    def response_driver_stats(self, driver):
        stats = self.current_season[self.current_season["driver_name"] == driver].iloc[-1]
        print(f"\n--- Stats for {driver} (Round {stats['race_round']}) ---")
        print(f"Current Form (Last 5 Avg): {stats['rolling_avg_5_races']:.1f}")
        print(f"Season Win Rate: {stats['season_win_rate']*100:.1f}%")
        print(f" consistency Score: {stats['consistency_score_std']:.2f} (Lower is better)")
        print(f"Points per Race: {stats['points_per_race']:.1f}")

    def response_predict_driver(self, driver):
        if not self.model:
            print("F1Bot: Prediction model is unavailable.")
            return

        # Get latest known features for driver
        latest_data = self.current_season[self.current_season["driver_name"] == driver].iloc[-1:]
        
        # MOCK INPUT for Next Race:
        # We assume the user wants to predict the 'next' potential result based on current form.
        # Ideally, we would ask user for 'Qualifying Position' input, but for now we auto-fill 
        # with their average qualifying or last qualifying.
        # Let's use their *last* qualifying position as the estimate.
        
        try:
            # Must match feature columns from ml_baseline_7
            features_needed = [
                "qualifying_position", 
                "rolling_avg_5_races", 
                "season_win_rate", 
                "consistency_score_std", 
                "rolling_instability_score",
                "points_per_race",
                "car_number"
            ]
            
            X_input = latest_data[features_needed]
            pred_pos = self.model.predict(X_input)[0]
            
            print(f"\n--- AI Prediction for {driver} ---")
            print(f"Based on current form & Qualifying P{int(X_input['qualifying_position'].values[0])}")
            print(f"Predicted Finish: P{int(round(pred_pos))} (Exact: {pred_pos:.2f})")
        except Exception as e:
            print(f"F1Bot: Couldn't generate prediction. Data mismatch? ({e})")

    def response_consistency(self):
        print("\n--- Consistency Leaderboard ---")
        ranking = self.current_season.groupby("driver_name")["consistency_score_std"].mean().sort_values()
        print(ranking.head(5))
        
    def response_reliability(self):
        print("\n--- Most Unreliable (High Instability Score) ---")
        ranking = self.current_season.groupby("driver_name")["rolling_instability_score"].mean().sort_values(ascending=False)
        print(ranking.head(5))

    def response_winners(self):
        print("\n--- Win Rate Leaders ---")
        ranking = self.current_season.groupby("driver_name")["season_win_rate"].last().sort_values(ascending=False)
        print(ranking[ranking > 0])

def main():
    bot = F1Bot()
    print("\nF1Bot: Ready! Ask me about 2024 drivers, stats, or consistency.")
    print("(Type 'exit' to quit)")
    
    active = True
    while active:
        user_input = input("\nYou: ")
        active = bot.handle_query(user_input)

if __name__ == "__main__":
    main()
