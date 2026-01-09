# F1 DATA COMPANION (CHATBOT)
# GOAL: Interactive interface for querying F1 Insights & ML Predictions
# LOGIC:
#   1. Loads Gold Key Features and ML Model.
#   2. Parses user natural language queries (Intent Recognition).
#   3. Returns structured response data (for both Console and Web API).

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

    def get_response(self, query):
        """
        Process query and return a structured dictionary response.
        Format:
        {
            "text": "Human readable string",
            "type": "text" | "table" | "kv_pairs",
            "data": ... (raw data for frontend rendering)
        }
        """
        query = query.lower()

        # INTENT: EXIT
        if query in ["exit", "quit", "bye"]:
            return {"text": "Goodbye! Enjoy the race.", "type": "text"}

        # INTENT: CONSISTENCY LEADERBOARD
        if "consistency" in query or "consistent" in query:
            return self.response_consistency()
            
        # INTENT: RELIABILITY / CRASHES
        if "reliability" in query or "crash" in query or "dnf" in query:
            return self.response_reliability()
            
        # INTENT: WIN RATE
        if "win" in query or "winner" in query:
            return self.response_winners()

        # INTENT: DRIVER STATS / PREDICTION
        found_driver = None
        for driver in self.drivers:
            if driver.lower() in query or (len(query.split()) > 1 and driver.split()[-1].lower() in query):
                found_driver = driver
                break
        
        if not found_driver:
            words = query.split()
            if words:
                potential_name = words[-1]
                matches = difflib.get_close_matches(potential_name, self.drivers, n=1, cutoff=0.6)
                if matches:
                     found_driver = matches[0]

        if "predict" in query and found_driver:
            return self.response_predict_driver(found_driver)
        
        if found_driver and ("stats" in query or "how is" in query or "who is" in query):
            return self.response_driver_stats(found_driver)

        # FALLBACK
        fallback_msg = (
            "I didn't quite catch that. Try asking about:\n"
            "- 'Who is the most consistent?'\n"
            "- 'Stats for [Driver Name]'\n"
            "- 'Predict position for [Driver Name]'"
        )
        return {"text": fallback_msg, "type": "text"}

    def response_driver_stats(self, driver):
        stats = self.current_season[self.current_season["driver_name"] == driver].iloc[-1]
        
        data = {
            "Driver": driver,
            "Current Form (Last 5 Avg)": f"{stats['rolling_avg_5_races']:.1f}",
            "Season Win Rate": f"{stats['season_win_rate']*100:.1f}%",
            "Consistency Score": f"{stats['consistency_score_std']:.2f} (Low is good)",
            "Points per Race": f"{stats['points_per_race']:.1f}"
        }
        
        msg = f"Stats for {driver} (Round {stats['race_round']}):\n"
        for k, v in data.items():
            msg += f"- {k}: {v}\n"
            
        return {
            "text": msg,
            "type": "kv_pairs",
            "data": data
        }

    def response_predict_driver(self, driver):
        if not self.model:
            return {"text": "Prediction model is unavailable.", "type": "error"}

        latest_data = self.current_season[self.current_season["driver_name"] == driver].iloc[-1:]
        
        try:
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
            qual_pos = int(X_input['qualifying_position'].values[0])
            
            msg = (
                f"AI Prediction for {driver}:\n"
                f"Based on current form & Qualifying P{qual_pos}\n"
                f"Predicted Finish: P{int(round(pred_pos))} (Exact: {pred_pos:.2f})"
            )
            
            return {
                "text": msg,
                "type": "prediction",
                "data": {
                    "driver": driver,
                    "predicted_position": round(pred_pos, 2),
                    "qualifying_used": qual_pos
                }
            }
        except Exception as e:
            return {"text": f"Couldn't generate prediction. Data mismatch? ({e})", "type": "error"}

    def response_consistency(self):
        ranking = self.current_season.groupby("driver_name")["consistency_score_std"].mean().sort_values()
        top_5 = ranking.head(5)
        
        msg = "Consistency Leaderboard (Lower Std Dev is better):\n" + top_5.to_string()
        return {
            "text": msg,
            "type": "table",
            "data": top_5.to_dict()
        }
        
    def response_reliability(self):
        ranking = self.current_season.groupby("driver_name")["rolling_instability_score"].mean().sort_values(ascending=False)
        top_5 = ranking.head(5)
        
        msg = "Most Unreliable (High Instability Score):\n" + top_5.to_string()
        return {
            "text": msg,
            "type": "table",
            "data": top_5.to_dict()
        }

    def response_winners(self):
        ranking = self.current_season.groupby("driver_name")["season_win_rate"].last().sort_values(ascending=False)
        leaders = ranking[ranking > 0]
        
        msg = "Win Rate Leaders:\n" + leaders.to_string()
        return {
            "text": msg,
            "type": "table",
            "data": leaders.to_dict()
        }

def main():
    bot = F1Bot()
    print("\nF1Bot: Ready! Ask me about 2024 drivers, stats, or consistency.")
    print("(Type 'exit' to quit)")
    
    active = True
    while active:
        user_input = input("\nYou: ")
        response = bot.get_response(user_input)
        
        # Console Renderer
        print(f"F1Bot: {response['text']}")
        
        if user_input.lower() in ["exit", "quit", "bye"]:
            active = False

if __name__ == "__main__":
    main()
