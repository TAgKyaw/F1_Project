# API SERVER
# ----------
# GOAL: Expose F1 Analytics & Chatbot via REST API for the Frontend.
# LOGIC:
#   1. Initialize FastAPI app.
#   2. Load F1Bot instance.
#   3. Define endpoints for Chat, Stats, and Health checks.

import sys
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Ensure 'scripts' is in path so we can import siblings
sys.path.append(str(Path(__file__).parent))

from data_bot_8 import F1Bot

# 1. SETUP
# --------
app = FastAPI(title="F1 Analytics API", version="1.0")

# Allow CORS for local frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Bot (Global State)
print("Startup: Initializing F1Bot...")
bot = F1Bot()

# Data Models
class ChatRequest(BaseModel):
    query: str

# 2. ENDPOINTS
# ------------

@app.get("/health")
def health_check():
    """Verify API is running."""
    return {"status": "online", "model_loaded": bot.model is not None}

@app.post("/chat")
def chat_endpoint(request: ChatRequest):
    """
    Main Chat Interface.
    Input: {"query": "message"}
    Output: JSON response from F1Bot
    """
    response = bot.get_response(request.query)
    return response

@app.get("/stats/consistency")
def get_consistency():
    """Get raw consistency data for dashboard charts."""
    # Re-using bot logic or accessing bot.df directly
    # Ideally, we should abstract this, but accessing bot.response_consistency 
    # gives us the formatted table. Let's return raw data.
    
    # We can invoke bot's method which returns {type: table, data: dict}
    return bot.response_consistency()

@app.get("/stats/reliability")
def get_reliability():
    """Get raw reliability data."""
    return bot.response_reliability()

@app.get("/stats/overview")
def get_overview():
    """Get high level season stats."""
    # Simple aggregation
    top_driver = bot.current_season.loc[bot.current_season["points"].idxmax()]
    total_races = bot.current_season["race_id"].nunique()
    
    return {
        "season_year": int(bot.latest_year),
        "races_completed": int(total_races),
        "leader": top_driver["driver_name"],
        "leader_points": float(top_driver["points"])
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
