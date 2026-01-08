"""
ADVANCED ANALYTICS PIPELINE
---------------------------
GOAL: Orchestrate the advanced analytical stages (Features -> EDA -> ML).
LOGIC:
    1. Feature Engineering: Calculate rolling stats, win rates, reliability scores.
    2. Advanced EDA: Viz consistency, reliability, and dominance.
    3. ML Modeling: Train Random Forest Regressor and evaluate.
"""

import subprocess
import sys
import os
from pathlib import Path
from config import GOLD_PATH

# Define scripts to run in order
PIPELINE_STEPS = [
    {
        "name": "Feature Engineering",
        "script": "scripts/feature_engineering_5.py",
        "description": "Generating rolling averages, win rates, and consistency metrics..."
    },
    {
        "name": "Advanced EDA & Viz",
        "script": "scripts/consistency_eda_6.py",
        "description": "Running Consistency EDA and generating plots in imgs/..."
    },
    {
        "name": "ML Baseline Model",
        "script": "scripts/ml_baseline_7.py",
        "description": "Training Random Forest Regressor and evaluating performance..."
    }
]

def check_dependencies():
    """Ensure we have the prerequisite Gold Master Table."""
    master_file = Path(f"{GOLD_PATH}/master_table.parquet")
    if not master_file.exists():
        print(f"❌ Critical Error: Gold Master data not found at {master_file}")
        print("Please run the main 'pipeline.py' first to generate the base data.")
        sys.exit(1)
    print("Base Data Found: master_table.parquet")

def run_script(step):
    """Execute a python script as a subprocess."""
    script_path = step["script"]
    print(f"\n[{step['name']}]")
    print(f"-> {step['description']}")
    
    # Use the current python executable to ensure venv usage
    cmd = [sys.executable, script_path]
    
    try:
        result = subprocess.run(cmd, check=True, text=True)
        print(f"{step['name']} completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"\nPipeline Failed at step: {step['name']}")
        print(f"   Script: {script_path}")
        print(f"   Exit Code: {e.returncode}")
        sys.exit(e.returncode)

def main():
    print("=======================================================")
    print("   F1 Dynamics: Feature Engineering & Analytics Pipeline")
    print("=======================================================")
    
    # 1. Pre-flight Check
    check_dependencies()
    
    # 2. Run Steps
    for step in PIPELINE_STEPS:
        run_script(step)
        
    print("\n=======================================================")
    print("Feature Pipeline Completed Successfully!")
    print("   - Features saved to: data/gold/master_features.parquet")
    print("   - Visualizations saved to: imgs/")
    print("=======================================================")

if __name__ == "__main__":
    main()
