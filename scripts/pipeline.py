"""
MAIN PIPELINE ORCHESTRATOR
--------------------------
GOAL: Run the end-to-end data processing flow (Bronze -> Silver -> Gold).
LOGIC:
    1. Ingestion: Raw CSVs -> Bronze Parquet
    2. Transformation: Standardize Schema -> Silver Parquet
    3. Merging: Join all tables -> Gold Master Table
    4. Validation: Run initial EDA check.
"""

import subprocess

def run_script(script_path):
    """Run a Python script and stop if it fails."""
    print(f"\nRunning {script_path}...\n")
    result = subprocess.run(["python", script_path], capture_output=False)
    if result.returncode != 0:
        raise RuntimeError(f"{script_path} failed. Stopping pipeline.")

def main():
    scripts = [
        "scripts/bronze_data_ingestion_1.py",
        "scripts/bronze_to_silver_2.py",
        "scripts/silver_to_gold_master_merge_3.py",
        "scripts/gold_eda_4.py"
    ]

    for script in scripts:
        run_script(script)

    print("\n✅ Pipeline completed successfully!")

if __name__ == "__main__":
    main()
