"""
pipeline.py
------------
Runs the full F1 Data Pipeline:
1. Bronze Data Ingestion
2. Bronze → Silver Transformation
3. Silver → Gold Master Merge
4. Gold Layer EDA
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
