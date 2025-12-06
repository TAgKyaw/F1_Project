
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# print(PROJECT_ROOT)
GLOBAL_DATA_PATH = f'{PROJECT_ROOT}/data'

Path(GLOBAL_DATA_PATH).mkdir(parents=True, exist_ok=True)

F1_PATH = f"{GLOBAL_DATA_PATH}/f1"
SILVER_PATH = f"{GLOBAL_DATA_PATH}/silver"
BRONZE_PATH = f"{GLOBAL_DATA_PATH}/bronze"
GOLD_PATH = f"{GLOBAL_DATA_PATH}/gold"

# print(GLOBAL_DATA_PATH)
# print(F1_PATH)
# print(SILVER_PATH)
# print(BRONZE_PATH)
# print(GOLD_PATH)