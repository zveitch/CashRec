# /src/config.py
from pathlib import Path

# Base directories (assumes running from project root)
DATA_DIR = Path("./data")
INPUT_DIR = DATA_DIR
OUTPUT_DIR = DATA_DIR / "output"
RULES_DIR = DATA_DIR / "rules"
RULES_PATH = RULES_DIR / "tag_rules.json"

# Matching & validation parameters
DEFAULT_TOLERANCE = 0.01        # currency tolerance for matching (e.g., £0.01)
MAX_DATE_LAG_DAYS = 5           # flag if cash vs bank dates differ by > N days
DEFAULT_OUTPUT_ENCODING = "utf-8-sig"
