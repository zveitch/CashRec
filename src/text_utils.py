# /src/text_utils.py
import math
import re
from typing import List, Optional
import pandas as pd

def coerce_date(series: pd.Series) -> pd.Series:
    # Parse to pandas datetime64[ns]; blanks => NaT
    return pd.to_datetime(series, errors='coerce')

def coerce_number(series: pd.Series) -> pd.Series:
    return (series.astype(str)
            .str.replace(r"[,\s£$]", "", regex=True)
            .replace({"": "0", "nan": "0", "None": "0"})
            .astype(float))

def parse_match_id(text: str) -> Optional[str]:
    if not isinstance(text, str):
        return None
    m = re.search(r"(\d+)\s*$", text.strip())
    return m.group(1) if m else None

def normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s_up = s.upper()
    s_clean = re.sub(r"[^A-Z0-9]+", " ", s_up)
    return re.sub(r"\s+", " ", s_clean).strip()

def tokenize(text: str) -> List[str]:
    return normalize_text(text).split()

def ngrams(tokens: List[str], n_min: int = 1, n_max: int = 3) -> List[str]:
    grams = []
    for n in range(n_min, n_max + 1):
        for i in range(len(tokens) - n + 1):
            grams.append(" ".join(tokens[i: i + n]))
    return grams

def extract_years(text: str) -> List[str]:
    return re.findall(r"\b(20\d{2})\b", text or "")

def first_nonempty(*vals) -> str:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v
    return ""

def signed_bank_amount(debit: float, credit: float) -> float:
    return float(credit) - float(debit)

def is_finite_number(x) -> bool:
    return x is not None and isinstance(x, (int, float)) and math.isfinite(x)
