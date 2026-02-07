# /src/audit_mislabels.py
import re
import pandas as pd

FEE_WORDS_RX = re.compile(r"\b(FEE|FEES|EXPENSE|CHARGE|BANK CHARGE|SWIFT)\b", re.I)
BANKY_RX = re.compile(r"\b(HSBC|BARCLAYS|CITI|FRB|BONY|JPM|JPMORGAN|J\.P\. MORGAN|BNP|DEUTSCHE|TRANSFER|SWIFT|CHAPS|BACS)\b", re.I)
MGMT_RX = re.compile(
    r"\b(MANAGEMENT\s+FEES?|MGMT\s*FEES?|ADVIS(OR|ORY)\s+FEES?)\b",
    re.I
)


def _has_fee_like(text: str) -> bool:
    return bool(FEE_WORDS_RX.search(text or ""))

def _has_mgmt_like(text: str) -> bool:
    return bool(MGMT_RX.search(text or ""))

def _has_banky_counterparty(text: str) -> bool:
    return bool(BANKY_RX.search(text or ""))

def audit_mgmt_vs_expenses(detailed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Heuristics to flag likely mislabels between "Mgmt Fees" and "Fees and Expenses".
    Returns a dataframe with Suggested_Type and Confidence for review.
    """
    df = detailed_df.copy()
    df["__text"] = (
        df["Description1B"].astype(str) + " | " +
        df["Description2"].astype(str) + " | " +
        df["Detail"].astype(str)
    )

    suspects = []
    for idx, r in df.iterrows():
        cur_type = str(r.get("Type") or "")
        text = r.get("__text") or ""
        reasons = []
        score = 0.0
        suggested = None

        if cur_type not in ("Mgmt Fees", "Fees and Expenses"):
            continue

        fee_like = _has_fee_like(text)
        mgmt_like = _has_mgmt_like(text)
        banky = _has_banky_counterparty(text)

        if cur_type == "Mgmt Fees":
            if fee_like and banky and not mgmt_like:
                suggested = "Fees and Expenses"; score += 0.8; reasons.append("fee_words+bank_counterparty")
            elif fee_like and not mgmt_like:
                suggested = "Fees and Expenses"; score += 0.6; reasons.append("fee_words_no_mgmt")
        elif cur_type == "Fees and Expenses":
            if mgmt_like and not banky:
                suggested = "Mgmt Fees"; score += 0.8; reasons.append("mgmt_words_no_banky")
            elif mgmt_like:
                suggested = "Mgmt Fees"; score += 0.6; reasons.append("mgmt_words")

        if suggested and score >= 0.6:
            suspects.append({
                "Row_Index": idx,
                "Current_Type": cur_type,
                "Suggested_Type": suggested,
                "Confidence": round(min(score, 1.0), 2),
                "Reasons": ";".join(reasons),
                "Calculated_Date": r.get("Calculated_Date"),
                "Cash_Date": r.get("Cash_Date"),
                "Description1B": r.get("Description1B"),
                "Description2": r.get("Description2"),
                "Detail": r.get("Detail"),
                "Manual_Amount": r.get("Manual_Amount"),
                "Match_ID": r.get("Match_ID"),
                "Tag": r.get("Tag"),
                "Status": r.get("Status"),
            })

    return pd.DataFrame(suspects)