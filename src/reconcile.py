# /src/reconcile.py
import pandas as pd
from typing import Tuple
from .config import DEFAULT_TOLERANCE, MAX_DATE_LAG_DAYS
from .tagging import tag_row
from .text_utils import (
    coerce_date, coerce_number, parse_match_id, signed_bank_amount, first_nonempty
)

def _status(row, tol: float):
    match_id = row.get("Match_ID")
    if not match_id:
        return "UNLINKED_NO_MATCH_ID"
    amt_diff = row.get("Amount_Diff")
    try:
        amt_diff = float(amt_diff)
    except Exception:
        return "INVALID_AMOUNTS"
    return "MATCHED" if abs(amt_diff) <= float(tol) else "MISMATCH"

def reconcile_account(df: pd.DataFrame, account_id: str, rules) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Ensure required columns
    for col in ["Calculated_Date", "Cash_Date", "Description1A", "Description1B", "Description2",
                "Detail", "Type", "Amount", "Reconciled", "Debit", "Credit"]:
        if col not in df.columns:
            df[col] = None

    # Coerce
    df["Calculated_Date"] = coerce_date(df.get("Calculated_Date"))
    df["Cash_Date"] = coerce_date(df.get("Cash_Date"))
    df["Debit"] = coerce_number(df.get("Debit"))
    df["Credit"] = coerce_number(df.get("Credit"))
    df["CashRec_Amount"] = coerce_number(df.get("Amount"))
    df["Bank_Amount"] = df.apply(lambda r: signed_bank_amount(r["Debit"], r["Credit"]), axis=1)
    df["Match_ID"] = df["Reconciled"].apply(parse_match_id)

    # Tagging
    tag_results = df.apply(lambda r: tag_row(r, rules), axis=1, result_type='expand')
    tag_results.columns = ["Type_Group", "Tag", "Investor", "Originator", "SPV", "Tag_Year", "Tag_Source", "Tag_Year_Required_Missing"]
    detailed_df = pd.concat([df, tag_results], axis=1)

    # Ensure numeric before aggregations
    detailed_df["Bank_Amount"] = pd.to_numeric(detailed_df["Bank_Amount"], errors="coerce").fillna(0.0)
    detailed_df["CashRec_Amount"] = pd.to_numeric(detailed_df["CashRec_Amount"], errors="coerce").fillna(0.0)

    # ✅ Ensure datetime64[ns] for date columns (blanks -> NaT)
    for col in ["Calculated_Date", "Cash_Date"]:
        detailed_df[col] = pd.to_datetime(detailed_df[col], errors="coerce")

    # Aggregates per Match_ID: bank
    bank_agg = (
        detailed_df
        .groupby("Match_ID", dropna=False)
        .agg(
            Bank_Amount_Total=("Bank_Amount", "sum"),
            Bank_Date_Min=("Calculated_Date", "min"),
            Bank_Date_Max=("Calculated_Date", "max"),
            Bank_Desc1B=("Description1B", lambda x: first_nonempty(*x)),
            Bank_Desc2=("Description2", lambda x: first_nonempty(*x)),
        )
        .reset_index()
    )
    # Aggregates per Match_ID: manual
    manual_agg = (
        detailed_df
        .groupby("Match_ID", dropna=False)
        .agg(
            CashRec_Amount_Total=("CashRec_Amount", "sum"),
            Cash_Date_Min=("Cash_Date", "min"),
            Cash_Date_Max=("Cash_Date", "max"),
        )
        .reset_index()
    )

    detailed_df = detailed_df.merge(bank_agg, on="Match_ID", how="left") \
                             .merge(manual_agg, on="Match_ID", how="left")

    # --- Amount_Diff numeric & safe ---
    detailed_df["Amount_Diff"] = (
        pd.to_numeric(detailed_df["Bank_Amount_Total"], errors="coerce").fillna(0)
        - pd.to_numeric(detailed_df["CashRec_Amount_Total"], errors="coerce").fillna(0)
    ).round(2)

    # --- Date lag numeric or NaN ---
    def min_date_lag(row):
        bmin, cmin = row["Bank_Date_Min"], row["Cash_Date_Min"]
        if pd.isna(bmin) or pd.isna(cmin):
            return float("nan")
        try:
            return abs((pd.to_datetime(bmin) - pd.to_datetime(cmin)).days)
        except Exception:
            return float("nan")

    detailed_df["Date_Lag_Days"] = detailed_df.apply(min_date_lag, axis=1)
    detailed_df["Date_Lag_Days"] = pd.to_numeric(detailed_df["Date_Lag_Days"], errors="coerce")

    detailed_df["Status"] = detailed_df.apply(lambda r: _status(r, DEFAULT_TOLERANCE), axis=1)

    # Split/fee heuristic (numeric-safe group_ok)
    amt_diff_num = pd.to_numeric(detailed_df["Amount_Diff"], errors="coerce")
    group_ok = (
        amt_diff_num.groupby(detailed_df["Match_ID"], dropna=False)
        .first()
        .abs()
        .le(float(DEFAULT_TOLERANCE))
    )

    fee_like = (
        detailed_df["Detail"].astype(str).str.contains(r"\bFEE|CHARGE|EXPENSE\b", regex=True, case=False, na=False)
        | detailed_df["Description2"].astype(str).str.contains(r"\bFEE|CHARGE|EXPENSE\b", regex=True, case=False, na=False)
        | detailed_df["Description1B"].astype(str).str.contains(r"\bFEE|CHARGE|EXPENSE\b", regex=True, case=False, na=False)
    )
    detailed_df.loc[
        (detailed_df["Status"] == "MISMATCH")
        & (fee_like | detailed_df["Match_ID"].map(group_ok).fillna(False)),
        "Status"
    ] = "MATCHED_WITH_SPLIT_FEES"

    # Exceptions
    untagged = detailed_df["Tag"].isna() & detailed_df["Type_Group"].ne("Investment payments")
    inv_missing = (detailed_df["Type_Group"] == "Investment payments") & detailed_df["Originator"].isna() & detailed_df["SPV"].isna()
    year_missing = detailed_df.get("Tag_Year_Required_Missing", False) == True
    bad_status = detailed_df["Status"].isin(["UNLINKED_NO_MATCH_ID", "INVALID_AMOUNTS", "MISMATCH"])
    big_lag = detailed_df["Date_Lag_Days"].fillna(0) > float(MAX_DATE_LAG_DAYS)

    exceptions_df = detailed_df[untagged | inv_missing | year_missing | bad_status | big_lag].copy()

    # Summary
    summary_df = (
        detailed_df
        .groupby(["Type", "Type_Group", "Tag", "Tag_Year"], dropna=False)
        .agg(CashRec_Amount_Total=("CashRec_Amount", "sum"), Rows=("CashRec_Amount", "count"))
        .reset_index()
        .sort_values(["Type_Group", "Type", "Tag", "Tag_Year"], na_position="last")
    )

    # Suggestions (from untagged lines)
    def text_for_suggestions(r):
        return " ".join([str(r.get("Description1B") or ""), str(r.get("Description2") or ""), str(r.get("Detail") or "")])

    from .text_utils import normalize_text, tokenize, ngrams
    untagged_rows = detailed_df[untagged | inv_missing].copy()

    token_counts, ngram_counts = {}, {}
    for _, r in untagged_rows.iterrows():
        t = normalize_text(text_for_suggestions(r))
        toks = [tok for tok in tokenize(t) if len(tok) >= 3 and not tok.isdigit()]
        for tok in toks:
            token_counts[tok] = token_counts.get(tok, 0) + 1
        for g in ngrams(toks, 2, 3):
            ngram_counts[g] = ngram_counts.get(g, 0) + 1

    suggestions_df = (
        pd.DataFrame(sorted(ngram_counts.items(), key=lambda x: x[1], reverse=True), columns=["ngram_2_3", "count"]).head(100)
        .reset_index(drop=True)
    )
    top_tokens_df = (
        pd.DataFrame(sorted(token_counts.items(), key=lambda x: x[1], reverse=True), columns=["token", "count"]).head(100)
        .reset_index(drop=True)
    )
    max_len = max(len(suggestions_df), len(top_tokens_df))
    suggestions_df = suggestions_df.reindex(range(max_len))
    top_tokens_df = top_tokens_df.reindex(range(max_len))
    suggestions_df = pd.concat([suggestions_df, top_tokens_df], axis=1)

    # Traceability
    for df_out in (detailed_df, exceptions_df, summary_df, suggestions_df):
        df_out["Account_ID"] = account_id

    return detailed_df, exceptions_df, summary_df, suggestions_df