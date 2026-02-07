# /src/cli.py
import argparse
from pathlib import Path
import glob
import pandas as pd

from .config import INPUT_DIR, OUTPUT_DIR, RULES_DIR, DEFAULT_OUTPUT_ENCODING
from .rules_csv import load_rules_from_csv
from .reconcile import reconcile_account

def _load_accounts_from_csv(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Accounts file not found: {path}")
    df = pd.read_csv(path, dtype=str)
    cols = [c.lower() for c in df.columns]
    if "account_id" not in cols:
        raise ValueError("accounts_order.csv must contain a column named 'account_id'")
    return [str(x).strip() for x in df[df.columns[cols.index("account_id")]].dropna().tolist()]

def _list_available_accounts(input_dir: Path) -> dict[str, Path]:
    # Map account_id -> file path
    mapping = {}
    for csv_path in glob.glob(str(input_dir / "cashrec_report_*.csv")):
        p = Path(csv_path)
        account_id = p.stem.replace("cashrec_report_", "")
        mapping[account_id] = p
    return mapping

def main():
    parser = argparse.ArgumentParser(description="Cash reconciliation tagging + export (ordered accounts via CSV)")
    parser.add_argument("--input", type=Path, default=INPUT_DIR, help="Directory containing cashrec_report_{account}.csv")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR, help="Directory to write outputs")
    parser.add_argument("--rules-dir", type=Path, default=RULES_DIR, help="Directory containing CSV rules")
    parser.add_argument("--accounts-file", type=Path, default=RULES_DIR / "accounts_order.csv",
                        help="Path to accounts_order.csv with column 'account_id' (newest→oldest)")
    parser.add_argument("--include-unlisted", action="store_true",
                        help="Also process accounts in input dir that are NOT in accounts_order.csv (appended after)")
    parser.add_argument("--strict-order", action="store_true",
                        help="Abort if any account in accounts_order.csv is missing in input dir")
    args = parser.parse_args()

    # Load rules (CSV)
    print(f"[INFO] Loading rules from CSVs in {args.rules_dir}")
    rules = load_rules_from_csv(args.rules_dir)

    # Determine account file mapping
    available = _list_available_accounts(args.input)
    if not available:
        print(f"[WARN] No CSVs found in {args.input} matching 'cashrec_report_*.csv'")
        return

    # Load ordered list
    ordered_accounts = _load_accounts_from_csv(args.accounts_file)
    print(f"[INFO] Loaded {len(ordered_accounts)} accounts from {args.accounts_file}")

    # Validate & queue
    missing = [a for a in ordered_accounts if a not in available]
    if missing:
        msg = f"[WARN] {len(missing)} accounts in your list have no matching CSV in {args.input}: {missing}"
        if args.strict_order:
            raise FileNotFoundError(msg)
        else:
            print(msg)

    queue = [a for a in ordered_accounts if a in available]

    if args.include_unlisted:
        extras = [a for a in sorted(available.keys()) if a not in set(queue)]
        if extras:
            print(f"[INFO] Appending {len(extras)} unlisted accounts after your ordered list: {extras}")
            queue.extend(extras)

    # Execute
    args.output.mkdir(parents=True, exist_ok=True)
    for account_id in queue:
        csv_path = available[account_id]
        print(f"[INFO] Processing account (in order): {account_id} -> {csv_path}")
        df = pd.read_csv(csv_path, dtype=str)

        detailed_df, exceptions_df, summary_df, suggestions_df = reconcile_account(df, account_id, rules)

        # Optional: mislabel auditor, if present
        try:
            from .audit_mislabels import audit_mgmt_vs_expenses
            mislabels_df = audit_mgmt_vs_expenses(detailed_df)
        except Exception:
            mislabels_df = pd.DataFrame()

        out_prefix = args.output / f"{account_id}"
        detailed_df.to_csv(f"{out_prefix}_reconciliation_detailed.csv", index=False, encoding=DEFAULT_OUTPUT_ENCODING)
        exceptions_df.to_csv(f"{out_prefix}_exceptions.csv", index=False, encoding=DEFAULT_OUTPUT_ENCODING)
        summary_df.to_csv(f"{out_prefix}_summary.csv", index=False, encoding=DEFAULT_OUTPUT_ENCODING)
        suggestions_df.to_csv(f"{out_prefix}_tag_suggestions.csv", index=False, encoding=DEFAULT_OUTPUT_ENCODING)
        if not mislabels_df.empty:
            mislabels_df.to_csv(f"{out_prefix}_mislabel_suspicions.csv", index=False, encoding=DEFAULT_OUTPUT_ENCODING)

        print(f"[OK] Wrote outputs for {account_id} to {args.output}")

if __name__ == "__main__":
    main()