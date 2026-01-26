import pandas as pd
import numpy as np
from pathlib import Path
import csv
import os

# Define your paths
# 1. Get the path of the current script (src/<filename>.py)
current_file = Path(__file__).resolve()
src_directory = current_file.parent
project_root = src_directory.parent
data_dir = project_root / "data"
input_folder = ("C:/Users/z.veitch/OneDrive - wafracapital.com/ICP-UK - Documents/Track Record/Python Output/BankStmtCSV")
input_path = Path(input_folder)

# Default terminal output formatting
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')

def date_check(data_dir, cash_rec_filename, bankstmt_filename):
    # List of keywords to exclude
    exclude_keywords = [
        "OPENING BALANCE",
        "CLOSING BALANCE",
        "BALANCE CARRIED FORWARD",
        "BALANCE BROUGHT FORWARD"
    ]

    # Create a combined regex pattern (e.g., "OPENING BALANCE|CLOSING BALANCE|...")
    pattern = '|'.join(exclude_keywords)

    # --- 0. Load Data ---
    # Assuming CSV files for this example. Replace with your file paths.
    # Ensure your date columns are in datetime format.
    df_cash = pd.read_csv(data_dir / cash_rec_filename)
    df_bank = pd.read_csv(input_path / bankstmt_filename)

    # Filter cash flows for the right fund
    target_funds =  [fund_short_name]
    df_cash = df_cash[df_cash['FundShortName'].isin(target_funds)].copy()
    df_cash = df_cash.reset_index(drop=True)
    print(f"Filtered Cash Rec to {len(df_cash)} relevant rows.")

    # Filter the bank statement
    # ~ is the 'NOT' operator, so we keep rows that DO NOT contain the pattern
    df_bank = df_bank[~df_bank['Description1A'].astype(str).str.contains(pattern, case=False, na=False)].copy()

    # Reset the index to keep things clean for the matching loops
    df_bank = df_bank.reset_index(drop=True)

    print(df_cash.head(5))
    print(df_bank.head(300))

    print(df_bank['Date'].dtype)
    print(f"Missing dates: {df_bank['Date'].isna().sum()}")

    return df_cash, df_bank

## usage
USDFundList_filename = "USDFund_Accountlist.csv"
df_USDFundList = pd.read_csv(data_dir / USDFundList_filename)
##"bankstmt_flows_400310050003.csv"

for i, fund in enumerate(df_USDFundList['FundShortName']):
    if i == 38:
        try:
            account_number = df_USDFundList['Account'].iloc[i]
            fund_short_name = df_USDFundList['FundShortName'].iloc[i]
            print(i, fund_short_name, account_number)
            #cash_rec_filename = f'cash_rec_{account_number}.csv'
            cash_rec_filename = f'FullCashFlows.csv'
            bankstmt_filename = f'bankstmt_flows_{account_number}.csv'
            date_check(data_dir, cash_rec_filename, bankstmt_filename)
            #cash_rec(data_dir, cash_rec_filename, bankstmt_filename)
        except Exception as e:
            print(e)