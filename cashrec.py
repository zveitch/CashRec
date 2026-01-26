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

# def force_dates(df, column_name):
#     # This tries to convert to datetime.
#     # If it sees 2024-05-01, it handles it.
#     # If it sees 01/05/2024, it handles that too.
#     df[column_name] = pd.to_datetime(df[column_name], errors='coerce', dayfirst=False)
#     # Drop rows where the date is completely missing/unparseable
#     return df

def force_dates(df, column_name):
    # 1. Force to string and strip whitespace
    date_series = df[column_name].astype(str).str.strip()

    # 2. Pass 1: Try YYYY-MM-DD (standard ISO format)
    parsed_dates = pd.to_datetime(date_series, format='%Y-%m-%d', errors='coerce')

    # 3. Pass 2: For anything that failed (is NaT), try DD/MM/YYYY
    # The 'fillna' only updates the cells that failed the first pass
    parsed_dates = parsed_dates.fillna(
        pd.to_datetime(date_series, format='%d/%m/%Y', errors='coerce', dayfirst=True)
    )

    # 4. Finalize
    df[column_name] = parsed_dates
    df[column_name] = df[column_name].dt.normalize()      # Convert to date objects
    return df


def use_bankref_dates(df_bank):
    """
    Patches missing Calculated_Date values using the 'Date from BankRef' column.
    """
    # 1. Ensure 'Date from BankRef' is in datetime format to match Calculated_Date
    # We use errors='coerce' to turn junk data into NaT (Not a Time)
    bank_ref_dates = pd.to_datetime(df_bank['Date from BankRef'], format='%Y-%m-%d', errors='coerce')

    # 2. Fill the NaNs in Calculated_Date with the values from bank_ref_dates
    df_bank['Calculated_Date'] = df_bank['Calculated_Date'].fillna(bank_ref_dates)

    # 3. Optional: Normalize to ensure no time-stamp interference
    df_bank['Calculated_Date'] = df_bank['Calculated_Date'].dt.normalize()
    # df_bank = df_bank.drop(columns=['Date'])
    # df_bank = df_bank.rename(columns={'Calculated_Date': 'Date'})
    return df_bank

def force_dates_dayfirst(df, column_name):
    # This tries to convert to datetime.
    # If it sees 2024-05-01, it handles it.
    # If it sees 01/05/2024, it handles that too.
    df[column_name] = pd.to_datetime(df[column_name], errors='coerce', dayfirst=True)
    # Drop rows where the date is completely missing/unparseable
    return df

def cash_rec(data_dir, cash_rec_filename, bankstmt_filename):
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

    # Ensure the columns are strictly datetime objects (errors='coerce' turns bad dates to NaT)
    df_cash['Date'] = pd.to_datetime(df_cash['Date'], errors='coerce')
    #df_bank['Calculated_Date'] = pd.to_datetime(df_bank['Calculated_Date'], errors='coerce')

    #df_cash = force_dates(df_cash, 'Date')
    df_bank = force_dates(df_bank, 'Calculated_Date')
    df_bank = use_bankref_dates(df_bank)
    #print(df_bank['Calculated_Date'])

    print(df_cash.head(3))
    print(df_bank.head(3))

    # Initialize the Reconciled column as an 'object' type (strings)
    df_cash['Reconciled'] = None
    df_cash['Reconciled'] = df_cash['Reconciled'].astype(object)

    df_bank['Reconciled'] = None
    df_bank['Reconciled'] = df_bank['Reconciled'].astype(object)

    #df_bank['Acct_From_Filename'] = df_bank['Acct_From_Filename'].astype(str).str.strip()
    acct_from_filename = int(df_bank['Acct_From_Filename'].iloc[0])
    shortfundname = df_cash['FundShortName'].iloc[0]

    # --- Data Cleaning (Add this section) ---
    def clean_currency(column):
        # 1. Convert to string and remove non-numeric chars
        cleaned = column.astype(str).str.replace(r'[^\d.-]', '', regex=True)
        # 2. Convert to numeric
        numeric_vals = pd.to_numeric(cleaned, errors='coerce').fillna(0)
        # 3. Round to 2 decimal places (nearest penny)
        return numeric_vals.round(2)

    # Helper to calculate Net Amount
    def get_net(df):
        return df['Credit'].fillna(0) - df['Debit'].fillna(0)

    # Apply cleaning to both DataFrames
    df_bank['Credit'] = clean_currency(df_bank['Credit'])
    df_bank['Debit'] = clean_currency(df_bank['Debit'])
    df_cash['Amount'] = clean_currency(df_cash['Amount'])

    df_cash['Net'] = df_cash['Amount']
    df_bank['Net'] = get_net(df_bank)

    match_id = 1

    # --- 1. Check for Missing Bank Statement Months ---
    cash_months = df_cash['Date'].dt.to_period('M').unique()
    bank_months = df_bank['Calculated_Date'].dt.to_period('M').unique()

    missing_bank_months = [m for m in cash_months if m not in bank_months]

    for month in missing_bank_months:
        mask = (df_cash['Date'].dt.to_period('M') == month) & (df_cash['Reconciled'].isna())
#        year = df_cash.loc[mask, 'Date'].dt.year
        df_cash.loc[mask, 'Reconciled'] = f"BANK STATEMENT MISSING - {month}"

    missing_cash_months = [m for m in bank_months if m not in cash_months]

    for month in missing_cash_months:
        mask = (df_bank['Calculated_Date'].dt.to_period('M') == month) & (df_bank['Reconciled'].isna())
#        year = df_bank.loc[mask, 'Calculated_Date'].dt.year
        df_bank.loc[mask, 'Reconciled'] = f"CASH REC MISSING - {month}"

    #print(df_bank[df_bank['Reconciled'].isna()])
    # --- 2. Exact Matches (Date + Amount) ---
    # We iterate to ensure 1-to-1 matching if there are duplicate amounts on the same day
    for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
        # if(str(df_bank['Calculated_Date'].iloc[idx]) == "2022-12-05 00:00:00"):
        #     print(str(df_bank['Calculated_Date'].iloc[idx]),idx)
        #     print(str(row['Date']),idx)
        #     print("FOUND IT")
        # if (str(df_cash['Date'].iloc[idx]) == "2022-12-05 00:00:00"):
        #     print(str(df_cash['Date'].iloc[idx]), idx)
        #     print("FOUND IT")

        match = df_cash[
            (df_cash['Date'] == row['Calculated_Date']) &
            (df_cash['Net'] == row['Net']) &
            (df_cash['Reconciled'].isna())
            ]

        if not match.empty:
            cash_idx = match.index[0]
            label = f"EXACT MATCH - {match_id}"
            df_bank.at[idx, 'Reconciled'] = label
            df_cash.at[cash_idx, 'Reconciled'] = label
            match_id += 1

    # --- 3. Split Payments (2 Cash entries = 1 Bank entry) ---
    for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
        # Find all unreconciled cash items on the same day
        potential_splits = df_cash[(df_cash['Date'] == row['Calculated_Date']) & (df_cash['Reconciled'].isna())]

        # Check combinations of two payments
        found = False
        for i in range(len(potential_splits)):
            for j in range(i + 1, len(potential_splits)):
                if potential_splits.iloc[i]['Net'] + potential_splits.iloc[j]['Net'] == row['Net']:
                    label = f"EXACT BUT SPLIT - {match_id}"
                    df_bank.at[idx, 'Reconciled'] = label
                    df_cash.at[potential_splits.index[i], 'Reconciled'] = label
                    df_cash.at[potential_splits.index[j], 'Reconciled'] = label
                    match_id += 1
                    found = True
                    break
            if found: break

    # --- 4. Bad Date: Year Errors (1, 2, 10 years) ---
    year_offsets = [1, 2, 10]
    for offset in year_offsets:
        for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
            # Look for cash entries where bank date - cash date = X years
            match = df_cash[
                (abs(df_bank.at[idx, 'Calculated_Date'].year - df_cash['Date'].dt.year) == offset) &
                (df_cash['Date'].dt.month == row['Calculated_Date'].month) &
                (df_cash['Date'].dt.day == row['Calculated_Date'].day) &
                (df_cash['Net'] == row['Net']) &
                (df_cash['Reconciled'].isna())
                ]
            if not match.empty:
                cash_idx = match.index[0]
                label = f"BAD DATE, CORRECT AMOUNT - Date off {offset} years - {match_id}"
                df_bank.at[idx, 'Reconciled'] = label
                df_cash.at[cash_idx, 'Reconciled'] = label
                match_id += 1

    # --- 5. Minor Bad Date: Day Errors (1 to 7 days) ---
    for days in range(1, 28):
        for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
            # Check if the cash record is within X days of the bank record
            match = df_cash[
                (abs((df_cash['Date'] - row['Calculated_Date']).dt.days) == days) &
                (df_cash['Net'] == row['Net']) &
                (df_reconciled := df_cash['Reconciled'].isna())
                ]
            if not match.empty:
                cash_idx = match.index[0]
                label = f"MINOR BAD DATE, CORRECT AMOUNT - Date off {days} days - {match_id}"
                df_bank.at[idx, 'Reconciled'] = label
                df_cash.at[cash_idx, 'Reconciled'] = label
                match_id += 1

    # --- 5c. Small Amount Difference (Penny Matching) ---
    # We loop from 0.01 to 0.99 difference
    for diff in range(1, 100):
        tolerance = diff / 100

        for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
            # Look for a cash entry on the same date where the amount is within the tolerance
            # and hasn't been reconciled yet
            match = df_cash[
                (df_cash['Date'] == row['Calculated_Date']) &
                (abs(df_cash['Net'] - row['Net']).round(2) == tolerance) &
                (df_cash['Reconciled'].isna())
                ]

            if not match.empty:
                cash_idx = match.index[0]
                actual_diff = (row['Net'] - match.iloc[0]['Net']).round(2)

                label = f"MINOR AMOUNT DIFF - {actual_diff} difference - {match_id}"

                df_bank.at[idx, 'Reconciled'] = label
                df_cash.at[cash_idx, 'Reconciled'] = label
                match_id += 1

    # --- 5d. Split Payments (Same Date) with Minor Amount Difference ---
    for diff in range(1, 100):
        tolerance = round(diff / 100, 2)

        for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
            bank_net = row['Net']
            bank_date = row['Calculated_Date']

            # Find all unreconciled cash items on the exact same day
            potential_splits = df_cash[
                (df_cash['Date'] == bank_date) &
                (df_cash['Reconciled'].isna())
                ]

            found = False
            # Check combinations of two payments
            for i in range(len(potential_splits)):
                for j in range(i + 1, len(potential_splits)):
                    cash_sum = round(potential_splits.iloc[i]['Net'] + potential_splits.iloc[j]['Net'], 2)

                    if round(abs(bank_net - cash_sum), 2) == tolerance:
                        actual_diff = round(bank_net - cash_sum, 2)
                        label = f"SPLIT MATCH, MINOR DIFF - {actual_diff} difference - {match_id}"

                        # Mark Bank row
                        df_bank.at[idx, 'Reconciled'] = label
                        # Mark both Cash rows
                        df_cash.at[potential_splits.index[i], 'Reconciled'] = label
                        df_cash.at[potential_splits.index[j], 'Reconciled'] = label

                        match_id += 1
                        found = True
                        break
                if found: break

    for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
        bank_date = row['Calculated_Date']
        bank_net = row['Net']

        # 1. Find all unreconciled cash items on the exact same day
        exact_day_cash = df_cash[(df_cash['Date'] == bank_date) & (df_cash['Reconciled'].isna())]

        # 2. Find all unreconciled cash items within +/- 7 days (excluding the exact day)
        # This creates a 'window' for the second part of the split
        near_cash = df_cash[
            (df_cash['Reconciled'].isna()) &
            (abs((df_cash['Date'] - bank_date).dt.days) <= 7) &
            (df_cash['Date'] != bank_date)
            ]

        found = False
        for i_idx, i_row in exact_day_cash.iterrows():
            for j_idx, j_row in near_cash.iterrows():
                if round(i_row['Net'] + j_row['Net'], 2) == bank_net:
                    day_diff = int(abs((j_row['Date'] - bank_date).days))

                    label_bank = f"MATCHED, BUT SPLIT, ONE PAYMENT OFF BY {day_diff} days - {match_id}"
                    label_cash_exact = f"MATCHED, BUT SPLIT, ONE PAYMENT OFF BY {day_diff} days - {match_id}"
                    label_cash_off = f"MATCHED, BUT SPLIT, ONE PAYMENT OFF BY {day_diff} days - {match_id}"

                    # Assign labels
                    df_bank.at[idx, 'Reconciled'] = label_bank
                    df_cash.at[i_idx, 'Reconciled'] = label_cash_exact
                    df_cash.at[j_idx, 'Reconciled'] = label_cash_off

                    match_id += 1
                    found = True
                    break
            if found: break

    # --- 5f. Split Payments (Same Day) with Date Shift (< 3 days) ---
    # We loop through day offsets 1, 2, and 3
    for d_offset in range(1, 4):
        for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
            bank_date = row['Calculated_Date']
            bank_net = row['Net']

            # Find all unreconciled cash items that are exactly d_offset days away
            # We group by Date because the user specified the two payments are on the SAME day
            potential_days = df_cash[
                (df_cash['Reconciled'].isna()) &
                (abs((df_cash['Date'] - bank_date).dt.days) == d_offset)
                ]

            # Get unique dates within that set to check each day's group of payments
            unique_dates = potential_days['Date'].unique()

            found = False
            for c_date in unique_dates:
                day_items = potential_days[potential_days['Date'] == c_date]

                # Check combinations of two payments on that specific day
                for i in range(len(day_items)):
                    for j in range(i + 1, len(day_items)):
                        cash_sum = round(day_items.iloc[i]['Net'] + day_items.iloc[j]['Net'], 2)

                        if cash_sum == bank_net:
                            label = f"SPLIT MATCH, DATE SHIFT - {d_offset} days off - {match_id}"

                            # Mark Bank row
                            df_bank.at[idx, 'Reconciled'] = label
                            # Mark both Cash rows
                            df_cash.at[day_items.index[i], 'Reconciled'] = label
                            df_cash.at[day_items.index[j], 'Reconciled'] = label

                            match_id += 1
                            found = True
                            break
                    if found: break
                if found: break

    # --- 8. Moderate Bad Date: Day Errors (1 to 3 months) --- !!! CAUTION MANY PAYMENTS OFF BY A MONTH, THIS SHOULD BE ONE OF LAST CHECKS
    # We loop through a range of months (e.g., 1 to 3 months)
    for m_offset in range(1, 4):
        for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
            bank_date = row['Calculated_Date']

            # We look for a cash entry where:
            # 1. Year and Day are the same
            # 2. Month difference is exactly m_offset
            # 3. Amount matches exactly
            match = df_cash[
                (df_cash['Reconciled'].isna()) &
                (df_cash['Net'] == row['Net']) &
                (df_cash['Date'].dt.year == bank_date.year) &
                (df_cash['Date'].dt.day == bank_date.day) &
                (abs(df_cash['Date'].dt.month - bank_date.month) == m_offset)
                ]

            if not match.empty:
                cash_idx = match.index[0]
                label = f"MONTH ERROR, CORRECT AMOUNT - Off by {m_offset} months - {match_id}"

                df_bank.at[idx, 'Reconciled'] = label
                df_cash.at[cash_idx, 'Reconciled'] = label
                match_id += 1

    # --- 9. Bulk Daily Match (Totals per Date) ---
    # 1. Pre-calculate the daily sums for unreconciled cash
    # This creates a Series where the index is the Date and the value is the total Net
    cash_daily_sums = df_cash[df_cash['Reconciled'].isna()].groupby('Date')['Net'].sum().round(2)

    # 2. Iterate through unreconciled Bank rows
    for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
        bank_date = row['Calculated_Date']
        bank_amount = round(row['Net'], 2)

        # 3. Check if this specific bank amount matches the total cash for that day
        # We check:
        #   a) Does this date exist in our cash summary?
        #   b) Is the total on that day exactly equal to this one bank row?
        if bank_date in cash_daily_sums.index and bank_amount == cash_daily_sums[bank_date]:
            label = f"SINGLE BANK TO DAILY CASH - {match_id}"

            # Mark the single Bank row
            df_bank.at[idx, 'Reconciled'] = label

            # Mark ALL unreconciled Cash rows for that date
            df_cash.loc[
                (df_cash['Date'] == bank_date) & (df_cash['Reconciled'].isna()),
                'Reconciled'
            ] = label

            match_id += 1

    print(f"Completed One-to-Many matching using Calculated_Date.")


    # 1. Group by Calculated_Date on the Bank side, and Date on the Cash side
    # We round to 2 decimal places to prevent floating point mismatch (e.g., 0.000000001)
    # bank_daily = df_bank[df_bank['Reconciled'].isna()].groupby('Calculated_Date') #['Net'].sum().round(2)
    # print("BANK:",bank_daily.head())
    # cash_daily = df_cash[df_cash['Reconciled'].isna()].groupby('Date') #['Net'].sum().round(2)
    # print("CASH:",cash_daily.head())
    # # 2. Align the labels (Indexes)
    # # This finds dates present in both 'Calculated_Date' and cash 'Date'
    # common_dates = bank_daily.index.intersection(cash_daily.index)
    # print("COMMON DATES:", common_dates)
    # # 3. Compare totals for the overlapping dates only
    # # This creates a boolean list of which dates have identical sums
    # matching_dates = common_dates[bank_daily.loc[common_dates] == cash_daily.loc[common_dates]]

    # # 4. Loop through and mark the matches
    # for m_date in matching_dates:
    #     # Avoid matching dates where the sum is 0.00
    #     if bank_daily[m_date] == 0:
    #         continue
    #
    #     label = f"DAILY TOTAL MATCH - {match_id}"
    #
    #     # Mark the Bank side using 'Calculated_Date'
    #     df_bank.loc[
    #         (df_bank['Calculated_Date'] == m_date) & (df_bank['Reconciled'].isna()),
    #         'Reconciled'
    #     ] = label
    #
    #     # Mark the Cash side using 'Date'
    #     df_cash.loc[
    #         (df_cash['Date'] == m_date) & (df_cash['Reconciled'].isna()),
    #         'Reconciled'
    #     ] = label
    #
    #     match_id += 1
    #
    # print(f"Matched {len(matching_dates)} full days via Daily Totals.")

    # # --- 9. Triple Split Payments (3 Cash entries = 1 Bank entry) ---
    # for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
    #     # Only look at unreconciled cash items on the same day as the bank entry
    #     potential_splits = df_cash[
    #         (df_cash['Date'] == row['Calculated_Date']) &
    #         (df_cash['Reconciled'].isna())
    #         ]
    #
    #     # We need at least 3 items to check for a triple split
    #     if len(potential_splits) < 3:
    #         continue
    #
    #     found = False
    #     # Triple loop to check combinations of i, j, and k
    #     for i in range(len(potential_splits)):
    #         for j in range(i + 1, len(potential_splits)):
    #             for k in range(j + 1, len(potential_splits)):
    #
    #                 cash_sum = round(
    #                     potential_splits.iloc[i]['Net'] +
    #                     potential_splits.iloc[j]['Net'] +
    #                     potential_splits.iloc[k]['Net'], 2
    #                 )
    #
    #                 if cash_sum == row['Net']:
    #                     label = f"TRIPLE SPLIT MATCH - {match_id}"
    #
    #                     # Mark Bank row
    #                     df_bank.at[idx, 'Reconciled'] = label
    #
    #                     # Mark all three Cash rows
    #                     df_cash.at[potential_splits.index[i], 'Reconciled'] = label
    #                     df_cash.at[potential_splits.index[j], 'Reconciled'] = label
    #                     df_cash.at[potential_splits.index[k], 'Reconciled'] = label
    #
    #                     match_id += 1
    #                     found = True
    #                     break
    #             if found: break
    #         if found: break

    # --- 8. Final Audit: Check for Entirely Missing Months ---

    # 1. Extract all unique year-month periods from both datasets
    # We use .to_period('M') to turn '2026-01-20' into '2026-01'
    cash_months = set(df_cash['Date'].dt.to_period('M').unique())
    bank_months = set(df_bank['Calculated_Date'].dt.to_period('M').unique())

    # 2. Identify Discrepancies
    missing_in_bank = sorted(list(cash_months - bank_months))
    missing_in_cash = sorted(list(bank_months - cash_months))

    # 3. Print the Warning Report
    print("\n" + "=" * 40)
    print(f"       CASHREC AUDIT FOR {shortfundname} {acct_from_filename} ")
    print("=" * 40)

    if missing_in_bank:
        print(f"⚠️  WARNING: Missing BANK STATEMENTS for the following months:")
        for m in missing_in_bank:
            print(f"   - {m}")
    else:
        print("✅ Bank Statement coverage is complete relative to CashRec.")

    print("-" * 40)

    if missing_in_cash:
        print(f"⚠️  WARNING: Missing CASHRECS for the following months:")
        for m in missing_in_cash:
            print(f"   - {m}")
    else:
        print("✅ CashRec coverage is complete relative to Bank Statements.")

    print("=" * 40)
    # --- 6. List Remaining Differences ---
    unreconciled_bank = df_bank[df_bank['Reconciled'].isna()]
    unreconciled_cash = df_cash[df_cash['Reconciled'].isna()]

    print(f"Unreconciled Bank Items: {len(unreconciled_bank)}")
    print(f"Unreconciled Cash Items: {len(unreconciled_cash)}")

    print("=" * 40 + "\n")

    # --- 9. Save Summary to Audit Log CSV ---
    log_file = project_root / "audit_log.csv"

    # Prepare the data row
    # We join the lists of missing months into strings so they fit in a single CSV cell
    log_data = {
        "Account Number": acct_from_filename,
        "Fund Short Name": shortfundname,
        "Unreconciled Cash Lines": len(unreconciled_cash),
        "Unreconciled Bank Lines": len(unreconciled_bank),
        "Number of Missing Bank Statements": len(missing_in_bank),
        "Number of Missing Cash Rec Months": len(missing_in_cash),
        "List of Missing Bank Statements": ", ".join([str(m) for m in missing_in_bank]),
        "List of Missing Cash Rec Months": ", ".join([str(m) for m in missing_in_cash])
    }

    # Define headers
    headers = log_data.keys()

    # Write to CSV (Append mode 'a')
    file_exists = os.path.isfile(log_file)

    with open(log_file, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)

        # Only write header if the file is new
        if not file_exists:
            writer.writeheader()

        writer.writerow(log_data)

    print(f"Audit log updated: {log_file}")

    # Save to CSV
    df_bank.to_csv(data_dir/'reconciled_bank.csv', index=False)
    df_cash.to_csv(data_dir/'reconciled_cash.csv', index=False)

    print("Reconciliation complete. Files saved.")

    # --- 7. Create Combined Stacked Report (Clean Version) ---

    # 1. Separate Matched from Unreconciled
    # We identify 'Matched' as rows where 'Reconciled' is NOT null/empty
    bank_matched = df_bank[df_bank['Reconciled'].notna()].copy()
    cash_matched = df_cash[df_cash['Reconciled'].notna()].copy()

    # Unreconciled are rows where 'Reconciled' IS null/empty
    bank_unrec = df_bank[df_bank['Reconciled'].isna()].copy()
    cash_unrec = df_cash[df_cash['Reconciled'].isna()].copy()

    # 2. Block 1: Reconciled matches (Side-by-Side)
    bank_sub = bank_matched[[
        'Calculated_Date', 'Acct_From_Filename', 'Company_Name', 'CCY_Type',
        'Description1A', 'Description1B', 'Description2', 'Debit', 'Credit', 'Reconciled'
    ]]
    cash_sub = cash_matched[[
        'Reconciled', 'FundShortName', 'Type', 'Date', 'Detail', 'Net'
    ]].rename(columns={'Date': 'Cash_Date', 'Net': 'Amount'})

    # 1. Define the 'Missing' filter
    missing_pattern = "MISSING"
    # Missing = Has an ID and contains 'MISSING'
    # (Usually these are in df_cash because the bank side doesn't exist)
    bankstmt_missing = df_cash[df_cash['Reconciled'].str.contains(missing_pattern, na=False)].copy()

    # Left merge handles the 1-to-1 and 1-to-many (splits)
    block1 = pd.merge(bank_sub, cash_sub, on='Reconciled', how='left')

    # Visual Blanking: Only show Bank info on the first row of a split match
    bank_cols = ['Calculated_Date', 'Acct_From_Filename', 'Company_Name', 'CCY_Type',
                 'Description1A', 'Description1B', 'Description2', 'Debit', 'Credit']
    for col in bank_cols:
        block1[col] = block1[col].astype(object)

    # We only want to blank if:
    # 1. The ID is duplicated (Split payment)
    # 2. AND it's NOT a 'Missing' status label
    is_duplicate = block1.duplicated(subset=['Reconciled'])
    is_status_label = block1['Reconciled'].str.contains("MISSING|UNRECONCILED", na=False)

    # Apply blanking only to real split duplicates
    block1.loc[is_duplicate & ~is_status_label, bank_cols] = ""

    #  block1.loc[block1.duplicated(subset=['Reconciled']), bank_cols] = ""

    # 6. Block 4: Missing Statements (Cash side only)
    block4 = bankstmt_missing[['Reconciled', 'FundShortName', 'Type', 'Date', 'Detail', 'Net']].rename(
        columns={'Date': 'Cash_Date', 'Net': 'Amount'}).copy()
    for col in bank_cols: block4[col] = ""

    # 3. Block 2: Unreconciled Bank Rows
    block2 = bank_unrec[[
        'Calculated_Date', 'Acct_From_Filename', 'Company_Name', 'CCY_Type',
        'Description1A', 'Description1B', 'Description2', 'Debit', 'Credit', 'Reconciled'
    ]].copy()  # Added .copy() here for extra safety

    for col in ['FundShortName', 'Type', 'Cash_Date', 'Detail', 'Amount']:
        block2[col] = ""  # This will no longer trigger the warning

    # 4. Block 3: Unreconciled Cash Rows
    block3 = cash_unrec[[
        'Reconciled', 'FundShortName', 'Type', 'Date', 'Detail', 'Net'
    ]].rename(columns={'Date': 'Cash_Date', 'Net': 'Amount'}).copy()  # Added .copy() here

    for col in bank_cols:
        block3[col] = ""  # This will no longer trigger the warning

    # 5. Combine and Export
    final_report = pd.concat([block1, block4, block2, block3], ignore_index=True)

    # Reorder columns for the final file
    cols = bank_cols + ['Reconciled'] + ['FundShortName', 'Type', 'Cash_Date', 'Detail', 'Amount']
    final_report = final_report[cols]

    # Save to CSV
    final_report.to_csv(data_dir/f'cashrec_report_{acct_from_filename}.csv', index=False)
    return final_report

## usage
USDFundList_filename = "USDFund_Accountlist.csv"
df_USDFundList = pd.read_csv(data_dir / USDFundList_filename)
##"bankstmt_flows_400310050003.csv"

for i, fund in enumerate(df_USDFundList['FundShortName']):
    if i < 133:
        try:
            account_number = df_USDFundList['Account'].iloc[i]
            fund_short_name = df_USDFundList['FundShortName'].iloc[i]
            print(i, fund_short_name, account_number)
            #cash_rec_filename = f'cash_rec_{account_number}.csv'
            cash_rec_filename = f'FullCashFlows.csv'
            bankstmt_filename = f'bankstmt_flows_{account_number}.csv'
            cash_rec(data_dir, cash_rec_filename, bankstmt_filename)
        except Exception as e:
            print(e)
# account_number = "400310062003"
# #cash_rec_filename = f'cash_rec_{account_number}.csv'
# cash_rec_filename = f'FullCashFlows.csv'
# bankstmt_filename = f'bankstmt_flows_{account_number}.csv'
#
# cash_rec(data_dir, cash_rec_filename, bankstmt_filename)