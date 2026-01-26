import pandas as pd
import numpy as np
from pathlib import Path

# Define your paths
# 1. Get the path of the current script (src/<filename>.py)
current_file = Path(__file__).resolve()
src_directory = current_file.parent
project_root = src_directory.parent
data_dir = project_root / "data"

# Default terminal output formatting
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')

def force_dates(df, column_name):
    # This tries to convert to datetime.
    # If it sees 2024-05-01, it handles it.
    # If it sees 01/05/2024, it handles that too.
    df[column_name] = pd.to_datetime(df[column_name], errors='coerce', dayfirst=False)
    # Drop rows where the date is completely missing/unparseable
    return df

def force_dates_dayfirst(df, column_name):
    # This tries to convert to datetime.
    # If it sees 2024-05-01, it handles it.
    # If it sees 01/05/2024, it handles that too.
    df[column_name] = pd.to_datetime(df[column_name], errors='coerce', dayfirst=True)
    # Drop rows where the date is completely missing/unparseable
    return df

# --- 0. Load Data ---
# Assuming CSV files for this example. Replace with your file paths.
# Ensure your date columns are in datetime format.
df_cash = pd.read_csv( data_dir/'cash_rec.csv')
df_bank = pd.read_csv(data_dir/'bankstmt_flows_400310052003.csv')

# List of keywords to exclude
exclude_keywords = [
    "OPENING BALANCE",
    "CLOSING BALANCE",
    "BALANCE CARRIED FORWARD",
    "BALANCE BROUGHT FORWARD"
]

# Create a combined regex pattern (e.g., "OPENING BALANCE|CLOSING BALANCE|...")
pattern = '|'.join(exclude_keywords)

# Filter the bank statement
# ~ is the 'NOT' operator, so we keep rows that DO NOT contain the pattern
df_bank = df_bank[~df_bank['Description1A'].astype(str).str.contains(pattern, case=False, na=False)].copy()

# Reset the index to keep things clean for the matching loops
df_bank = df_bank.reset_index(drop=True)

# Ensure the columns are strictly datetime objects (errors='coerce' turns bad dates to NaT)
df_cash['Date'] = pd.to_datetime(df_cash['Date'], errors='coerce')
df_bank['Date'] = pd.to_datetime(df_bank['Calculated_Date'], errors='coerce')

df_cash = force_dates_dayfirst(df_cash, 'Date')
df_bank = force_dates(df_bank, 'Calculated_Date')

print(df_cash.head(3))
print(df_bank.head(3))

# Initialize the Reconciled column as an 'object' type (strings)
df_cash['Reconciled'] = None
df_cash['Reconciled'] = df_cash['Reconciled'].astype(object)

df_bank['Reconciled'] = None
df_bank['Reconciled'] = df_bank['Reconciled'].astype(object)


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
bank_months = df_bank['Date'].dt.to_period('M').unique()

missing_bank_months = [m for m in cash_months if m not in bank_months]

for month in missing_bank_months:
    mask = (df_cash['Date'].dt.to_period('M') == month) & (df_cash['Reconciled'].isna())
    year = df_cash.loc[mask, 'Date'].dt.year
    df_cash.loc[mask, 'Reconciled'] = f"BANK STATEMENT MISSING - {month} {year}"

missing_cash_months = [m for m in bank_months if m not in cash_months]

for month in missing_cash_months:
    mask = (df_bank['Calculated_Date'].dt.to_period('M') == month) & (df_bank['Reconciled'].isna())
    year = df_bank.loc[mask, 'Calculated_Date'].dt.year
    df_bank.loc[mask, 'Reconciled'] = f"CASH REC MISSING - {month} {year}"


# --- 2. Exact Matches (Date + Amount) ---
# We iterate to ensure 1-to-1 matching if there are duplicate amounts on the same day
for idx, row in df_bank[df_bank['Reconciled'].isna()].iterrows():
    match = df_cash[
        (df_cash['Date'] == row['Date']) &
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
    potential_splits = df_cash[(df_cash['Date'] == row['Date']) & (df_cash['Reconciled'].isna())]

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
            (abs(df_bank.at[idx, 'Date'].year - df_cash['Date'].dt.year) == offset) &
            (df_cash['Date'].dt.month == row['Date'].month) &
            (df_cash['Date'].dt.day == row['Date'].day) &
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
            (abs((df_cash['Date'] - row['Date']).dt.days) == days) &
            (df_cash['Net'] == row['Net']) &
            (df_reconciled := df_cash['Reconciled'].isna())
            ]
        if not match.empty:
            cash_idx = match.index[0]
            label = f"MINOR BAD DATE, CORRECT AMOUNT - Date off {days} days - {match_id}"
            df_bank.at[idx, 'Reconciled'] = label
            df_cash.at[cash_idx, 'Reconciled'] = label
            match_id += 1

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


# --- 6. List Remaining Differences ---
unreconciled_bank = df_bank[df_bank['Reconciled'].isna()]
unreconciled_cash = df_cash[df_cash['Reconciled'].isna()]

print(f"Unreconciled Bank Items: {len(unreconciled_bank)}")
print(f"Unreconciled Cash Items: {len(unreconciled_cash)}")

# Save to CSV
df_bank.to_csv(data_dir/'reconciled_bank.csv', index=False)
df_cash.to_csv(data_dir/'reconciled_cash.csv', index=False)

print("Reconciliation complete. Files saved.")

# --- 7. Create Combined Grouped Report ---

# Filter for items that have been reconciled (exclude NaN/None)
# 1. Label the leftovers first
df_bank['Reconciled'] = df_bank['Reconciled'].fillna("*UNRECONCILED*")
df_cash['Reconciled'] = df_cash['Reconciled'].fillna("*UNRECONCILED*")


# Prepare Bank columns (rename or select only the ones you requested)
bank_subset = df_bank[[
    'Acct_From_Filename', 'Company_Name', 'CCY_Type',
    'Date', 'Description1A', 'Description1B',
    'Description2', 'Debit', 'Credit', 'Reconciled'
]].rename(columns={'Date': 'Calculated_Date'})
bank_subset['Source'] = 'BANK'

# Prepare Cash columns
cash_subset = df_cash[[
    'FundShortName', 'Type', 'Date', 'Detail',
    'Net', 'Reconciled'
]].rename(columns={'Net': 'Amount','Date':'Cash_Date'})
cash_subset['Source'] = 'CASH'

# 3. Merge (Outer Join)
# Using 'outer' ensures that an unreconciled Cash item still appears
# even if it has no match on the Bank side.
combined_df = pd.merge(bank_subset, cash_subset, on='Reconciled', how='outer')

# 4. Sorting
# Put Unreconciled items at the bottom or keep them in date order
combined_df = combined_df.sort_values(by=['Calculated_Date', 'Reconciled'], na_position='last')

# 6. Blank out duplicate Bank Info for split payments
# This makes it so the Bank info only shows once, even if there are 2+ Cash rows
bank_columns = [
    'Calculated_Date', 'Acct_From_Filename', 'Company_Name',
    'CCY_Type', 'Description1A', 'Description1B',
    'Description2', 'Debit', 'Credit'
]

# 5. Blank out duplicate Bank headers for split payments
# We only do this for rows that AREN'T marked as UNRECONCILED
mask = (combined_df.duplicated(subset=['Reconciled'])) & (combined_df['Reconciled'] != "*UNRECONCILED*")
combined_df.loc[mask, bank_columns] = ""

# 7. Final Polish: Reorder columns to put Reconciled ID in the middle as a separator
cols = bank_columns + ['Reconciled'] + ['FundShortName', 'Type', 'Cash_Date', 'Detail', 'Amount']
combined_df = combined_df[cols]

# Save to CSV
combined_df.to_csv(data_dir/'reconciliation_final_report.csv', index=False)