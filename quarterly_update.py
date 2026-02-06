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
#fund_folder = ("J:/Wafra Structured Leasing Funds")
fund_folder = ("J:/")
fund_path = Path(fund_folder)

# Default terminal output formatting
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')

# --- CONFIGURATION ---
search_root = fund_path  # Change this to your starting folder
output_file = 'Folder_Search_Results_ALL_J_DRIVE.csv'
target_names = ["Cash Recs", "Bank Statements","Cash rec", "Cash Rec","Bank","BS"]

# This list will store our findings for the final Excel export
found_folders = []

print(f"--- Starting search in: {search_root} ---")

try:
    # We iterate through everything. rglob('*') gets all items.
    for path in search_root.rglob('*'):
        try:
            # Check if it's a directory and matches our criteria
            if path.is_dir():
                # We check if 'Cash Recs' is the exact name OR if 'Bank Statements' is in the name
                if path.name == "Cash Recs" or "Bank Statements" in path.name:
                    # Print immediately to console
                    print(f"MATCH FOUND: {path}")

                    # Add to our list for Excel
                    found_folders.append({
                        "Folder Name": path.name,
                        "Full Path": str(path)
                    })
        except PermissionError:
            # Skip folders we don't have access to (common in system directories)
            continue

except Exception as e:
    print(e)


# --- EXPORT TO EXCEL ---
if found_folders:
    df = pd.DataFrame(found_folders)
    df.to_csv(output_file, index=False)
    print(f"\n--- Success! Results saved to {output_file} ---")
else:
    print("\nNo matching folders were found.")