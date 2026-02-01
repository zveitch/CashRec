import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import os

# Define your paths
# 1. Get the path of the current script (src/<filename>.py)
current_file = Path(__file__).resolve()
src_directory = current_file.parent
project_root = src_directory.parent
data_dir = project_root / "data"
input_folder = ("C:/Users/zachv/OneDrive - wafracapital.com/ICP-UK - Documents/Track Record/Python Output/CashRecs-20260130-ANNA_VERSION")
input_path = Path(input_folder)

#changes_file = ("Auto_CashRec_Changes.csv")
#changes_file = "Manual_CashRec_Changes.csv"
changes_file = "matched_changes.csv"

#master_file = "FullCashflows_20260126v7.csv"
master_file = "master_updated_20260201_225121.csv"

# Default terminal output formatting
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')

import pandas as pd
from datetime import datetime
import os

import pandas as pd
from datetime import datetime
import os


def update_master_file(master_path, changes_path, date_column_index=0):
    # 1. Load data
    master_df = pd.read_csv(master_path, header=None)
    changes_df = pd.read_csv(changes_path, header=None)

    # Cleaning: Convert to string/strip for matching logic only
    master_df_clean = master_df.astype(str).apply(lambda x: x.str.strip())
    changes_df_clean = changes_df.astype(str).apply(lambda x: x.str.strip())

    to_delete_indices = []
    to_replace = []
    to_add = []

    matched_log = []  # To store successful matches
    unmatched_log = []  # To store failures

    # 2. Validation & Classification Loop
    for i, row in changes_df_clean.iterrows():
        original_row = changes_df.iloc[i].tolist()  # Keep original formatting
        old_vals = row.iloc[0:5]
        new_vals = row.iloc[6:11]

        is_old_blank = (old_vals == "nan").all() or (old_vals == "").all()
        is_new_blank = (new_vals == "nan").all() or (new_vals == "").all()

        # Operation: ADD
        if is_old_blank and not is_new_blank:
            to_add.append(new_vals.values)
            matched_log.append(original_row + ["Successfully Queued (Add)"])

        # Operation: REPLACE or DELETE
        elif not is_old_blank:
            mask = (master_df_clean.iloc[:, 0:5] == old_vals.values).all(axis=1)
            matches = master_df_clean[mask]

            if len(matches) == 0:
                unmatched_log.append(original_row + ["No Match Found"])
            else:
                idx = matches.index[0]
                if is_new_blank:
                    to_delete_indices.append(idx)
                    matched_log.append(original_row + [f"Matched (Delete) at Master Index {idx}"])
                else:
                    to_replace.append((idx, new_vals.values))
                    matched_log.append(original_row + [f"Matched (Replace) at Master Index {idx}"])

    # 3. Save Reports
    if matched_log:
        pd.DataFrame(matched_log).to_csv(input_path/'matched_changes.csv', index=False, header=False)
        print(f"📝 {len(matched_log)} matches logged in 'matched_changes.csv'.")

    if unmatched_log:
        pd.DataFrame(unmatched_log).to_csv(input_path/'unmatched_changes.csv', index=False, header=False)
        print(f"⚠️ {len(unmatched_log)} rows failed. Details in 'unmatched_changes.csv'.")
        print("Master file update aborted: All rows must match to proceed.")
        return

        # 4. Execution Phase (Only if no unmatched rows)
    print("🚀 All rows verified. Finalizing master file update...")
    updated_df = master_df.copy()

    for idx, vals in to_replace:
        updated_df.iloc[idx] = vals

    updated_df = updated_df.drop(to_delete_indices)

    if to_add:
        add_df = pd.DataFrame(to_add)
        updated_df = pd.concat([updated_df, add_df], ignore_index=True)

    # 5. Sorting
    try:
        # Convert date column for sorting
        updated_df[date_column_index] = pd.to_datetime(updated_df[date_column_index])
        updated_df = updated_df.sort_values(by=date_column_index).reset_index(drop=True)
    except Exception as e:
        print(f"Warning: Sorting failed. Error: {e}")

    # 6. Save Updated Master
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"master_updated_{timestamp}.csv"
    updated_df.to_csv(input_path/new_filename, index=False, header=False)
    print(f"✅ Success! New master file created: {new_filename}")


# Usage:
# Adjust 'date_column_index' to 0, 1, 2, 3, or 4 depending on where the date is.
update_master_file(input_path /master_file, input_path/changes_file, date_column_index=2)