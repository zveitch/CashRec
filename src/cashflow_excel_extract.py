import pandas as pd
from pathlib import Path
from openpyxl import load_workbook
import win32com.client as win32

# Define your paths
# 1. Get the path of the current script (src/<filename>.py)
current_file = Path(__file__).resolve()
src_directory = current_file.parent
project_root = src_directory.parent
data_dir = project_root / "data"
input_folder = ("C:/Users/zachv/OneDrive - wafracapital.com/ICP-UK - Documents/Track Record/Python Output/Q4Update")
input_path = Path(input_folder)

# Default terminal output formatting
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')

def find_Excel_files_in_path(path):
    filename_list = []
    for file_path in path.glob(f"*.xlsx"):
        filename_list.append(file_path.name)
    return filename_list


def get_pivot_as_dataframe(file_path, sheet_name, pivot_name_or_index=1):
    # 1. Connect to Excel
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(file_path)
    ws = wb.Sheets(sheet_name)

    # 2. Access the Pivot Table
    pt = ws.PivotTables(pivot_name_or_index)

    # .TableRange1 includes headers and data (but excludes page filters)
    data = pt.TableRange1.Value

    # 3. Convert to DataFrame
    # The first row of the range is usually the header
    df = pd.DataFrame(list(data[1:]), columns=data[0])

    # 4. Cleaning
    # Drop "Grand Total" Row (usually the last row)
    df = df[df.iloc[:, 0] != "Grand Total"]

    # Drop "Grand Total" Column if it exists
    if "Grand Total" in df.columns:
        df = df.drop(columns=["Grand Total"])

    # Optional: Drop rows where the first column is None (empty space in pivot)
    df = df.dropna(subset=[df.columns[0]])

    wb.Close(False)
    # excel.Quit() # Keep open if you're doing more work, otherwise uncomment

    return df


def extract_dynamic_excel_data(file_path, sheet_name):
    # 1. Connect to Excel
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(file_path)
    ws = wb.Sheets(sheet_name)

    # 2. Get the "UsedRange" (Everything from the first to last cell with data)
    # This is much faster than reading cell-by-cell
    raw_data = ws.UsedRange.Value
    wb.Close(False)

    # 3. Convert to a temporary DataFrame to find our anchor
    df_raw = pd.DataFrame(list(raw_data))

    # 4. Find the row index where columns A:D match your headers
    target_headers = ['Type', 'Date', 'Detail', 'Amount']
    header_row_index = None

    for idx, row in df_raw.iterrows():
        # Clean the row values for comparison (remove whitespace/None)
        current_row_start = [str(val).strip() if val is not None else "" for val in row.values[:4]]

        if current_row_start == target_headers:
            header_row_index = idx
            break

    if header_row_index is None:
        print("Could not find the 'Type, Date, Detail, Amount' header row.")
        return None

    # 5. Extract the data from the header row downwards
    # Slice the dataframe starting at the header row
    df_final = df_raw.iloc[header_row_index:, :4].copy()

    # Set the first row as columns and drop it from the data
    df_final.columns = df_final.iloc[0]
    df_final = df_final[1:]

    # 6. Cleanup: Remove "Grand Total" or empty rows at the bottom
    # We drop rows where 'Type' is empty or contains "Total"
    df_final = df_final[df_final['Type'].notna()]
    df_final = df_final[~df_final['Type'].astype(str).str.contains("Total", case=False)]

    # 1. Convert the 'Date' column to actual datetime objects
    # Ensure the column is treated as a string and take the first 10 characters
    df_final['Date'] = df_final['Date'].astype(str).str[:10]
    # 'errors=coerce' turns unparseable dates into NaT (Not a Time) so the code doesn't crash
    df_final['Date'] = pd.to_datetime(df_final['Date'], errors='coerce')

    # 2. Drop any rows where the date couldn't be parsed (optional but recommended)
    # df_final = df_final.dropna(subset=['Date'])

    return df_final.reset_index(drop=True)


import os
import re
import time
import pandas as pd
import win32com.client as win32
from pywintypes import com_error

def safe_str_date(val):
    """Safely converts Excel date objects to YYYY-MM-DD strings without triggering Pandas errors."""
    if val is None:
        return ""
    # If it's already a string, just take the first 10 chars
    s_val = str(val).strip()
    return s_val[:10]

def process_cash_reports_2(input_path, excel_files, output_csv):
    # Optional: Delete existing file for a fresh run
    if os.path.exists(output_csv):
        os.remove(output_csv)

    excel = win32.gencache.EnsureDispatch('Excel.Application')
    excel.Visible = False  # Keep Excel hidden

    error_log = []
    empty_files = []
    total_processed = 0

    for file_name in excel_files:
        try:
            print(f"Processing: {file_name}")

            # 1. Parse filename for <code> and <name>
            # Format: "<code> Cash Rec <name> 12.31.2025.xlsx"
            # Using regex to capture first word as code and middle part as name
            match = re.search(r'^(\S+)\s+Cash\s+Rec\s+(.*?)\s+\d{1,2}\.\d{1,2}\.\d{4}', file_name)
            if match:
                f_code = match.group(1)
                f_name = match.group(2)
            else:
                f_code, f_name = "Unknown", "Unknown"


            # 2. Open Workbook with Retry Logic for "Busy" Excel
            wb = None
            for i in range(10):  # 10 retries
                try:
                    df_raw = extract_dynamic_excel_data(file_name, 'Cash Breakout')
                    break
                except com_error:
                    print(f"  Excel busy... retry {i + 1}")
                    time.sleep(2)

            if not wb:
                error_log.append(f"{file_name}: Failed to open after retries.")
                continue


            # # 5. Safe Date Processing
            # # Use our custom function instead of direct Pandas conversion to start
            # df_final['Date'] = df_final['Date'].apply(safe_str_date)
            #
            # # Date Filtering
            # # df_final['Date'] = df_final['Date'].astype(str).str[:10]
            # df_final['Date'] = pd.to_datetime(df_final['Date'], errors='coerce')
            # df_filtered = df_final[df_final['Date'] > "2025-09-30"].copy()

            df_filtered = df_raw
            if df_filtered.empty:
                empty_files.append(file_name)
                continue

            # 4. Insert Identifier Columns at the start
            df_filtered.insert(0, 'Name', f_name)
            df_filtered.insert(0, 'Code', f_code)

            # 5. Save/Append to CSV
            # If file doesn't exist, write header. If it does, append without header.
            file_exists = os.path.isfile(output_csv)
            df_filtered.to_csv(output_csv, mode='a', index=False, header=not file_exists)

            total_processed += 1

        except Exception as e:
            error_log.append(f"{file_name}: Unexpected error: {str(e)}")

    # Final Summary
    print("\n" + "=" * 30)
    print(f"Process Complete.")
    print(f"Files Appended: {total_processed}")
    print(f"Empty Files Skipped: {len(empty_files)}")
    print(f"Errors Encountered: {len(error_log)}")

    if error_log:
        print("\nError Details:")
        for err in error_log: print(f" - {err}")

    excel.Quit()

def process_cash_reports(input_path, excel_files, output_csv):
    # Optional: Delete existing file for a fresh run
    if os.path.exists(output_csv):
        os.remove(output_csv)

    excel = win32.gencache.EnsureDispatch('Excel.Application')
    excel.Visible = False  # Keep Excel hidden

    error_log = []
    empty_files = []
    total_processed = 0

    for file_name in excel_files:
        try:
            print(f"Processing: {file_name}")

            # 1. Parse filename for <code> and <name>
            # Format: "<code> Cash Rec <name> 12.31.2025.xlsx"
            # Using regex to capture first word as code and middle part as name
            match = re.search(r'^(\S+)\s+Cash\s+Rec\s+(.*?)\s+\d{1,2}\.\d{1,2}\.\d{4}', file_name)
            if match:
                f_code = match.group(1)
                f_name = match.group(2)
            else:
                f_code, f_name = "Unknown", "Unknown"

            # 2. Open Workbook with Retry Logic for "Busy" Excel
            wb = None
            for i in range(10):  # 10 retries
                try:
                    full_path = str(input_path / file_name)
                    wb = excel.Workbooks.Open(full_path, ReadOnly=True)
                    break
                except com_error:
                    print(f"  Excel busy... retry {i + 1}")
                    time.sleep(2)

            if not wb:
                error_log.append(f"{file_name}: Failed to open after retries.")
                continue

            try:
                ws = wb.Sheets('Cash Breakout')
                # Grab the raw tuple and immediately convert to a list of lists
                # to break the link with Excel/COM/Pandas Datetime types
                raw_data = list(ws.UsedRange.Value)
            except Exception as e:
                error_log.append(f"{file_name}: Sheet error - {e}")
                wb.Close(False)
                continue

            wb.Close(False)

            # 3. Process into DataFrame
            df_raw = pd.DataFrame(list(raw_data)).astype(object)

            # Find the header anchor
            target_headers = ['Type', 'Date', 'Detail', 'Amount']
            header_idx = None
            for i, row in enumerate(raw_data):
                if row and len(row) >= 4:
                    clean_row = [str(x).strip().lower() if x is not None else "" for x in row[:4]]
                    if clean_row == target_headers:
                        header_idx = i
                        break

            if header_idx is None:
                error_log.append(f"{file_name}: Header 'Type, Date...' not found.")
                continue

            # Slice and clean
            # Find the header anchor
            target_headers = ['Type', 'Date', 'Detail', 'Amount']
            header_idx = None
            for idx, row in df_raw.iterrows():
                if [str(v).strip() if v else "" for v in row[:4]] == target_headers:
                    header_idx = idx
                    break

            if header_idx is None:
                error_log.append(f"{file_name}: Header 'Type, Date...' not found.")
                continue

            # Slice and clean
            df_final = df_raw.iloc[header_idx:, :4].copy()
            df_final.columns = df_final.iloc[0]
            df_final = df_final[1:].reset_index(drop=True)

            # # 5. Safe Date Processing
            # # Use our custom function instead of direct Pandas conversion to start
            # df_final['Date'] = df_final['Date'].apply(safe_str_date)
            #
            # # Date Filtering
            # # df_final['Date'] = df_final['Date'].astype(str).str[:10]
            # df_final['Date'] = pd.to_datetime(df_final['Date'], errors='coerce')
            # df_filtered = df_final[df_final['Date'] > "2025-09-30"].copy()

            df_filtered = df_final
            if df_filtered.empty:
                empty_files.append(file_name)
                continue

            # 4. Insert Identifier Columns at the start
            df_filtered.insert(0, 'Name', f_name)
            df_filtered.insert(0, 'Code', f_code)

            # 5. Save/Append to CSV
            # If file doesn't exist, write header. If it does, append without header.
            file_exists = os.path.isfile(output_csv)
            df_filtered.to_csv(output_csv, mode='a', index=False, header=not file_exists)

            total_processed += 1

        except Exception as e:
            error_log.append(f"{file_name}: Unexpected error: {str(e)}")

    # Final Summary
    print("\n" + "=" * 30)
    print(f"Process Complete.")
    print(f"Files Appended: {total_processed}")
    print(f"Empty Files Skipped: {len(empty_files)}")
    print(f"Errors Encountered: {len(error_log)}")

    if error_log:
        print("\nError Details:")
        for err in error_log: print(f" - {err}")

    excel.Quit()



# Usage:
# process_cash_reports(input_path, Excel_files, 'Consolidated_Cash_Flow.csv')

## usage
Excel_files = find_Excel_files_in_path(input_path)
#process_cash_reports_2(input_path, Excel_files, 'Consolidated_Cash_Flow.csv')
# Load the Excel file
# df = pd.read_excel(input_path / Excel_files[0], sheet_name='Cash Breakout')
for excel_file in Excel_files:
    for i in range(2):  # 10 retries
        try:
            df_clean = extract_dynamic_excel_data(input_path / excel_file,'Cash Breakout')
            break
        except com_error:
            print(f"  Excel busy... retry {i + 1}")
            time.sleep(2)


    match = re.search(r'^(\S+)\s+Cash\s+Rec\s+(.*?)\s+\d{1,2}\.\d{1,2}\.\d{4}', excel_file)
    if match:
        f_code = match.group(1)
        f_name = match.group(2)
    else:
        f_code, f_name = "Unknown", "Unknown"

    # 3. Filter for dates strictly after 2025-09-30
    cutoff_date = "2025-09-30"
    df_filtered = df_clean[df_clean['Date'] > cutoff_date]

    # 4. Insert Identifier Columns at the start
    df_filtered.insert(0, 'Name', f_name)
    df_filtered.insert(0, 'Code', f_code)

    # 5. Save/Append to CSV
    # If file doesn't exist, write header. If it does, append without header.
    output_csv = 'Consolidated_Cash_Flow.csv'
    file_exists = os.path.isfile(output_csv)
    df_filtered.to_csv(output_csv, mode='a', index=False, header=not file_exists)

# 4. (Optional) Format the date back to a clean string for printing/CSV
#df_filtered['Date'] = df_filtered['Date'].dt.strftime('%Y-%m-%d')

#print(f"Rows after filtering: {len(df_filtered)}")
#print(df_filtered)

