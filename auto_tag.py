import pandas as pd
import glob
import re
import os
import numpy as np
from pathlib import Path
from rapidfuzz import process, fuzz

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

# --- CONFIGURATION ---
account_number = "400310050003"
INPUT_FILES_PATTERN = "cashrec_report_*.csv"
MASTER_DICT_PATH = data_dir / "MASTER -SPV-to-Originator dictionary 2026-02-01.csv"
#OUTPUT_FILE = "tagged_transactions_report.csv"
OUTPUT_FILE = os.path.join(data_dir, "tagged_transactions_tfidf.csv")

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def get_best_match(text, choices):
    """Returns the best match and a confidence score."""
    if pd.isna(text) or str(text).strip() == "":
        return None, 0

    # We use partial_ratio because the SPV name is often embedded in a longer string
    result = process.extractOne(
        str(text),
        choices,
        scorer=fuzz.partial_ratio,
        score_cutoff=60  # Ignore very weak matches
    )

    if result:
        return result[0], result[1]
    return None, 0


def get_tfidf_matches(source_strings, target_choices):
    """
    Uses TF-IDF and Cosine Similarity to find the best match for each string.
    """
    if not source_strings or not target_choices:
        return []

    # Initialize Vectorizer - we use char_wb analyzer to handle typos/small variations
    vectorizer = TfidfVectorizer(analyzer='char_wb', ngram_range=(2, 4))

    # Fit on the Master Dictionary and transform both
    tfidf_matrix_targets = vectorizer.fit_transform(target_choices)
    tfidf_matrix_source = vectorizer.transform(source_strings)

    # Calculate similarity matrix
    # Row = Source Text, Column = Master Dictionary Entry
    similarities = cosine_similarity(tfidf_matrix_source, tfidf_matrix_targets)

    results = []
    for row in similarities:
        best_idx = np.argmax(row)
        score = row[best_idx] * 100  # Convert to 0-100 scale
        results.append((target_choices[best_idx], score))

    return results

def run_tagging_engine_rapidfuzz():
    # 1. Load Master Dictionary
    print("Loading master dictionary...")
    master_df = pd.read_csv(MASTER_DICT_PATH)
    spv_list = master_df['SPV NAME'].dropna().unique().tolist()
    spv_to_co_map = master_df.set_index('SPV NAME')['ORIGINATOR NAME'].to_dict()

    # 2. Load and Combine Transaction CSVs
    print(f"Searching for files matching: {INPUT_FILES_PATTERN}")
    search_path = os.path.join(data_dir, INPUT_FILES_PATTERN)
    files = glob.glob(search_path)
    if not files:
        print("No files found! Check your INPUT_FILES_PATH.")
        return

    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

    # 3. Filter for Target Types
    target_types = ["Rent", "Investment", "Repayment"]
    df = df[df['Type'].isin(target_types)].copy()

    # 4. Extract Match Index from 'Reconciled' (e.g., "SPLIT MATCH - 25" -> "25")
    df['match_id'] = df['Reconciled'].str.extract(r'- (\d+)')

    # 5. Perform Fuzzy Matching on 'Detail' column
    print("Analyzing 'Detail' column for SPV matches...")
    matches = df['Detail'].apply(lambda x: get_best_match(x, spv_list))
    df[['Matched_SPV', 'Conf_Score']] = pd.DataFrame(matches.tolist(), index=df.index)

    # 6. Secondary Check: Search 'Description 1B' for rows with low confidence
    low_conf_mask = df['Conf_Score'] < 90
    print("Performing secondary check on low-confidence rows...")

    desc_matches = df.loc[low_conf_mask, 'Description2'].apply(lambda x: get_best_match(x, spv_list))

    # Update only if the Description check yields a higher score
    for idx, (m_spv, m_score) in desc_matches.items():
        if m_score > df.at[idx, 'Conf_Score']:
            df.at[idx, 'Matched_SPV'] = m_spv
            df.at[idx, 'Conf_Score'] = m_score

    # 7. Match Index Consensus (The "Fill-in" Logic)
    # If one part of a split match (match_id) is identified, apply it to the siblings
    print("Applying Match Index consensus...")
    df = df.sort_values(['match_id', 'Conf_Score'], ascending=[True, False])
    df['Matched_SPV'] = df.groupby('match_id')['Matched_SPV'].ffill().bfill()

    # 8. Map back to Company
    df['Matched_Company'] = df['Matched_SPV'].map(spv_to_co_map)

    # 9. Final Cleanup & Export
    # Create a 'Review Status' column for easy filtering in Excel/CSV
    df['Status'] = 'Auto-Approved'
    df.loc[df['Conf_Score'] < 85, 'Status'] = 'Needs Review'
    df.loc[df['Matched_SPV'].isna(), 'Status'] = 'No Match Found'

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Process complete! Output saved to: {OUTPUT_FILE}")


def run_tagging_engine():
    # 1. Load Master Dictionary
    master_df = pd.read_csv(MASTER_DICT_PATH)
    spv_list = master_df['SPV NAME'].dropna().unique().tolist()
    spv_to_co_map = master_df.set_index('SPV NAME')['ORIGINATOR NAME'].to_dict()

    # 2. Load & Filter Transaction CSVs
    files = glob.glob(os.path.join(data_dir, INPUT_FILES_PATTERN))
    if not files: return print("No files found.")

    all_dfs = []
    for f in files:
        temp_df = pd.read_csv(f)
        temp_df['Source_Account'] = os.path.basename(f).replace("cashrec_report_", "").replace(".csv", "")
        all_dfs.append(temp_df)

    df = pd.concat(all_dfs, ignore_index=True)
    target_types = ["Rent", "Investment", "Repayment"]
    df = df[df['Type'].isin(target_types)].copy()
    df['match_id'] = df['Reconciled'].str.extract(r'- (\d+)')

    # 3. TF-IDF Matching
    print("Running TF-IDF Matching Engine...")
    # Combine Detail and Descriptions for a richer search string
    df['search_text'] = df['Detail'].fillna('') + " " + df['Description1B'].fillna('')

    # Run the math
    match_results = get_tfidf_matches(df['search_text'].tolist(), spv_list)

    # 4. Extract results
    df['Matched_SPV'] = [res[0] for res in match_results]
    df['Conf_Score'] = [res[1] for res in match_results]

    # 5. Logical Cleanups
    # If the score is too low, it's likely a random match
    df.loc[df['Conf_Score'] < 30, ['Matched_SPV', 'Conf_Score']] = [None, 0]

    # Apply Match Index consensus (propagate best tag in a group)
    df = df.sort_values(['match_id', 'Conf_Score'], ascending=[True, False])
    df['Matched_SPV'] = df.groupby('match_id')['Matched_SPV'].ffill().bfill()

    # Map to Company
    df['Matched_Company'] = df['Matched_SPV'].map(spv_to_co_map)

    # 6. Status and Export
    df['Status'] = 'Auto-Approved'
    df.loc[df['Conf_Score'] < 75, 'Status'] = 'Needs Review'
    df.loc[df['Matched_SPV'].isna(), 'Status'] = 'No Match Found'

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Success! Processed {len(df)} rows. Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    run_tagging_engine()