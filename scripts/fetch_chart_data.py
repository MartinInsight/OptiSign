import gspread
import json
import os
import pandas as pd
import traceback
import re

# --- Configuration ---
# Get credentials and spreadsheet ID from GitHub Secrets environment variables
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDENTIAL_JSON = os.environ.get("GOOGLE_CREDENTIAL_JSON")

# --- Debugging Prints ---
print(f"DEBUG: SPREADSHEET_ID from environment: {SPREADSHEET_ID}")
print(f"DEBUG: GOOGLE_CREDENTIAL_JSON from environment (first 50 chars): {GOOGLE_CREDENTIAL_JSON[:50] if GOOGLE_CREDENTIAL_JSON else 'None'}")
# --- End Debugging Prints ---

# Name of the worksheet containing the data
WORKSHEET_NAME = "Crawling_Data"
# Path to save the processed JSON file (accessible by GitHub Pages)
OUTPUT_JSON_PATH = "data/crawling_data.json"

# --- Header Mapping Definitions ---
# These are for standard columnar data. IACI will be handled separately.
SECTION_MARKER_SEQUENCE = [
    ("종합지수(Point)와 그 외 항로별($/FEU)", "KCCI"),
    ("종합지수($/TEU), 미주항로별($/FEU), 그 외 항로별($/TEU)", "SCFI"),
    ("종합지수와 각 항로별($/FEU)", "WCI"),
    # "IACIdate" is now handled by direct row/column search
    ("Index", "BLANK_SAILING"),
    ("종합지수와 각 항로별($/FEU)", "FBX"),
    ("각 항로별($/FEU)", "XSI"),
    ("Index(종합지수), $/day(정기용선, Time charter)", "MBCI")
]

COMMON_DATA_HEADERS_TO_PREFIX = {
    "종합지수": "Composite_Index",
    "미주서안": "US_West_Coast",
    "미주동안": "US_East_Coast",
    "유럽": "Europe",
    "지중해": "Mediterranean",
    "중동": "Middle_East",
    "호주": "Australia",
    "남미동안": "South_America_East_Coast",
    "남미서안": "South_America_West_Coast",
    "남아프리카": "South_Africa",
    "서아프리카": "West_Africa",
    "중국": "China",
    "일본": "Japan",
    "동남아시아": "Southeast_Asia",
    "북유럽": "North_Europe",
    "미주동안 → 북유럽": "US_East_Coast_North_Europe",
    "북유럽 → 미주동안": "North_Europe_US_East_Coast",
    "유럽 → 남미동안": "Europe_South_America_East_Coast",
    "유럽 → 남미서안": "Europe_South_America_West_Coast",
    "동아시아 → 북유럽": "East_Asia_North_Europe",
    "북유럽 → 동아시아": "North_Europe_East_Asia",
    "동아시아 → 미주서안": "East_Asia_US_West_Coast",
    "미주서안 → 동아시아": "US_West_Coast_East_Asia",
    "동아시아 → 남미동안": "East_Asia_South_America_East_Coast",
    "북유럽 → 남미동안": "North_Europe_South_America_East_Coast",
    "MBCI": "MBCI_Value"
}

SPECIFIC_RENAMES = {
    "호주/뉴질랜드": "Australia_New_Zealand_SCFI",
    "남아메리카": "South_America_SCFI",
    "일본서안": "Japan_West_Coast_SCFI",
    "일본동안": "Japan_East_Coast_SCFI",
    "한국": "Korea_SCFI",
    "동부/서부 아프리카": "East_West_Africa_SCFI",
    "남아공": "South_Africa_SCFI",
    "상하이 → 로테르담": "Shanghai_Rotterdam_WCI",
    "로테르담 → 상하이": "Rotterdam_Shanghai_WCI",
    "상하이 → 제노바": "Shanghai_Genoa_WCI",
    "상하이 → 로스엔젤레스": "Shanghai_Los_Angeles_WCI",
    "로스엔젤레스 → 상하이": "Los_Angeles_Shanghai_WCI",
    "상하이 → 뉴욕": "Shanghai_New_York_WCI",
    "뉴욕 → 로테르담": "New_York_Rotterdam_WCI",
    "로테르담 → 뉴욕": "Rotterdam_New_York_WCI",
    "Gemini Cooperation": "Gemini_Cooperation_Blank_Sailing",
    "MSC": "MSC_Alliance_Blank_Sailing",
    "OCEAN Alliance": "OCEAN_Alliance_Blank_Sailing",
    "Premier Alliance": "Premier_Alliance_Blank_Sailing",
    "Others/Independent": "Others_Independent_Blank_Sailing",
    "Total": "Total_Blank_Sailings",
    "중국/동아시아 → 미주서안": "China_EA_US_West_Coast_FBX",
    "미주서안 → 중국/동아시아": "US_West_Coast_China_EA_FBX",
    "중국/동아시아 → 미주동안": "China_EA_US_East_Coast_FBX",
    "미주동안 → 중국/동아시아": "US_East_Coast_China_EA_FBX",
    "중국/동아시아 → 북유럽": "China_EA_North_Europe_FBX",
    "북유럽 → 중국/동아시아": "North_Europe_China_EA_FBX",
    "중국/동아시아 → 지중해": "China_EA_Mediterranean_FBX",
    "지중해 → 중국/동아시아": "Mediterranean_China_EA_FBX",
}


def fetch_and_process_data():
    """
    Fetches data from Google Sheet, processes it, and saves it as a JSON file.
    Handles both columnar data and special transposed IACI data.
    """
    if not SPREADSHEET_ID or not GOOGLE_CREDENTIAL_JSON:
        print("Error: SPREADSHEET_ID or GOOGLE_CREDENTIAL_JSON environment variables are not set.")
        if not SPREADSHEET_ID:
            print("Reason: SPREADSHEET_ID is None.")
        if not GOOGLE_CREDENTIAL_JSON:
            print("Reason: GOOGLE_CREDENTIAL_JSON is None.")
        return

    try:
        credentials_dict = json.loads(GOOGLE_CREDENTIAL_JSON)
        gc = gspread.service_account_from_dict(credentials_dict)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        all_data = worksheet.get_all_values()

        if not all_data:
            print("Error: No data fetched from the sheet.")
            return

        main_header_row_index = -1
        iaci_date_col_index = -1
        iaci_value_col_index = -1
        
        # Find relevant row and column indices
        for i, row in enumerate(all_data):
            # Find the main header row by looking for "date" (case-insensitive, trimmed)
            if any(cell.strip().lower() == "date" for cell in row):
                main_header_row_index = i
            
            # Search for IACI headers in the first row (index 0)
            if i == 0: # Assuming IACI headers are in the very first row
                print(f"DEBUG: Contents of the first row (index 0): {row}") # NEW DEBUG: Print entire first row
                for col_idx, cell_value in enumerate(row):
                    if cell_value.strip() == "IACIdate":
                        iaci_date_col_index = col_idx
                    elif cell_value.strip() == "종합지수": # This might be the IACI value header
                        iaci_value_col_index = col_idx

        print(f"DEBUG: Main header row index: {main_header_row_index}")
        print(f"DEBUG: IACI date column index (from row 0): {iaci_date_col_index}")
        print(f"DEBUG: IACI value column index (from row 0): {iaci_value_col_index}")


        if main_header_row_index == -1:
            print("Error: Could not find the main header row containing 'date'.")
            return

        # --- Process Main Columnar Data ---
        # Get the actual header row for columnar data
        raw_headers_original = [h.strip().replace('"', '') for h in all_data[main_header_row_index]]
        
        final_column_names = []
        current_section_prefix = ""
        empty_col_counter = 0
        seen_final_names_set = set()

        for h_orig in raw_headers_original:
            cleaned_h_orig = h_orig.strip().replace('"', '')
            final_name_candidate = cleaned_h_orig

            found_section_marker_in_sequence = False
            for i in range(len(SECTION_MARKER_SEQUENCE)): # Iterate from start for each header
                marker_string, marker_prefix_base = SECTION_MARKER_SEQUENCE[i]
                if cleaned_h_orig == marker_string:
                    current_section_prefix = f"{marker_prefix_base}_"
                    if marker_prefix_base == "BLANK_SAILING":
                        final_name_candidate = "Date_Blank_Sailing" # This is a date column for Blank Sailing
                        current_section_prefix = "" # No prefix for subsequent columns in this section
                    else:
                        final_name_candidate = f"{marker_prefix_base}_Container_Index" 
                    found_section_marker_in_sequence = True
                    break
            
            if found_section_marker_in_sequence:
                pass
            elif cleaned_h_orig in SPECIFIC_RENAMES:
                final_name_candidate = SPECIFIC_RENAMES[cleaned_h_orig]
            elif cleaned_h_orig in COMMON_DATA_HEADERS_TO_PREFIX:
                base_name = COMMON_DATA_HEADERS_TO_PREFIX[cleaned_h_orig]
                if current_section_prefix:
                    final_name_candidate = f"{current_section_prefix}{base_name}"
                else:
                    final_name_candidate = base_name
            elif cleaned_h_orig == 'date':
                final_name_candidate = 'date'
            elif cleaned_h_orig == '':
                final_name_candidate = f'_EMPTY_COL_{empty_col_counter}'
                empty_col_counter += 1
            else:
                final_name_candidate = cleaned_h_orig
            
            final_unique_name = final_name_candidate
            suffix = 0
            while final_unique_name in seen_final_names_set:
                suffix += 1
                final_unique_name = f"{final_name_candidate}_{suffix}"
            
            seen_final_names_set.add(final_unique_name)
            final_column_names.append(final_unique_name)

        print(f"DEBUG: Main DataFrame column names: {final_column_names}")

        # Filter out rows that are part of the IACI data block from the main data_rows
        # This assumes IACI data is in a separate section of the sheet and not overlapping,
        # we don't need to exclude rows from the main data. This exclusion logic might be removed
        # if IACI is truly in its own dedicated columns. For now, keeping it general.
        rows_to_exclude_from_main = set()
        # If IACI data is in its own columns and not rows, this exclusion is not needed.
        # Removing this for now, as it was based on a transposed IACI assumption.
        # if iaci_date_row_index != -1:
        #     rows_to_exclude_from_main.add(iaci_date_row_index)
        # if iaci_value_row_index != -1:
        #     rows_to_exclude_from_main.add(iaci_value_row_index)

        main_data_rows = []
        # Start from the row *after* the main header row
        for i, row in enumerate(all_data[main_header_row_index + 1:]):
            # original_row_index = i + main_header_row_index + 1
            # if original_row_index not in rows_to_exclude_from_main:
            main_data_rows.append(row)
        
        # Adjust for potential length mismatches for main data
        processed_main_data_rows = []
        num_expected_main_cols = len(final_column_names)
        for i, row in enumerate(main_data_rows):
            cleaned_row = [str(cell) if cell is not None else '' for cell in row]
            if len(cleaned_row) < num_expected_main_cols:
                padded_row = cleaned_row + [''] * (num_expected_main_cols - len(cleaned_row))
                processed_main_data_rows.append(padded_row)
            elif len(cleaned_row) > num_expected_main_cols:
                truncated_row = cleaned_row[:num_expected_main_cols]
                processed_main_data_rows.append(truncated_row)
            else:
                processed_main_data_rows.append(cleaned_row)

        df_main = pd.DataFrame(processed_main_data_rows, columns=final_column_names)
        
        # --- Process IACI Data from its specific columns ---
        df_iaci = pd.DataFrame()
        if iaci_date_col_index != -1 and iaci_value_col_index != -1:
            iaci_data_list = []
            # Start extracting data from the row *after* the IACI headers (which is row 1, index 1)
            # and go up to row 24 as per user's info (index 23)
            # Assuming IACI data starts from row 1 (index 1) and goes to row 24 (index 23)
            # And headers are in row 0 (index 0)
            for row_idx in range(1, len(all_data)): # Iterate through all data rows after header row 0
                row_data = all_data[row_idx]
                if len(row_data) > max(iaci_date_col_index, iaci_value_col_index):
                    date_val = row_data[iaci_date_col_index]
                    iaci_val = row_data[iaci_value_col_index]
                    
                    if date_val and iaci_val: # Only add if both date and value are present
                        iaci_data_list.append({
                            'date': date_val,
                            'IACI_Composite_Index': iaci_val
                        })
            
            df_iaci = pd.DataFrame(iaci_data_list)
            
            # Convert IACI dates and values to proper types
            df_iaci['date'] = pd.to_datetime(df_iaci['date'], errors='coerce')
            df_iaci['IACI_Composite_Index'] = pd.to_numeric(df_iaci['IACI_Composite_Index'].astype(str).str.replace(',', ''), errors='coerce')
            
            print(f"DEBUG: IACI DataFrame created:\n{df_iaci.to_string()}")
            print(f"DEBUG: Raw IACI Dates extracted (first 5): {iaci_data_list[:5]}") # NEW DEBUG
            print(f"DEBUG: Raw IACI Values extracted (first 5): {iaci_data_list[:5]}") # NEW DEBUG


        # --- Date column processing for main DataFrame ---
        df_main['date'] = pd.to_datetime(df_main['date'], errors='coerce')
        
        # --- Merge DataFrames ---
        # Use an outer merge to keep all dates from both dataframes
        # Prioritize IACI_Composite_Index from df_iaci if it exists
        df_final = pd.merge(df_main, df_iaci[['date', 'IACI_Composite_Index']], on='date', how='outer', suffixes=('_main', '_iaci'))

        # Combine IACI_Composite_Index columns, prioritizing the one from df_iaci
        if 'IACI_Composite_Index_iaci' in df_final.columns:
            df_final['IACI_Composite_Index'] = df_final['IACI_Composite_Index_iaci'].fillna(df_final.get('IACI_Composite_Index_main'))
            df_final.drop(columns=['IACI_Composite_Index_main', 'IACI_Composite_Index_iaci'], inplace=True, errors='ignore')
        elif 'IACI_Composite_Index_main' in df_final.columns:
             df_final.rename(columns={'IACI_Composite_Index_main': 'IACI_Composite_Index'}, inplace=True)
        # Ensure IACI_Composite_Index exists even if no IACI data was found at all
        if 'IACI_Composite_Index' not in df_final.columns:
            df_final['IACI_Composite_Index'] = None


        # Clean up other temporary date columns if they were created and not used as primary 'date'
        temp_date_cols = ['IACI_Date_Column', 'Date_Blank_Sailing', '_EMPTY_COL_0']
        for col in temp_date_cols:
            if col in df_final.columns:
                df_final.drop(columns=[col], inplace=True, errors='ignore') # Use errors='ignore' to prevent error if col doesn't exist


        df_final.dropna(subset=['date'], inplace=True)
        df_final = df_final.sort_values(by='date', ascending=True)
        df_final['date'] = df_final['date'].dt.strftime('%Y-%m-%d')

        if df_final['date'].empty:
            print("Warning: After all date processing, the 'date' column is empty. Charts might not display correctly.")

        # --- Convert all numeric columns ---
        numeric_cols = [col for col in df_final.columns if col != 'date']
        
        # Convert to numeric, handling commas and coercing errors
        for col in numeric_cols:
            df_final[col] = pd.to_numeric(df_final[col].astype(str).str.replace(',', ''), errors='coerce')
        
        # Convert NaN to None for JSON serialization
        df_final = df_final.replace({pd.NA: None, float('nan'): None})

        processed_data = df_final.to_dict(orient='records')

        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)

        print(f"Data successfully saved to '{OUTPUT_JSON_PATH}'.")
        print(f"Sample of saved data (first 3 entries): {processed_data[:3]}")

    except Exception as e:
        print(f"Error during data processing: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_process_data()
