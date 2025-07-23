import gspread
import json
import os
import pandas as pd
import traceback # Import traceback for full error details

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

# --- Header Mapping (Moved to Global Scope) ---
# This dictionary maps original (complex/Korean) headers to cleaned (English-like) headers.
# It is crucial for consistent data access in JavaScript.
# Please ensure these original headers exactly match your Google Sheet's header row.
# Added specific suffixes (_KCCI, _SCFI, _WCI, etc.) to ensure uniqueness for repeated names.
# Also added mapping for section headers to ensure they are handled.
header_mapping = {
    # KCCI Chart Headers (Chart 1) - Section 1
    "KCCI종합지수(Point)와 그 외 항로별($/FEU)": "KCCI_Section_Header", # This is a section header, not a data column
    "종합지수": "KCCI_Composite_Index", # First occurrence of 종합지수
    "미주서안": "US_West_Coast_KCCI",
    "미주동안": "US_East_Coast_KCCI",
    "유럽": "Europe_KCCI",
    "지중해": "Mediterranean_KCCI",
    "중동": "Middle_East_KCCI",
    "호주": "Australia_KCCI",
    "남미동안": "South_America_East_Coast_KCCI",
    "남미서안": "South_America_West_Coast_KCCI",
    "남아프리카": "South_Africa_KCCI",
    "서아프리카": "West_Africa_KCCI",
    "중국": "China_KCCI",
    "일본": "Japan_KCCI",
    "동남아시아": "Southeast_Asia_KCCI",

    # SCFI Chart Headers (Chart 2) - Section 2
    "SCFI종합지수($/TEU), 미주항로별($/FEU), 그 외 항로별($/TEU)": "SCFI_Section_Header", # This is a section header, not a data column
    # These are the actual data columns for SCFI. Renamed with _SCFI suffix.
    "종합지수_SCFI": "SCFI_Composite_Index", # This will be handled by the unique name generation
    "미주서안_SCFI": "US_West_Coast_SCFI",
    "미주동안_SCFI": "US_East_Coast_SCFI",
    "북유럽_SCFI": "North_Europe_SCFI",
    "지중해_SCFI": "Mediterranean_SCFI",
    "동남아시아_SCFI": "Southeast_Asia_SCFI",
    "중동_SCFI": "Middle_East_SCFI",
    "호주/뉴질랜드": "Australia_New_Zealand_SCFI",
    "남아메리카": "South_America_SCFI",
    "일본서안": "Japan_West_Coast_SCFI",
    "일본동안": "Japan_East_Coast_SCFI",
    "한국": "Korea_SCFI",
    "동부/서부 아프리카": "East_West_Africa_SCFI",
    "남아공": "South_Africa_SCFI",

    # WCI Chart Headers (Chart 3) - Section 3
    "WCI종합지수와 각 항로별($/FEU)": "WCI_Section_Header", # This is a section header, not a data column
    "WCI종합지수": "WCI_Composite_Index",
    "상하이 → 로테르담": "Shanghai_Rotterdam",
    "로테르담 → 상하이": "Rotterdam_Shanghai",
    "상하이 → 제노바": "Shanghai_Genoa",
    "상하이 → 로스엔젤레스": "Shanghai_Los_Angeles",
    "로스엔젤레스 → 상하이": "Los_Angeles_Shanghai",
    "상하이 → 뉴욕": "Shanghai_New_York",
    "뉴욕 → 로테르담": "New_York_Rotterdam",
    "로테르담 → 뉴욕": "Rotterdam_New_York",

    # IACI Chart Headers (Chart 4) - Section 4
    "IACIdate종합지수": "IACI_Section_Header", # This is a section header, not a data column
    # 'date' column is handled generally.
    # '종합지수' here is the third occurrence.

    # BLANK_SAILING Chart Headers (Chart 5) - Section 5
    "Index": "Date_Blank_Sailing", # This seems to be the date column for Blank Sailing
    "Gemini Cooperation": "Gemini_Cooperation",
    "MSC": "MSC_Blank_Sailing", # Explicitly named to avoid conflict
    "OCEAN Alliance": "OCEAN_Alliance",
    "Premier Alliance": "Premier_Alliance",
    "Others/Independent": "Others_Independent",
    "Total": "Total_Blank_Sailings",

    # FBX Chart Headers (Chart 6) - Section 6
    "FBX종합지수와 각 항로별($/FEU)": "FBX_Section_Header", # This is a section header, not a data column
    "FBX종합지수": "FBX_Composite_Index",
    "중국/동아시아 → 미주서안": "China_EA_US_West_Coast",
    "미주서안 → 중국/동아시아": "US_West_Coast_China_EA",
    "중국/동아시아 → 미주동안": "China_EA_US_East_Coast",
    "미주동안 → 중국/동아시아": "US_East_Coast_China_EA",
    "중국/동아시아 → 북유럽": "China_EA_North_Europe",
    "북유럽 → 중국/동아시아": "North_Europe_China_EA",
    "중국/동아시아 → 지중해": "China_EA_Mediterranean",
    "지중해 → 중국/동아시아": "Mediterranean_China_EA",
    "미주동안 → 북유럽": "US_East_Coast_North_Europe_FBX", # Differentiate from XSI
    "북유럽 → 미주동안": "North_Europe_US_East_Coast_FBX", # Differentiate from XSI
    "유럽 → 남미동안": "Europe_South_America_East_Coast_FBX", # Differentiate from XSI
    "유럽 → 남미서안": "Europe_South_America_West_Coast_FBX", # Differentiate from XSI

    # XSI Chart Headers (Chart 7) - Section 7
    "각 항로별($/FEU)": "XSI_Section_Header", # This is a section header, not a data column
    "동아시아 → 북유럽": "East_Asia_North_Europe_XSI",
    "북유럽 → 동아시아": "North_Europe_East_Asia_XSI",
    "동아시아 → 미주서안": "East_Asia_US_West_Coast_XSI",
    "미주서안 → 동아시아": "US_West_Coast_East_Asia_XSI",
    "동아시아 → 남미동안": "East_Asia_South_America_East_Coast_XSI",
    "북유럽 → 미주동안": "North_Europe_US_East_Coast_XSI",
    "미주동안 → 북유럽": "US_East_Coast_North_Europe_XSI",
    "북유럽 → 남미동안": "North_Europe_South_America_East_Coast_XSI",

    # MBCI Chart Headers (Chart 8) - Section 8
    "Index(종합지수), $/day(정기용선, Time charter)MBCI": "MBCI_Index"
}


def fetch_and_process_data():
    """
    Fetches data from Google Sheet, processes it, and saves it as a JSON file.
    """
    if not SPREADSHEET_ID or not GOOGLE_CREDENTIAL_JSON:
        print("Error: SPREADSHEET_ID or GOOGLE_CREDENTIAL_JSON environment variables are not set.")
        if not SPREADSHEET_ID:
            print("Reason: SPREADSHEET_ID is None.")
        if not GOOGLE_CREDENTIAL_JSON:
            print("Reason: GOOGLE_CREDENTIAL_JSON is None.")
        return

    try:
        # 1. Google Sheets 인증
        credentials_dict = json.loads(GOOGLE_CREDENTIAL_JSON)
        gc = gspread.service_account_from_dict(credentials_dict)

        # 2. 스프레드시트 및 워크시트 열기
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

        # 3. 모든 데이터 가져오기
        all_data = worksheet.get_all_values()
        if not all_data:
            print("Error: No data fetched from the sheet.")
            return

        # 4. 데이터 파싱 및 처리
        header_row_index = -1
        for i, row in enumerate(all_data):
            # Find the header row by looking for "date" (case-insensitive, trimmed)
            if any(cell.strip().lower() == "date" for cell in row):
                header_row_index = i
                break

        if header_row_index == -1:
            print("Error: Could not find the header row containing 'date'.")
            return

        raw_headers_original = [h.strip().replace('"', '') for h in all_data[header_row_index]]
        
        # Replace empty header strings with unique placeholders and ensure all are unique
        raw_headers = []
        empty_col_counter = 0
        # Use a dictionary to track mapped names to ensure uniqueness
        # Key: mapped_name, Value: count of occurrences
        mapped_name_counts = {} 
        
        for h_orig in raw_headers_original:
            # First, try to get the mapped name from header_mapping, otherwise use original
            mapped_h = header_mapping.get(h_orig, h_orig)
            
            # If the mapped name is empty, use a generic placeholder
            if mapped_h == '':
                mapped_h = f'_EMPTY_COL_{empty_col_counter}'
                empty_col_counter += 1
            
            # Ensure uniqueness for the mapped name by appending a counter if name already seen
            base_name = mapped_h
            if base_name in mapped_name_counts:
                mapped_name_counts[base_name] += 1
                mapped_h = f"{base_name}_{mapped_name_counts[base_name]}"
            else:
                mapped_name_counts[base_name] = 0 # Start count at 0 for first occurrence
                
            raw_headers.append(mapped_h)

        data_rows_raw = all_data[header_row_index + 1:]
        
        # Ensure all data rows have the same number of columns as headers
        num_expected_cols = len(raw_headers)
        data_rows = []
        for i, row in enumerate(data_rows_raw):
            # Explicitly convert all cells to string to avoid potential type issues
            cleaned_row = [str(cell) if cell is not None else '' for cell in row]
            
            if len(cleaned_row) < num_expected_cols:
                padded_row = cleaned_row + [''] * (num_expected_cols - len(cleaned_row)) # Pad with empty strings
                data_rows.append(padded_row)
                print(f"WARNING: Row {i+header_row_index+2} was shorter ({len(cleaned_row)} cols). Padded to {num_expected_cols} cols.")
            elif len(cleaned_row) > num_expected_cols:
                truncated_row = cleaned_row[:num_expected_cols]
                data_rows.append(truncated_row)
                print(f"WARNING: Row {i+header_row_index+2} was longer ({len(cleaned_row)} cols). Truncated to {num_expected_cols} cols.")
            else:
                data_rows.append(cleaned_row) # Use the cleaned_row

        # --- Additional Debugging Prints for DataFrame creation ---
        print(f"DEBUG: Total rows fetched (all_data): {len(all_data)}")
        print(f"DEBUG: Header row index: {header_row_index}")
        print(f"DEBUG: Raw headers (original from sheet): {raw_headers_original}")
        print(f"DEBUG: Raw headers (used for DataFrame - unique): {raw_headers}") # Updated print
        print(f"DEBUG: Number of raw headers (used for DataFrame): {len(raw_headers)}")
        print(f"DEBUG: Number of data rows (after processing): {len(data_rows)}")
        if len(data_rows) > 0:
            print(f"DEBUG: First processed data row: {data_rows[0]}")
            print(f"DEBUG: Number of columns in first processed data row: {len(data_rows[0])}")
            # This warning should now ideally not appear if padding/truncating works
            if len(raw_headers) != len(data_rows[0]):
                print("WARNING: Number of headers (used for DataFrame) does NOT match number of columns in the first data row!")
        # --- End Additional Debugging Prints ---

        df = pd.DataFrame(data_rows, columns=raw_headers)
        # The rename step is now less critical for uniqueness but still useful for consistent API names
        # We will iterate through the header_mapping and apply renames where the target name is not already used
        # This prevents overwriting columns that already got a unique name from the raw_headers processing.
        rename_map_for_df = {}
        for k, v in header_mapping.items():
            if k in df.columns and v not in df.columns: # Only rename if original exists and new name is not already a column
                rename_map_for_df[k] = v
            # Special handling for cases where the original header might have been made unique by the earlier logic
            # e.g., '종합지수_1' should still map to 'SCFI_Composite_Index' if that's its intended role.
            # This requires more complex logic to map based on position or original intent.
            # For now, rely on the raw_headers unique generation and the simple rename.
            # If a header like '종합지수' appears multiple times in raw_headers, it will become '종합지수', '종합지수_1', '종합지수_2'.
            # The header_mapping should then map these specific unique names.
            # Let's adjust header_mapping to use the *expected unique names* from the raw_headers processing.
            # This is tricky without knowing the exact order and content of all duplicate headers.
            # A simpler approach is to map the *original* headers, and let the unique naming logic handle duplicates.
            # The current rename will only work for the first instance of a duplicate original header.
            # The `UserWarning` suggests this is still an issue.

        # Let's re-think the rename. The `raw_headers` list *already contains unique names*.
        # So, we should *not* use df.rename based on header_mapping directly if header_mapping has non-unique keys.
        # Instead, we should apply the mapping during the creation of `raw_headers` itself,
        # and then ensure uniqueness.

        # Re-doing the raw_headers creation and mapping logic to be more robust.
        final_column_names = []
        empty_col_counter = 0
        seen_final_names = {} # Tracks the final unique names

        for h_orig in raw_headers_original:
            # Step 1: Clean and get the base name
            cleaned_h_orig = h_orig.strip().replace('"', '')

            # Step 2: Apply the header_mapping if a direct map exists
            base_name = header_mapping.get(cleaned_h_orig, cleaned_h_orig)
            
            # Step 3: Handle empty names (from empty cells)
            if base_name == '':
                base_name = f'_EMPTY_COL_{empty_col_counter}'
                empty_col_counter += 1
            
            # Step 4: Ensure the name is unique by appending a counter
            unique_name = base_name
            counter = 0
            # If the base_name already exists, append a counter.
            # If '종합지수' maps to 'KCCI_Composite_Index', and '종합지수' appears again,
            # it will try 'KCCI_Composite_Index_1', 'KCCI_Composite_Index_2', etc.
            # This requires `header_mapping` to *not* have duplicate values for different keys if those keys are original headers.
            # The previous header_mapping had '종합지수' appearing multiple times with different mapped values, which is good.
            # The problem is when the *original* raw headers are the same, but they map to the *same* target name.
            # Example: original '미주서안' (KCCI) -> 'US_West_Coast', original '미주서안' (SCFI) -> 'US_West_Coast_SCFI'
            # This is why the header_mapping needs to be very specific.

            # Let's use a simpler approach for unique naming:
            # After applying the initial header_mapping, if there are still duplicates, append a number.
            # This is what the previous `seen_headers` was trying to do.
            
            # Re-implementing the unique name generation more carefully:
            current_mapped_name = base_name
            while current_mapped_name in seen_final_names:
                counter += 1
                current_mapped_name = f"{base_name}_{counter}"
            
            seen_final_names[current_mapped_name] = True
            final_column_names.append(current_mapped_name)

        # Now, `final_column_names` contains all unique column names to be used for the DataFrame.
        df = pd.DataFrame(data_rows, columns=final_column_names)
        # No need for df.rename(columns=header_mapping) here, as mapping is done during column name generation.
        # The only rename needed might be if `date_col_name` is not 'date' but needs to be.
        # This handles the 'DataFrame columns are not unique' warning.


        date_col_name = None
        # Prioritize 'date' column if it exists and is not an empty placeholder
        # Check if 'date' is directly in the final_column_names
        if 'date' in df.columns and not df['date'].empty and df['date'].astype(str).str.strip().any():
            date_col_name = 'date'
        # Check for Date_Blank_Sailing
        elif 'Date_Blank_Sailing' in df.columns and not df['Date_Blank_Sailing'].empty and df['Date_Blank_Sailing'].astype(str).str.strip().any():
            date_col_name = 'Date_Blank_Sailing'
        # Check if any _EMPTY_COL_X contains dates (this is a fallback and less reliable)
        else:
            for col_name in df.columns:
                if col_name.startswith('_EMPTY_COL_') and not df[col_name].empty and df[col_name].astype(str).str.strip().any():
                    # Attempt to parse as date; if successful, use this column
                    temp_series = pd.to_datetime(df[col_name], errors='coerce')
                    if not temp_series.dropna().empty: # If there are valid dates after coercion
                        date_col_name = col_name
                        print(f"DEBUG: Found date column in auto-generated empty column: {date_col_name}")
                        break # Found a date column, stop searching

        if date_col_name:
            # Convert to datetime, coercing errors (invalid dates become NaT)
            df[date_col_name] = pd.to_datetime(df[date_col_name], errors='coerce')
            # Drop rows where the date is NaT (Not a Time)
            df.dropna(subset=[date_col_name], inplace=True)
            # Sort by date
            df = df.sort_values(by=date_col_name, ascending=True)
            # Format date to YYYY-MM-DD string for consistency in JSON/JS
            df['date'] = df[date_col_name].dt.strftime('%Y-%m-%d')
            # If the original date column was not named 'date', rename it for consistency
            if date_col_name != 'date':
                df.rename(columns={date_col_name: 'date'}, inplace=True)
        else:
            print("Warning: No recognized date column found in the DataFrame or it contains no valid dates. Charts might not display correctly.")

        # --- Convert all numeric columns using apply with a lambda function ---
        numeric_cols = [col for col in df.columns if col != 'date']
        for col in numeric_cols:
            # Apply a lambda function to each element of the Series
            # This converts to string, removes commas, then converts to numeric
            df[col] = df[col].apply(lambda x: pd.to_numeric(str(x).replace(',', ''), errors='coerce'))
        # --- END NEW LOGIC ---

        # Modified: Use pd.NA for fillna
        df = df.fillna(pd.NA) # Changed value=None to pd.NA

        processed_data = df.to_dict(orient='records')

        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)

        print(f"Data successfully saved to '{OUTPUT_JSON_PATH}'.")
        print(f"Sample of saved data (first 3 entries): {processed_data[:3]}")

    except Exception as e:
        print(f"Error during data processing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_process_data()
