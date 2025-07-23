import gspread
import json
import os
import pandas as pd
import traceback
import re # Import re for regex operations

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
SECTION_MARKER_SEQUENCE = [
    ("종합지수(Point)와 그 외 항로별($/FEU)", "KCCI"),
    ("종합지수($/TEU), 미주항로별($/FEU), 그 외 항로별($/TEU)", "SCFI"),
    ("종합지수와 각 항로별($/FEU)", "WCI"),
    ("IACIdate종합지수", "IACI"), # This will map to IACI_Container_Index_Raw
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
        
        # --- NEW LOGIC for generating unique and meaningful column names ---
        final_column_names = []
        current_section_prefix = "" # e.g., "KCCI_", "SCFI_", etc.
        empty_col_counter = 0
        seen_final_names_set = set() # To ensure absolute uniqueness of final names
        section_marker_sequence_index = 0 # To track current position in SECTION_MARKER_SEQUENCE

        for h_orig in raw_headers_original:
            cleaned_h_orig = h_orig.strip().replace('"', '')
            
            final_name_candidate = cleaned_h_orig # Default to original Korean name

            # Rule priority: Section Marker (by sequence) > Specific Rename > Common Prefixed > Special fixed > Empty > Default (Original Korean)

            # 1. Check if it's the next expected section marker in the sequence (or any subsequent marker)
            found_section_marker_in_sequence = False
            for i in range(section_marker_sequence_index, len(SECTION_MARKER_SEQUENCE)):
                marker_string, marker_prefix_base = SECTION_MARKER_SEQUENCE[i]
                if cleaned_h_orig == marker_string:
                    # This is a section marker!
                    current_section_prefix = f"{marker_prefix_base}_"
                    
                    # Special handling for the 'Index' section marker for Blank Sailing
                    if marker_prefix_base == "BLANK_SAILING":
                        final_name_candidate = "Date_Blank_Sailing"
                        current_section_prefix = "" # No prefix for subsequent columns in this section, as they are specific renames
                    elif marker_prefix_base == "IACI": # Special handling for IACI
                        final_name_candidate = "IACI_Container_Index_Raw" # Keep raw string for parsing later
                        current_section_prefix = "" # IACI has no other prefixed columns
                    else:
                        final_name_candidate = f"{marker_prefix_base}_Container_Index" 
                    
                    section_marker_sequence_index = i + 1 # Advance sequence index to *after* this found marker
                    found_section_marker_in_sequence = True
                    break # Found the section marker, break from inner loop
            
            if found_section_marker_in_sequence:
                pass # Already handled by the section marker logic above
            # 2. Apply SPECIFIC_RENAMES (these are unique and should not be prefixed by section)
            elif cleaned_h_orig in SPECIFIC_RENAMES:
                final_name_candidate = SPECIFIC_RENAMES[cleaned_h_orig]
            # 3. Apply COMMON_DATA_HEADERS_TO_PREFIX (these should be prefixed if a section is active)
            elif cleaned_h_orig in COMMON_DATA_HEADERS_TO_PREFIX:
                base_name = COMMON_DATA_HEADERS_TO_PREFIX[cleaned_h_orig]
                if current_section_prefix: # Apply prefix if one is active
                    final_name_candidate = f"{current_section_prefix}{base_name}"
                else: # Fallback if no active prefix (e.g., for KCCI section if it's the first data column)
                    final_name_candidate = base_name
            # 4. Handle special fixed names (like 'date') - 'Index' is handled by SECTION_MARKER_SEQUENCE
            elif cleaned_h_orig == 'date':
                final_name_candidate = 'date'
            # 5. Handle empty cells
            elif cleaned_h_orig == '':
                final_name_candidate = f'_EMPTY_COL_{empty_col_counter}'
                empty_col_counter += 1
            # 6. Default: Keep original cleaned Korean name if no specific rule applies
            else:
                final_name_candidate = cleaned_h_orig
            
            # Ensure the final name is absolutely unique by appending a suffix if needed
            final_unique_name = final_name_candidate
            suffix = 0
            while final_unique_name in seen_final_names_set:
                suffix += 1
                final_unique_name = f"{final_name_candidate}_{suffix}"
            
            seen_final_names_set.add(final_unique_name)
            final_column_names.append(final_unique_name)
        # --- END NEW LOGIC ---

        print(f"DEBUG: Final column names list before DataFrame creation: {final_column_names}")

        data_rows_raw = all_data[header_row_index + 1:]
        
        # Ensure all data rows have the same number of columns as headers
        num_expected_cols = len(final_column_names)
        data_rows = []
        for i, row in enumerate(data_rows_raw):
            cleaned_row = [str(cell) if cell is not None else '' for cell in row]
            
            if len(cleaned_row) < num_expected_cols:
                padded_row = cleaned_row + [''] * (num_expected_cols - len(cleaned_row))
                data_rows.append(padded_row)
                print(f"WARNING: Row {i+header_row_index+2} was shorter ({len(cleaned_row)} cols). Padded to {num_expected_cols} cols.")
            elif len(cleaned_row) > num_expected_cols:
                truncated_row = cleaned_row[:num_expected_cols]
                data_rows.append(truncated_row)
                print(f"WARNING: Row {i+header_row_index+2} was longer ({len(cleaned_row)} cols). Truncated to {num_expected_cols} cols.")
            else:
                data_rows.append(cleaned_row)

        df = pd.DataFrame(data_rows, columns=final_column_names)

        # --- Special handling for IACI_Container_Index_Raw to extract date and value ---
        if 'IACI_Container_Index_Raw' in df.columns:
            # Regex to capture date (MM/DD/YYYY) and value (digits)
            iaci_pattern = re.compile(r'(\d{1,2}/\d{1,2}/\d{4})(\d+)')
            
            # Create new columns for parsed date and value
            parsed_iaci_data = df['IACI_Container_Index_Raw'].astype(str).apply(lambda x: pd.Series(iaci_pattern.match(x).groups()) if iaci_pattern.match(x) else pd.Series([None, None]))
            
            df['IACI_Parsed_Date'] = parsed_iaci_data[0]
            df['IACI_Composite_Index'] = parsed_iaci_data[1]

            # Convert the parsed IACI value to numeric
            df['IACI_Composite_Index'] = pd.to_numeric(df['IACI_Composite_Index'], errors='coerce')

            # Drop the original 'IACI_Container_Index_Raw' column as its content has been parsed
            df.drop(columns=['IACI_Container_Index_Raw'], inplace=True)
        else:
            # Ensure IACI_Composite_Index column exists even if IACI_Container_Index_Raw wasn't found
            if 'IACI_Composite_Index' not in df.columns:
                df['IACI_Composite_Index'] = None
            # Also ensure IACI_Parsed_Date exists for date processing later
            if 'IACI_Parsed_Date' not in df.columns:
                df['IACI_Parsed_Date'] = None


        # --- Date column processing ---
        # The 'date' column already exists from DataFrame creation and contains primary dates.
        # Convert the main 'date' column to datetime objects first
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Fill missing dates in the main 'date' column using IACI_Parsed_Date if available
        if 'IACI_Parsed_Date' in df.columns:
            df['date'] = df['date'].fillna(pd.to_datetime(df['IACI_Parsed_Date'], errors='coerce'))
            df.drop(columns=['IACI_Parsed_Date'], inplace=True) # Drop temporary column

        # Fill remaining missing dates using Date_Blank_Sailing if available
        if 'Date_Blank_Sailing' in df.columns:
            df['date'] = df['date'].fillna(pd.to_datetime(df['Date_Blank_Sailing'], errors='coerce'))
            df.drop(columns=['Date_Blank_Sailing'], inplace=True) # Drop temporary column
        
        # Drop any remaining _EMPTY_COL_0 if it's not the primary date column
        if '_EMPTY_COL_0' in df.columns:
            df.drop(columns=['_EMPTY_COL_0'], inplace=True)


        df.dropna(subset=['date'], inplace=True) # Drop rows where final date parsing failed
        df = df.sort_values(by='date', ascending=True)
        df['date'] = df['date'].dt.strftime('%Y-%m-%d') # Format to YYYY-MM-DD

        if df['date'].empty:
            print("Warning: After all date processing, the 'date' column is empty. Charts might not display correctly.")


        # --- Convert all numeric columns ---
        # Exclude 'date' and any _Container_Index or _Raw columns that are no longer needed
        numeric_cols = [col for col in df.columns if col != 'date' and not col.endswith('_Container_Index_Raw')]
        for col in numeric_cols:
            df[col] = df[col].apply(lambda x: pd.to_numeric(str(x).replace(',', ''), errors='coerce'))
        
        # Convert NaN to None for JSON serialization
        df = df.replace({pd.NA: None, float('nan'): None})

        processed_data = df.to_dict(orient='records')

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
