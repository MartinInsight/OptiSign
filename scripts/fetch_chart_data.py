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

# --- Header Mapping Definitions ---
# This list defines the sequence of section markers and their corresponding prefixes.
# This is crucial for handling identical section header strings that appear in different parts of the sheet.
SECTION_MARKER_SEQUENCE = [
    ("종합지수(Point)와 그 외 항로별($/FEU)", "KCCI"),
    ("종합지수($/TEU), 미주항로별($/FEU), 그 외 항로별($/TEU)", "SCFI"),
    ("종합지수와 각 항로별($/FEU)", "WCI"), # First occurrence of this string
    ("IACIdate종합지수", "IACI"),
    ("Index", "BLANK_SAILING"), # Special case for Blank Sailing 'Index' which acts as a date
    ("종합지수와 각 항로별($/FEU)", "FBX"), # Second occurrence of this string, mapped to FBX
    ("각 항로별($/FEU)", "XSI"),
    ("Index(종합지수), $/day(정기용선, Time charter)", "MBCI")
]

# Maps common data headers (Korean) to their base English names.
# These will be prefixed by the current section's English name.
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
    "MBCI": "MBCI_Value" # Data column within MBCI section
}

# Specific mappings for headers that are unique in the raw data
# but we want to rename for clarity. These will NOT be prefixed by section.
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

            # Debug print
            # print(f"DEBUG: Processing original header: '{cleaned_h_orig}'")
            # print(f"DEBUG:   current_section_prefix before check: '{current_section_prefix}'")
            # print(f"DEBUG:   section_marker_sequence_index: {section_marker_sequence_index}")

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
                        # print(f"DEBUG:   Blank Sailing Section Marker found. final_name_candidate: '{final_name_candidate}'")
                    else:
                        final_name_candidate = f"{marker_prefix_base}_Section_Header"
                        # print(f"DEBUG:   Section marker found (sequence match). New prefix: '{current_section_prefix}', final_name_candidate: '{final_name_candidate}'")
                    
                    section_marker_sequence_index = i + 1 # Advance sequence index to *after* this found marker
                    found_section_marker_in_sequence = True
                    break # Found the section marker, break from inner loop
            
            if found_section_marker_in_sequence:
                pass # Already handled by the section marker logic above
            # 2. Apply SPECIFIC_RENAMES (these are unique and should not be prefixed by section)
            elif cleaned_h_orig in SPECIFIC_RENAMES:
                final_name_candidate = SPECIFIC_RENAMES[cleaned_h_orig]
                # print(f"DEBUG:   Specific rename found. final_name_candidate: '{final_name_candidate}'")
            # 3. Apply COMMON_DATA_HEADERS_TO_PREFIX (these should be prefixed if a section is active)
            elif cleaned_h_orig in COMMON_DATA_HEADERS_TO_PREFIX:
                base_name = COMMON_DATA_HEADERS_TO_PREFIX[cleaned_h_orig]
                if current_section_prefix: # Apply prefix if one is active
                    final_name_candidate = f"{current_section_prefix}{base_name}"
                    # print(f"DEBUG:   Common data header found. Applied section prefix. final_name_candidate: '{final_name_candidate}'")
                else: # Fallback if no active prefix (e.g., for KCCI section if it's the first data column)
                    final_name_candidate = base_name
                    # print(f"DEBUG:   Common data header found. No active section prefix. final_name_candidate: '{final_name_candidate}'")
            # 4. Handle special fixed names (like 'date') - 'Index' is handled by SECTION_MARKER_SEQUENCE
            elif cleaned_h_orig == 'date':
                final_name_candidate = 'date'
                # print(f"DEBUG:   Date header found. final_name_candidate: '{final_name_candidate}'")
            # 5. Handle empty cells
            elif cleaned_h_orig == '':
                final_name_candidate = f'_EMPTY_COL_{empty_col_counter}'
                empty_col_counter += 1
                # print(f"DEBUG:   Empty column found. final_name_candidate: '{final_name_candidate}'")
            # 6. Default: Keep original cleaned Korean name if no specific rule applies
            else:
                final_name_candidate = cleaned_h_orig
                # print(f"DEBUG:   No specific mapping, keeping original: '{final_name_candidate}'")
            
            # Ensure the final name is absolutely unique by appending a suffix if needed
            final_unique_name = final_name_candidate
            suffix = 0
            while final_unique_name in seen_final_names_set:
                suffix += 1
                final_unique_name = f"{final_name_candidate}_{suffix}"
            
            seen_final_names_set.add(final_unique_name)
            final_column_names.append(final_unique_name)
            # print(f"DEBUG:   Final unique name added: '{final_unique_name}'")
        # --- END NEW LOGIC ---

        # Add this new debug print to confirm the final_column_names list
        print(f"DEBUG: Final column names list before DataFrame creation: {final_column_names}")

        data_rows_raw = all_data[header_row_index + 1:]
        
        # Ensure all data rows have the same number of columns as headers
        num_expected_cols = len(final_column_names) # Use the length of the new unique names
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
        print(f"DEBUG: Raw headers (used for DataFrame - unique): {final_column_names}") # Updated print
        print(f"DEBUG: Number of raw headers (used for DataFrame): {len(final_column_names)}")
        print(f"DEBUG: Number of data rows (after processing): {len(data_rows)}")
        if len(data_rows) > 0:
            print(f"DEBUG: First processed data row: {data_rows[0]}")
            print(f"DEBUG: Number of columns in first processed data row: {len(data_rows[0])}")
            if len(final_column_names) != len(data_rows[0]):
                print("WARNING: Number of headers (used for DataFrame) does NOT match number of columns in the first data row!")
        # --- End Additional Debugging Prints ---

        # Create DataFrame with the guaranteed unique column names
        df = pd.DataFrame(data_rows, columns=final_column_names)
        # No need for df.rename(columns=header_mapping) here, as names are set during column generation.

        date_col_name = None
        # Prioritize 'date' column if it exists and is not empty
        if 'date' in df.columns and not df['date'].empty and df['date'].astype(str).str.strip().any():
            date_col_name = 'date'
        # Fallback for 'Date_Blank_Sailing' if it exists and is not empty
        elif 'Date_Blank_Sailing' in df.columns and not df['Date_Blank_Sailing'].empty and df['Date_Blank_Sailing'].astype(str).str.strip().any():
            date_col_name = 'Date_Blank_Sailing'
        # Fallback for 'IACI_Composite_Index' if it contains dates (this is a guess based on previous logs)
        elif 'IACI_Composite_Index' in df.columns and not df['IACI_Composite_Index'].empty:
            # Attempt to parse as date; if successful, use this column
            temp_series = pd.to_datetime(df['IACI_Composite_Index'], errors='coerce')
            if not temp_series.dropna().empty: # If there are valid dates after coercion
                date_col_name = 'IACI_Composite_Index'
                print(f"DEBUG: Found date column in IACI_Composite_Index: {date_col_name}")

        if date_col_name:
            df[date_col_name] = pd.to_datetime(df[date_col_name], errors='coerce')
            df.dropna(subset=[date_col_name], inplace=True)
            df = df.sort_values(by=date_col_name, ascending=True)
            df['date'] = df[date_col_name].dt.strftime('%Y-%m-%d')
            if date_col_name != 'date':
                df.rename(columns={date_col_name: 'date'}, inplace=True)
        else:
            print("Warning: No recognized date column found in the DataFrame or it contains no valid dates. Charts might not display correctly.")

        # --- Convert all numeric columns using apply with a lambda function ---
        numeric_cols = [col for col in df.columns if col != 'date' and not col.endswith('_Section_Header')]
        for col in numeric_cols:
            df[col] = df[col].apply(lambda x: pd.to_numeric(str(x).replace(',', ''), errors='coerce'))
        # --- NEW: Convert NaN to None for JSON serialization ---
        # This is crucial because json.dumps does not support NaN directly, but converts None to null.
        df = df.replace({pd.NA: None, float('nan'): None})
        # --- END NEW LOGIC ---

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
