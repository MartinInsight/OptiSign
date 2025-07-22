import gspread
import json
import os
import pandas as pd

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
header_mapping = {
    # KCCI Chart Headers (Chart 1)
    "KCCI종합지수(Point)": "KCCI_Composite_Index",
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

    # SCFI Chart Headers (Chart 2)
    "SCFI종합지수($/TEU)": "SCFI_Composite_Index",
    "미주서안미주서안": "US_West_Coast_SCFI", # Assuming this is a typo in provided data, should be unique
    "미주동안미주동안": "US_East_Coast_SCFI", # Assuming this is a typo
    "북유럽": "North_Europe_SCFI",
    "지중해지중해": "Mediterranean_SCFI", # Assuming typo
    "동남아시아동남아시아": "Southeast_Asia_SCFI", # Assuming typo
    "중동중동": "Middle_East_SCFI", # Assuming typo
    "호주/뉴질랜드": "Australia_New_Zealand_SCFI",
    "남아메리카": "South_America_SCFI",
    "일본서안": "Japan_West_Coast_SCFI",
    "일본동안": "Japan_East_Coast_SCFI",
    "한국": "Korea_SCFI",
    "동부/서부 아프리카": "East_West_Africa_SCFI",
    "남아공": "South_Africa_SCFI",

    # WCI Chart Headers (Chart 3)
    "WCI종합지수": "WCI_Composite_Index",
    "상하이 → 로테르담": "Shanghai_Rotterdam",
    "로테르담 → 상하이": "Rotterdam_Shanghai",
    "상하이 → 제노바": "Shanghai_Genoa",
    "상하이 → 로스엔젤레스": "Shanghai_Los_Angeles",
    "로스엔젤레스 → 상하이": "Los_Angeles_Shanghai",
    "상하이 → 뉴욕": "Shanghai_New_York",
    "뉴욕 → 로테르담": "New_York_Rotterdam",
    "로테르담 → 뉴욕": "Rotterdam_New_York",

    # IACI Chart Headers (Chart 4)
    "IACIdate종합지수": "IACI_Composite_Index", # This header is very ambiguous. Assuming it's the column next to 'date'

    # BLANK_SAILING Chart Headers (Chart 5)
    "Index": "Date_Blank_Sailing", # Assuming 'Index' column is actually the date for this section
    "Gemini Cooperation": "Gemini_Cooperation",
    "MSC": "MSC", # Added MSC as per provided raw headers
    "OCEAN Alliance": "OCEAN_Alliance",
    "Premier Alliance": "Premier_Alliance",
    "Others/Independent": "Others_Independent",
    "Total": "Total_Blank_Sailings",

    # FBX Chart Headers (Chart 6)
    "FBX종합지수": "FBX_Composite_Index",
    "중국/동아시아 → 미주서안": "China_EA_US_West_Coast",
    "미주서안 → 중국/동아시아": "US_West_Coast_China_EA",
    "중국/동아시아 → 미주동안": "China_EA_US_East_Coast",
    "미주동안 → 중국/동아시아": "US_East_Coast_China_EA",
    "중국/동아시아 → 북유럽": "China_EA_North_Europe",
    "북유럽 → 중국/동아시아": "North_Europe_China_EA",
    "중국/동아시아 → 지중해": "China_EA_Mediterranean",
    "지중해 → 중국/동아시아": "Mediterranean_China_EA",
    "미주동안 → 북유럽": "US_East_Coast_North_Europe",
    "북유럽 → 미주동안": "North_Europe_US_East_Coast",
    "유럽 → 남미동안": "Europe_South_America_East_Coast",
    "유럽 → 남미서안": "Europe_South_America_West_Coast",

    # XSI Chart Headers (Chart 7)
    "동아시아 → 북유럽": "East_Asia_North_Europe_XSI",
    "북유럽 → 동아시아": "North_Europe_East_Asia_XSI",
    "동아시아 → 미주서안": "East_Asia_US_West_Coast_XSI",
    "미주서안 → 동아시아": "US_West_Coast_East_Asia_XSI",
    "동아시아 → 남미동안": "East_Asia_South_America_East_Coast_XSI",
    "북유럽 → 미주동안": "North_Europe_US_East_Coast_XSI",
    "미주동안 → 북유럽": "US_East_Coast_North_Europe_XSI",
    "북유럽 → 남미동안": "North_Europe_South_America_East_Coast_XSI",

    # MBCI Chart Headers (Chart 8 - if needed later)
    "Index(종합지수), $/day(정기용선, Time charter)MBCI": "MBCI_Index" # Corrected header from raw data
}
# --- End Header Mapping ---


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
        
        # Replace empty header strings with unique placeholders
        raw_headers = []
        empty_col_counter = 0
        for h in raw_headers_original:
            if h == '':
                raw_headers.append(f'_EMPTY_COL_{empty_col_counter}')
                empty_col_counter += 1
            else:
                raw_headers.append(h)

        data_rows_raw = all_data[header_row_index + 1:]
        
        # --- NEW LOGIC: Ensure all data rows have the same number of columns as headers ---
        num_expected_cols = len(raw_headers)
        data_rows = []
        for i, row in enumerate(data_rows_raw):
            if len(row) < num_expected_cols:
                # Pad shorter rows with None
                padded_row = row + [None] * (num_expected_cols - len(row))
                data_rows.append(padded_row)
                print(f"WARNING: Row {i+header_row_index+2} was shorter ({len(row)} cols). Padded to {num_expected_cols} cols.")
            elif len(row) > num_expected_cols:
                # Truncate longer rows
                truncated_row = row[:num_expected_cols]
                data_rows.append(truncated_row)
                print(f"WARNING: Row {i+header_row_index+2} was longer ({len(row)} cols). Truncated to {num_expected_cols} cols.")
            else:
                data_rows.append(row)
        # --- END NEW LOGIC ---

        # --- Additional Debugging Prints for DataFrame creation ---
        print(f"DEBUG: Total rows fetched (all_data): {len(all_data)}")
        print(f"DEBUG: Header row index: {header_row_index}")
        print(f"DEBUG: Raw headers (original from sheet): {raw_headers_original}")
        print(f"DEBUG: Raw headers (used for DataFrame): {raw_headers}")
        print(f"DEBUG: Number of raw headers (used for DataFrame): {len(raw_headers)}")
        print(f"DEBUG: Number of data rows (after processing): {len(data_rows)}") # Updated print
        if len(data_rows) > 0:
            print(f"DEBUG: First processed data row: {data_rows[0]}") # Updated print
            print(f"DEBUG: Number of columns in first processed data row: {len(data_rows[0])}") # Updated print
            # This warning should now ideally not appear if padding/truncating works
            if len(raw_headers) != len(data_rows[0]):
                print("WARNING: Number of headers (used for DataFrame) does NOT match number of columns in the first data row!")
        # --- End Additional Debugging Prints ---

        # Use the modified raw_headers (with placeholders for empty names)
        df = pd.DataFrame(data_rows, columns=raw_headers)
        df.rename(columns={k: v for k, v in header_mapping.items() if k in df.columns}, inplace=True)

        date_col_name = None
        # Prioritize 'date' column if it exists and is not an empty placeholder
        if 'date' in df.columns and df['date'].any(): # Check if 'date' column exists and has non-empty values
            date_col_name = 'date'
        elif 'Date_Blank_Sailing' in df.columns and df['Date_Blank_Sailing'].any(): # Fallback for BLANK_SAILING
            date_col_name = 'Date_Blank_Sailing'
        elif '_EMPTY_COL_4' in df.columns and df['_EMPTY_COL_4'].any(): # Fallback for IACI if 'date' is in _EMPTY_COL_4
             # This is a guess based on the raw headers provided earlier, where 'date' was at index 44 (0-indexed)
             # which could become _EMPTY_COL_4 if the header before it was empty.
            date_col_name = '_EMPTY_COL_4' # This needs to be the actual header name pandas uses

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
            # If no date column, consider using row index or a different approach for X-axis in JS.
            # For now, we proceed without a dedicated 'date' column if it's missing or invalid.

        for col in df.columns:
            # Skip the 'date' column as it's already processed and might be string
            if col != 'date':
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.fillna(value=None)

        processed_data = df.to_dict(orient='records')

        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)

        print(f"Data successfully saved to '{OUTPUT_JSON_PATH}'.")
        print(f"Sample of saved data (first 3 entries): {processed_data[:3]}")

    except Exception as e:
        print(f"Error during data processing: {e}")
        # Print full traceback for better debugging
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_process_data()
