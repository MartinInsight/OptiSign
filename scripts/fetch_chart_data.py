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
            if any(cell.strip().lower() == "date" for cell in row):
                header_row_index = i
                break

        if header_row_index == -1:
            print("Error: Could not find the header row containing 'date'.")
            return

        raw_headers_original = [h.strip().replace('"', '') for h in all_data[header_row_index]]
        
        # Replace empty header strings with unique placeholders
        # This ensures every column has a name, even if it was blank in the sheet.
        raw_headers = []
        empty_col_counter = 0
        for h in raw_headers_original:
            if h == '':
                raw_headers.append(f'_EMPTY_COL_{empty_col_counter}')
                empty_col_counter += 1
            else:
                raw_headers.append(h)

        data_rows = all_data[header_row_index + 1:]

        # --- Additional Debugging Prints for DataFrame creation ---
        print(f"DEBUG: Total rows fetched (all_data): {len(all_data)}")
        print(f"DEBUG: Header row index: {header_row_index}")
        print(f"DEBUG: Raw headers (original from sheet): {raw_headers_original}")
        print(f"DEBUG: Raw headers (used for DataFrame): {raw_headers}") # New print
        print(f"DEBUG: Number of raw headers (used for DataFrame): {len(raw_headers)}") # New print
        print(f"DEBUG: Number of data rows: {len(data_rows)}")
        if len(data_rows) > 0:
            print(f"DEBUG: First data row: {data_rows[0]}")
            print(f"DEBUG: Number of columns in first data row: {len(data_rows[0])}")
            if len(raw_headers) != len(data_rows[0]):
                print("WARNING: Number of headers (used for DataFrame) does NOT match number of columns in the first data row!")
        # --- End Additional Debugging Prints ---

        # Use the modified raw_headers (with placeholders for empty names)
        df = pd.DataFrame(data_rows, columns=raw_headers)
        df.rename(columns={k: v for k, v in header_mapping.items() if k in df.columns}, inplace=True)

        date_col_name = None
        if 'date' in df.columns:
            date_col_name = 'date'
        elif 'Date_Blank_Sailing' in df.columns:
            date_col_name = 'Date_Blank_Sailing'

        if date_col_name:
            df[date_col_name] = pd.to_datetime(df[date_col_name], errors='coerce')
            df.dropna(subset=[date_col_name], inplace=True)
            df = df.sort_values(by=date_col_name, ascending=True)
            df['date'] = df[date_col_name].dt.strftime('%Y-%m-%d')
            if date_col_name != 'date':
                df.rename(columns={date_col_name: 'date'}, inplace=True)
        else:
            print("Warning: No recognized date column found in the DataFrame. Charts might not display correctly.")

        for col in df.columns:
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

if __name__ == "__main__":
    fetch_and_process_data()
