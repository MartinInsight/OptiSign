import gspread
import json
import os
import pandas as pd

# --- Configuration ---
# Get credentials and spreadsheet ID from GitHub Secrets environment variables
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDENTIAL_JSON = os.environ.get("GOOGLE_CREDENTIAL_JSON")

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
        return

    try:
        # 1. Authenticate with Google Sheets
        # GOOGLE_CREDENTIAL_JSON is a JSON string, so it needs to be parsed
        credentials_dict = json.loads(GOOGLE_CREDENTIAL_JSON)
        gc = gspread.service_account_account_from_dict(credentials_dict)

        # 2. Open the spreadsheet and worksheet
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

        # 3. Fetch all data from the sheet (up to 400 rows as implied, get_all_values() gets all non-empty)
        # We'll fetch all values and then filter rows based on the header.
        all_data = worksheet.get_all_values()
        if not all_data:
            print("Error: No data fetched from the sheet.")
            return

        # 4. Parse and process data
        # Find the actual header row (the one containing 'date')
        header_row_index = -1
        for i, row in enumerate(all_data):
            # Check for 'date' in a case-insensitive way, and strip whitespace
            if any(cell.strip().lower() == "date" for cell in row):
                header_row_index = i
                break

        if header_row_index == -1:
            print("Error: Could not find the header row containing 'date'.")
            return

        # Extract actual headers and data rows
        # Clean headers by stripping whitespace and removing quotes
        raw_headers = [h.strip().replace('"', '') for h in all_data[header_row_index]]
        data_rows = all_data[header_row_index + 1:]

        # Define a mapping from original (complex/Korean) headers to cleaned (English-like) headers
        # This is crucial for consistent data access in JavaScript.
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
            "MSCOCEAN Alliance": "MSCOCEAN_Alliance",
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
            "MBCIIndex(종합지수), $/day(정기용선, Time charter)MBCI": "MBCI_Index"
        }

        # Create a DataFrame with original headers
        df = pd.DataFrame(data_rows, columns=raw_headers)

        # Rename columns using the mapping
        # Only rename columns that exist in the DataFrame and in our mapping
        df.rename(columns={k: v for k, v in header_mapping.items() if k in df.columns}, inplace=True)

        # Handle 'date' column specifically, as its original name might vary (e.g., 'date', 'Index' for BLANK_SAILING)
        # Find the actual date column in the DataFrame after renaming
        date_col_name = None
        if 'date' in df.columns:
            date_col_name = 'date'
        elif 'Date_Blank_Sailing' in df.columns: # For BLANK_SAILING section
            date_col_name = 'Date_Blank_Sailing'

        if date_col_name:
            df[date_col_name] = pd.to_datetime(df[date_col_name], errors='coerce')
            df.dropna(subset=[date_col_name], inplace=True) # Drop rows with invalid dates
            df = df.sort_values(by=date_col_name, ascending=True) # Sort by date
            df['date'] = df[date_col_name].dt.strftime('%Y-%m-%d') # Format date to YYYY-MM-DD string
            if date_col_name != 'date': # If original date column was not 'date', rename it to 'date' for consistency
                df.rename(columns={date_col_name: 'date'}, inplace=True)
        else:
            print("Warning: No recognized date column found in the DataFrame. Charts might not display correctly.")
            # If no date column, we might need to use row index or a different approach for X-axis in JS.
            # For now, let's proceed without a date column if it's missing.

        # Convert relevant columns to numeric, coercing errors
        # Iterate through all columns that are NOT 'date'
        for col in df.columns:
            if col != 'date':
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Fill NaN values with None (JSON null) for cleaner output, or 0 if preferred
        df = df.fillna(value=None) # Use None for NaN values in JSON

        # Convert DataFrame to a list of dictionaries (JSON format)
        processed_data = df.to_dict(orient='records')

        # 5. Save as JSON file
        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)

        print(f"Data successfully saved to '{OUTPUT_JSON_PATH}'.")
        print(f"Sample of saved data (first 3 entries): {processed_data[:3]}")

    except Exception as e:
        print(f"Error during data processing: {e}")

if __name__ == "__main__":
    fetch_and_process_data()
