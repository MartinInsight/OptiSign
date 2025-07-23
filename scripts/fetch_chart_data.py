import gspread
import json
import os
import pandas as pd
import traceback
import re
from datetime import datetime

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
WEATHER_WORKSHEET_NAME = "LA날씨" # New: Weather sheet name
EXCHANGE_RATE_WORKSHEET_NAME = "환율" # New: Exchange rate sheet name
CRAWLING_DATA2_WORKSHEET_NAME = "Crawling_Data2" # New: For summary tables
OUTPUT_JSON_PATH = "data/crawling_data.json"

# --- Header Mapping Definitions ---
SECTION_MARKER_SEQUENCE = [
    ("종합지수(Point)와 그 외 항로별($/FEU)", "KCCI"),
    ("종합지수($/TEU), 미주항로별($/FEU), 그 외 항로별($/TEU)", "SCFI"),
    ("종합지수와 각 항로별($/FEU)", "WCI"),
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
    Handles columnar data, weather data, and exchange rate data.
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

        # --- Fetch Main Chart Data (from Crawling_Data) ---
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        all_data = worksheet.get_all_values()

        print(f"DEBUG: Total rows fetched from Google Sheet (raw): {len(all_data)}")

        if not all_data:
            print("Error: No data fetched from the main chart sheet.")
            return

        # Find the main header row (likely the one with 'date' for IACI and other main headers)
        main_header_row_index = -1
        for i in range(len(all_data)):
            row = all_data[i]
            # Corrected NameError: iterate over cells in row
            if any(marker[0] in str(cell) for cell in row for marker in SECTION_MARKER_SEQUENCE):
                main_header_row_index = i
                break
        
        # Fallback if the combined heuristic fails (less likely but safer)
        if main_header_row_index == -1:
            for i in range(len(all_data)):
                row = all_data[i]
                if any(str(cell).strip().lower() == "date" for cell in row): # Ensure 'date' is explicitly checked
                    main_header_row_index = i
                    break

        print(f"DEBUG: Main chart data header row index: {main_header_row_index}")

        if main_header_row_index == -1:
            print("Error: Could not find a suitable header row containing 'date' or section markers in main chart data.")
            return

        # Get raw headers from the identified header row
        raw_headers_original = [str(h).strip().replace('"', '') for h in all_data[main_header_row_index]]
        print(f"DEBUG: Raw headers from Google Sheet: {raw_headers_original}")

        # --- Define section column mappings based on user's input (A:O, Q:AE etc.) ---
        # These are 0-indexed column numbers in the raw data (all_data)
        # The 'date_col_name' will be the key for the date column in the final JSON for that section
        # BASED ON USER'S LATEST EXPLICIT CORRECTIONS:
        section_col_details = {
            "KCCI": {"start_col": 0, "end_col": 14, "date_col_name": "KCCI_Date"}, # A:O
            "SCFI": {"start_col": 16, "end_col": 30, "date_col_name": "SCFI_Date"}, # Q:AE
            "WCI": {"start_col": 32, "end_col": 41, "date_col_name": "WCI_Date"}, # AG:AP
            "IACI": {"start_col": 43, "end_col": 44, "date_col_name": "IACI_Date"}, # AR:AS
            "BLANK_SAILING": {"start_col": 46, "end_col": 52, "date_col_name": "Blank_Sailing_Date"}, # AU:BA
            "FBX": {"start_col": 54, "end_col": 67, "date_col_name": "FBX_Date"}, # BC:BP (BP is index 67)
            "XSI": {"start_col": 69, "end_col": 77, "date_col_name": "XSI_Date"}, # BR:BZ (BR is index 69)
            "MBCI": {"start_col": 79, "end_col": 80, "date_col_name": "MBCI_Date"}, # CB:CC
        }
        print(f"DEBUG: Defined section_col_details: {section_col_details}")


        # Create a mapping from raw_header_original index to final desired name
        col_idx_to_final_header_name = {}
        
        # First, map the explicit section date columns based on their start_col
        for section_name, details in section_col_details.items():
            col_idx_to_final_header_name[details["start_col"]] = details["date_col_name"]

        # Then, map the rest of the data columns using the existing logic
        current_section_prefix = ""
        empty_col_counter = 0
        seen_final_names_set = set(col_idx_to_final_header_name.values()) # Initialize with section date names

        for col_idx, h_orig in enumerate(raw_headers_original):
            # If this column is already mapped as a section date column, skip it
            if col_idx in col_idx_to_final_header_name:
                continue

            cleaned_h_orig = h_orig.strip()
            final_name_candidate = cleaned_h_orig

            found_section_marker_in_sequence = False
            for marker_string, marker_prefix_base in SECTION_MARKER_SEQUENCE:
                if cleaned_h_orig == marker_string:
                    current_section_prefix = f"{marker_prefix_base}_"
                    if marker_prefix_base != "BLANK_SAILING": # BLANK_SAILING marker itself is not a data column
                        final_name_candidate = f"{marker_prefix_base}_Container_Index"
                    else: # For BLANK_SAILING, the marker itself doesn't become a data column
                        final_name_candidate = None # Set to None to indicate it's just a marker
                    found_section_marker_in_sequence = True
                    break
            
            if found_section_marker_in_sequence and final_name_candidate: # Only add if it's a data name
                unique_name = final_name_candidate
                suffix = 0
                while unique_name in seen_final_names_set:
                    suffix += 1
                    unique_name = f"{final_name_candidate}_{suffix}"
                col_idx_to_final_header_name[col_idx] = unique_name
                seen_final_names_set.add(unique_name)
            elif cleaned_h_orig in SPECIFIC_RENAMES:
                final_name_candidate = SPECIFIC_RENAMES[cleaned_h_orig]
                unique_name = final_name_candidate
                suffix = 0
                while unique_name in seen_final_names_set:
                    suffix += 1
                    unique_name = f"{final_name_candidate}_{suffix}"
                col_idx_to_final_header_name[col_idx] = unique_name
                seen_final_names_set.add(unique_name)
            elif cleaned_h_orig in COMMON_DATA_HEADERS_TO_PREFIX:
                base_name = COMMON_DATA_HEADERS_TO_PREFIX[cleaned_h_orig]
                if current_section_prefix and not base_name.startswith(current_section_prefix):
                    final_name_candidate = f"{current_section_prefix}{base_name}"
                else:
                    final_name_candidate = base_name
                unique_name = final_name_candidate
                suffix = 0
                while unique_name in seen_final_names_set:
                    suffix += 1
                    unique_name = f"{final_name_candidate}_{suffix}"
                col_idx_to_final_header_name[col_idx] = unique_name
                seen_final_names_set.add(unique_name)
            elif cleaned_h_orig == '': # Handle empty header cells that are not section date columns
                final_name_candidate = f'_EMPTY_COL_{empty_col_counter}'
                empty_col_counter += 1
                col_idx_to_final_header_name[col_idx] = final_name_candidate
                seen_final_names_set.add(final_name_candidate)
            else: # Default for unmapped headers
                unique_name = cleaned_h_orig
                suffix = 0
                while unique_name in seen_final_names_set:
                    suffix += 1
                    unique_name = f"{cleaned_h_orig}_{suffix}"
                col_idx_to_final_header_name[col_idx] = unique_name
                seen_final_names_set.add(unique_name)

        # Now, create a list of final column names in their original order
        # This will be the columns for the full raw DataFrame
        final_column_names_ordered = [col_idx_to_final_header_name.get(i, f'UNMAPPED_COL_{i}') 
                                      for i in range(len(raw_headers_original))]
        
        print(f"DEBUG: Final mapped column names (ordered): {final_column_names_ordered}")

        # Create a DataFrame from all raw data, using the newly ordered final column names
        # This DataFrame will contain all raw values, including empty strings for dates
        data_rows_for_df = all_data[main_header_row_index + 1:]
        # Pad rows if they are shorter than the number of columns
        processed_data_rows_for_df = []
        num_expected_cols_for_df = len(final_column_names_ordered)
        for row in data_rows_for_df:
            cleaned_row = [str(cell) if cell is not None else '' for cell in row]
            if len(cleaned_row) < num_expected_cols_for_df:
                padded_row = cleaned_row + [''] * (num_expected_cols_for_df - len(cleaned_row))
                processed_data_rows_for_df.append(padded_row)
            elif len(cleaned_row) > num_expected_cols_for_df:
                truncated_row = cleaned_row[:num_expected_cols_for_df]
                processed_data_rows_for_df.append(truncated_row)
            else:
                processed_data_rows_for_df.append(cleaned_row)

        df_raw_full = pd.DataFrame(processed_data_rows_for_df, columns=final_column_names_ordered)
        print(f"DEBUG: Raw full DataFrame shape after initial creation: {df_raw_full.shape}")

        # Drop columns that were identified as empty placeholders
        cols_to_drop = [col for col in df_raw_full.columns if col.startswith('_EMPTY_COL_') or col.startswith('UNMAPPED_COL_')]
        if cols_to_drop:
            print(f"DEBUG: Dropping empty placeholder and unmapped columns: {cols_to_drop}")
            df_raw_full.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        print(f"DEBUG: Raw full DataFrame shape after dropping empty/unmapped columns: {df_raw_full.shape}")


        processed_chart_data_by_section = {
            "KCCI": [], "SCFI": [], "WCI": [], "IACI": [],
            "BLANK_SAILING": [], "FBX": [], "XSI": [], "MBCI": []
        }

        # Iterate through each section and extract its specific data
        # Use the fixed section_col_details here
        for section_key, details in section_col_details.items():
            date_col_name_in_df = details["date_col_name"]
            
            # Get the actual column names for data within this section, including its date column
            section_data_col_names = []
            # Data columns start from the date column index + 1 up to the end column index of the section
            for col_idx in range(details["start_col"], details["end_col"] + 1):
                mapped_name = col_idx_to_final_header_name.get(col_idx)
                if mapped_name and mapped_name in df_raw_full.columns:
                    section_data_col_names.append(mapped_name)
            
            # Select only the date column and relevant data columns for this section
            cols_to_select = [date_col_name_in_df] + [col for col in section_data_col_names if col != date_col_name_in_df]
            
            # Ensure all selected columns actually exist in df_raw_full
            existing_cols_to_select = [col for col in cols_to_select if col in df_raw_full.columns]
            
            if not existing_cols_to_select:
                print(f"WARNING: No relevant columns found for section {section_key}. Skipping.")
                continue

            df_section = df_raw_full[existing_cols_to_select].copy()

            # --- NEW DEBUGGING FOR XSI AND FBX DATE/DATA COLUMNS ---
            if section_key == "XSI":
                # Debugging for XSI_Date (BR, index 69)
                br_col_idx_in_all_data = 69 # BR column index
                br_raw_values = [row[br_col_idx_in_all_data] for row in all_data[main_header_row_index + 1:] if len(row) > br_col_idx_in_all_data]
                print(f"DEBUG: RAW content of column BR (raw index {br_col_idx_in_all_data} - XSI_Date) from all_data: {br_raw_values[:100]}")
                
                # Debugging for BQ (index 68) which visually seemed to contain dates in screenshot
                bq_col_idx_in_all_data = 68 # BQ column index
                bq_raw_values = [row[bq_col_idx_in_all_data] for row in all_data[main_header_row_index + 1:] if len(row) > bq_col_idx_in_all_data]
                print(f"DEBUG: RAW content of column BQ (raw index {bq_col_idx_in_all_data} - Visually Date in Screenshot) from all_data: {bq_raw_values[:100]}")

            if section_key == "FBX": 
                # Debugging for BP (index 67)
                bp_col_idx_in_all_data = 67 # BP column index
                bp_raw_values = [row[bp_col_idx_in_all_data] for row in all_data[main_header_row_index + 1:] if len(row) > bp_col_idx_in_all_data]
                print(f"DEBUG: RAW content of column BP (raw index {bp_col_idx_in_all_data} - FBX last data) from all_data: {bp_raw_values[:100]}")
                
                # Debugging for BN (index 65)
                bn_col_idx_in_all_data = 65 # BN column index
                bn_raw_values = [row[bn_col_idx_in_all_data] for row in all_data[main_header_row_index + 1:] if len(row) > bn_col_idx_in_all_data]
                print(f"DEBUG: RAW content of column BN (raw index {bn_col_idx_in_all_data} - FBX data) from all_data: {bn_raw_values[:100]}")
            # --- END NEW DEBUGGING ---

            # Clean and parse dates for THIS section
            df_section[date_col_name_in_df] = df_section[date_col_name_in_df].astype(str).str.strip()
            
            # Log all original date strings for this section before parsing attempt
            print(f"DEBUG: All original date strings for {section_key} before parsing ({len(df_section[date_col_name_in_df])} entries): {df_section[date_col_name_in_df].tolist()}")

            # Store original date strings before conversion for accurate error logging
            original_date_strings_for_logging = df_section[date_col_name_in_df].copy() 

            df_section['parsed_date'] = pd.to_datetime(df_section[date_col_name_in_df], errors='coerce')
            
            # Log unparseable dates for this section
            unparseable_dates_series = original_date_strings_for_logging[df_section['parsed_date'].isna()]
            num_unparseable_dates = unparseable_dates_series.count() # Count non-empty unparseable strings
            if num_unparseable_dates > 0:
                print(f"WARNING: {num_unparseable_dates} dates could not be parsed for {section_key} and will be dropped. All unparseable date strings: {unparseable_dates_series.tolist()}")

            df_section.dropna(subset=['parsed_date'], inplace=True) # Drop rows where date parsing failed for this section
            print(f"DEBUG: DataFrame shape for {section_key} after date parsing and dropna: {df_section.shape}")

            # Convert numeric columns for this section (excluding the original date column and the new parsed_date column)
            cols_to_convert_to_numeric = [col for col in section_data_col_names if col != date_col_name_in_df]
            for col in cols_to_convert_to_numeric:
                df_section[col] = pd.to_numeric(df_section[col].astype(str).str.replace(',', ''), errors='coerce')
            
            df_section = df_section.replace({pd.NA: None, float('nan'): None})

            # Sort and format date
            df_section = df_section.sort_values(by='parsed_date', ascending=True)
            df_section['date'] = df_section['parsed_date'].dt.strftime('%Y-%m-%d')
            
            # Select final columns for output (rename the specific date column back to 'date')
            # The output should contain 'date' (formatted) and all other data columns
            output_cols = ['date'] + cols_to_convert_to_numeric
            processed_chart_data_by_section[section_key] = df_section[output_cols].to_dict(orient='records')

        # --- Fetch Weather Data ---
        weather_worksheet = spreadsheet.worksheet(WEATHER_WORKSHEET_NAME)
        weather_data_raw = weather_worksheet.get_all_values()
        
        current_weather = {}
        if len(weather_data_raw) >= 9: # Check if enough rows for current weather
            current_weather['LA_WeatherStatus'] = weather_data_raw[0][1] if len(weather_data_raw[0]) > 1 else None
            current_weather['LA_WeatherIcon'] = weather_data_raw[1][1] if len(weather_data_raw[1]) > 1 else None
            current_weather['LA_Temperature'] = float(weather_data_raw[2][1]) if len(weather_data_raw[2]) > 1 and weather_data_raw[2][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Humidity'] = float(weather_data_raw[3][1]) if len(weather_data_raw[3]) > 1 and weather_data_raw[3][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_WindSpeed'] = float(weather_data_raw[4][1]) if len(weather_data_raw[4]) > 1 and weather_data_raw[4][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Pressure'] = float(weather_data_raw[5][1]) if len(weather_data_raw[5]) > 1 and weather_data_raw[5][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Visibility'] = float(weather_data_raw[6][1]) if len(weather_data_raw[6]) > 1 and weather_data_raw[6][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Sunrise'] = weather_data_raw[7][1] if len(weather_data_raw[7]) > 1 else None
            current_weather['LA_Sunset'] = weather_data_raw[8][1] if len(weather_data_raw[8]) > 1 else None
            # Assuming Fine Dust is not in the current image, add as None or a placeholder
            current_weather['LA_FineDust'] = None # Placeholder for fine dust if not in sheet

        forecast_weather = []
        if len(weather_data_raw) > 12: # Check if forecast data exists (starts from row 12, index 11)
            for row in weather_data_raw[11:]: # From row 12 onwards
                if len(row) >= 5 and row[0]: # Ensure date and basic info exist
                    forecast_day = {
                        'date': row[0],
                        'min_temp': float(row[1]) if row[1].replace('.', '', 1).isdigit() else None,
                        'max_temp': float(row[2]) if row[2].replace('.', '', 1).isdigit() else None,
                        'status': row[3],
                        'icon': row[4] # Assuming icon name or path
                    }
                    forecast_weather.append(forecast_day)
        
        print(f"DEBUG: Current Weather Data: {current_weather}")
        print(f"DEBUG: Forecast Weather Data (first 3): {forecast_weather[:3]}")

        # --- Fetch Exchange Rate Data ---
        exchange_rate_worksheet = spreadsheet.worksheet(EXCHANGE_RATE_WORKSHEET_NAME)
        exchange_rate_data_raw = exchange_rate_worksheet.get_all_values()

        exchange_rates = []
        # Assuming D2:E24 contains date and rate, so we start from row index 1 (D2) and take 2 columns
        # The image shows D2 to E24, so we need to parse from the 2nd row (index 1)
        # And columns D (index 3) and E (index 4)
        if len(exchange_rate_data_raw) > 1: # Ensure there's header and data
            for row_idx in range(1, len(exchange_rate_data_raw)):
                row = exchange_rate_data_raw[row_idx]
                if len(row) > 4 and row[3] and row[4]: # Ensure D and E columns exist and are not empty
                    try:
                        date_str = row[3].strip()
                        rate_val = float(row[4].strip().replace(',', '')) # Remove commas and convert to float
                        exchange_rates.append({'date': date_str, 'rate': rate_val})
                    except ValueError:
                        print(f"Warning: Could not parse exchange rate data for row {row_idx+1}: {row}")
                        continue
        
        # Sort exchange rates by date for chart
        exchange_rates.sort(key=lambda x: pd.to_datetime(x['date'], errors='coerce'))

        print(f"DEBUG: Exchange Rate Data (first 3): {exchange_rates[:3]}")

        # --- Fetch Table Data (from Crawling_Data2) ---
        crawling_data2_worksheet = spreadsheet.worksheet(CRAWLING_DATA2_WORKSHEET_NAME)
        raw_table_data_from_sheet = crawling_data2_worksheet.get_all_values()
        
        # Process the raw data from Crawling_Data2 into structured tables
        processed_table_data = process_table_data_from_crawling_data2(raw_table_data_from_sheet)
        print(f"DEBUG: Processed Table Data Keys: {processed_table_data.keys()}")

        # --- Combine all data into a single dictionary for JSON output ---
        final_output_data = {
            "chart_data": processed_chart_data_by_section, # This is the new structure
            "weather_data": {
                "current": current_weather,
                "forecast": forecast_weather
            },
            "exchange_rates": exchange_rates,
            "table_data": processed_table_data # Add the new table data here
        }

        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_output_data, f, ensure_ascii=False, indent=4)

        print(f"All data successfully saved to '{OUTPUT_JSON_PATH}'.")
        # Print sample of each section's data
        for section_key, data_list in processed_chart_data_by_section.items():
            print(f"Sample of saved chart data for {section_key} (first 3 entries): {data_list[:3]}")

    except Exception as e:
        print(f"Error during data processing: {e}")
        traceback.print_exc()

# Helper to find the row index of a marker string in a specific column
def find_marker_row(data, marker_string, column_index=0, start_row=0):
    for i in range(start_row, len(data)):
        row = data[i]
        # Ensure the cell exists and contains the marker string
        if len(row) > column_index and marker_string in str(row[column_index]):
            return i
    return -1

# Helper to safely get data for a row, padding if necessary
def get_row_data_for_table(row_data, start_col, end_col):
    values = []
    for col in range(start_col, end_col + 1):
        values.append(row_data[col] if len(row_data) > col else '')
    return values

def calculate_change_and_percentage(current_val_str, previous_val_str):
    """
    Calculates the weekly change and percentage change.
    Returns a tuple (change_value, percentage_string, color_class)
    """
    try:
        current_val = float(str(current_val_str).replace(',', '').strip()) if current_val_str else 0
        previous_val = float(str(previous_val_str).replace(',', '').strip()) if previous_val_str else 0

        change = current_val - previous_val
        
        if previous_val == 0:
            percentage = 0.0
        else:
            percentage = (change / previous_val) * 100

        color_class = "text-blue-500" if change < 0 else "text-red-500" if change > 0 else "text-gray-700"
        
        # Format change and percentage for display
        formatted_change = f"{change:,.0f}" # No decimal for change value
        formatted_percentage = f"{percentage:,.2f}%" # Two decimal places for percentage

        # Add arrow for positive/negative change
        if change > 0:
            formatted_change = f"▲{formatted_change}"
        elif change < 0:
            formatted_change = f"▼{formatted_change}"

        return formatted_change, formatted_percentage, color_class
    except ValueError:
        return "-", "-", "text-gray-700" # Return placeholder for non-numeric data
    except TypeError: # Handle None values
        return "-", "-", "text-gray-700"


def process_table_data_from_crawling_data2(raw_data):
    table_data = {}
    
    # --- KCCI Table ---
    # Find the row containing "KCCIGroupIndexMainlaneMainlane" to start parsing KCCI
    # This is often an empty cell or a merged cell that marks the start of a section.
    kcci_section_marker_idx = find_marker_row(raw_data, "KCCIGroupIndexMainlaneMainlane", column_index=0)
    
    if kcci_section_marker_idx != -1:
        # User-defined headers for the KCCI table
        kcci_display_headers = [
            "항로", "Current Index", "Previous Index", "Weekly Change"
        ]

        # Extract dates from A6 and A7, then format them
        # Assuming A6 is raw_data[kcci_section_marker_idx + 5][0]
        # Assuming A7 is raw_data[kcci_section_marker_idx + 6][0]
        
        # Adjust indices based on the provided image and its text.
        # "Current Index (2025-07-21)" is in row kcci_section_marker_idx + 5, column 0
        # "Previous Index (2025-07-14)" is in row kcci_section_marker_idx + 6, column 0
        
        current_index_raw_label = raw_data[kcci_section_marker_idx + 5][0] if len(raw_data) > kcci_section_marker_idx + 5 and len(raw_data[kcci_section_marker_idx + 5]) > 0 else ""
        previous_index_raw_label = raw_data[kcci_section_marker_idx + 6][0] if len(raw_data) > kcci_section_marker_idx + 6 and len(raw_data[kcci_section_marker_idx + 6]) > 0 else ""

        # Regex to extract date from "Current Index (YYYY-MM-DD)"
        date_pattern = r"\d{4}-\d{2}-\d{2}"
        
        current_date_match = re.search(date_pattern, current_index_raw_label)
        previous_date_match = re.search(date_pattern, previous_index_raw_label)

        current_date_formatted = ""
        if current_date_match:
            try:
                date_obj = datetime.strptime(current_date_match.group(0), "%Y-%m-%d")
                current_date_formatted = date_obj.strftime("%m-%d-%Y")
            except ValueError:
                pass # Keep empty if parsing fails

        previous_date_formatted = ""
        if previous_date_match:
            try:
                date_obj = datetime.strptime(previous_date_match.group(0), "%Y-%m-%d")
                previous_date_formatted = date_obj.strftime("%m-%d-%Y")
            except ValueError:
                pass # Keep empty if parsing fails

        # Adjust headers to include formatted dates
        kcci_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
        kcci_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

        kcci_rows = []
        
        # Define the routes and their corresponding column indices (0-indexed from the sheet)
        # Based on the image, the data for KCCI is in columns B to O (indices 1 to 14)
        # And rows are relative to kcci_section_marker_idx
        
        # Route names and their corresponding row indices relative to kcci_section_marker_idx + 1
        # (row 1 is "종합지수(Point)와 그 외 항로별($/FEU)")
        # (row 2 is "CodeKCCIKUWIKUEIKNEIKMDIKMEIKAUIKLEIKLWIKSAIKWAIKCIKJIKSEI")
        # (row 3 is "종합지수(Point)와 그 외 항로별($/FEU)Comprehensive IndexUSWCUSECEuropeMediterraneanMiddle EastAustraliaLatin America East CoastLatin America West CoastSouth AfricaWest AfricaChinaJapanSouth East Asia")
        # (row 4 is "Weight100%15%10%10%5%5%5%5%5%2.50%2.50%15%10%10%")
        # (row 5 is "Current Index (2025-07-21)2,2852,2963,9733,4103,8022,6241,8726,2122,6613,6714,721502241,042")
        # (row 6 is "Previous Index (2025-07-14)2,3942,5004,6793,2853,9252,6621,7876,3252,8803,6954,691502241,053")
        
        # The actual data starts from row kcci_section_marker_idx + 5 (Current Index row)
        # And data for each route is in columns 1 to 14 (B to O)
        
        # Mapping for the rows based on the image's visual structure
        # These are the labels in the first column of the table (e.g., "종합지수", "미주서안")
        # And their corresponding column index in the sheet for the data (e.g., 1 for "종합지수", 2 for "미주서안")
        kcci_routes_mapping = {
            "종합지수": 1, # Column B
            "미주서안": 2, # Column C
            "미주동안": 3, # Column D
            "유럽": 4, # Column E
            "지중해": 5, # Column F
            "중동": 6, # Column G
            "호주": 7, # Column H
            "남미동안": 8, # Column I
            "남미서안": 9, # Column J
            "남아프리카": 10, # Column K
            "서아프리카": 11, # Column L
            "중국": 12, # Column M
            "일본": 13, # Column N
            "동남아시아": 14 # Column O
        }

        # Iterate through each route to build the table rows
        for route_name, col_idx_in_sheet in kcci_routes_mapping.items():
            current_val_raw = raw_data[kcci_section_marker_idx + 5][col_idx_in_sheet] if len(raw_data) > kcci_section_marker_idx + 5 and len(raw_data[kcci_section_marker_idx + 5]) > col_idx_in_sheet else ''
            previous_val_raw = raw_data[kcci_section_marker_idx + 6][col_idx_in_sheet] if len(raw_data) > kcci_section_marker_idx + 6 and len(raw_data[kcci_section_marker_idx + 6]) > col_idx_in_sheet else ''

            change_value, percentage_string, color_class = calculate_change_and_percentage(current_val_raw, previous_val_raw)
            
            kcci_rows.append({
                "route": route_name,
                "current_index": current_val_raw,
                "previous_index": previous_val_raw,
                "weekly_change": {
                    "value": change_value,
                    "percentage": percentage_string,
                    "color_class": color_class
                }
            })
        
        table_data["KCCI"] = {"headers": kcci_display_headers, "rows": kcci_rows}
    else:
        print("WARNING: KCCI section marker not found in Crawling_Data2. Skipping KCCI table.")

    # --- SCFI Table ---
    # Find the row containing "SCFIDescription" to start parsing SCFI
    scfi_section_marker_idx = find_marker_row(raw_data, "SCFIDescription", column_index=0)
    if scfi_section_marker_idx != -1:
        scfi_display_headers = [
            "Description", "Comprehensive Index", "Europe (Base port)", "Mediterranean (Base port)",
            "USWC (Base port)", "USEC (Base port)", "Persian Gulf and Red Sea (Dubai)",
            "Australia/New Zealand (Melbourne)", "East/West Africa (Lagos)", "South Africa (Durban)",
            "West Japan (Base port)", "East Japan (Base port)", "Southeast Asia (Singapore)",
            "Korea (Pusan)", "Central/South America West Coast(Manzanillo)"
        ]
        
        # Data rows for SCFI start relative to scfi_section_marker_idx
        # Weighting is at scfi_section_marker_idx + 1, col 0
        # Current Index is at scfi_section_marker_idx + 2, col 0
        # Previous Index is at scfi_section_marker_idx + 3, col 0
        # Compare With Last Week is at scfi_section_marker_idx + 4, col 0

        scfi_rows = []
        # Get the actual data row for Current Index, Previous Index etc.
        # The data starts from column 1 (Comprehensive Index) to 14 (Central/South America West Coast(Manzanillo))
        scfi_data_start_col = 1
        scfi_data_end_col = 14

        # Extracting the full rows for processing
        scfi_current_data_row = raw_data[scfi_section_marker_idx + 2] if len(raw_data) > scfi_section_marker_idx + 2 else []
        scfi_previous_data_row = raw_data[scfi_section_marker_idx + 3] if len(raw_data) > scfi_section_marker_idx + 3 else []
        scfi_compare_data_row = raw_data[scfi_section_marker_idx + 4] if len(raw_data) > scfi_section_marker_idx + 4 else []

        # Assuming the first value in the 'Current Index' row is the Comprehensive Index value
        # and the 'Compare With Last Week' row has the calculated change.
        
        # For SCFI, the "Comprehensive Index" is directly in the first data column (index 1)
        # The "Compare With Last Week" is also directly in the first data column (index 1)
        # This structure is different from KCCI where we calculate it.
        
        # We need to adapt the SCFI parsing to match the provided image's structure.
        # The image shows 'Current Index (2025-07-18) 1,647' in A23, B23, etc.
        # And 'Compare With Last Week -86.39' in A25, B25, etc.
        # So the change is already calculated in the sheet.

        # The headers for SCFI are in row 19 (index 18)
        # "Description", "Comprehensive Index", "Europe (Base port)" ...
        # Data starts from row 20 (index 19)
        
        # Let's re-align to the provided image's SCFI section.
        # The SCFI data starts after "SCFIDescription" which is at raw_data[scfi_start_idx][0]
        # The headers are in the same row as "SCFIDescription" but in subsequent columns.
        # The "Weighting" row is scfi_start_idx + 1
        # The "Current Index (2025-07-18)" is scfi_start_idx + 2
        # The "Previous Index (2025-07-11)" is scfi_start_idx + 3
        # The "Compare With Last Week" is scfi_start_idx + 4

        # The headers are actually in the row that contains "Description" (scfi_start_idx)
        # starting from column 1.
        scfi_actual_headers = [str(raw_data[scfi_start_idx][j]).strip() for j in range(1, len(raw_data[scfi_start_idx])) if raw_data[scfi_start_idx][j]]
        scfi_display_headers = ["Category"] + scfi_actual_headers
        
        scfi_rows.append(["Weighting"] + get_row_data_for_table(raw_data[scfi_start_idx + 1], scfi_data_start_col, scfi_data_end_col))
        scfi_rows.append([raw_data[scfi_start_idx + 2][0]] + get_row_data_for_table(raw_data[scfi_start_idx + 2], scfi_data_start_col, scfi_data_end_col))
        scfi_rows.append([raw_data[scfi_start_idx + 3][0]] + get_row_data_for_table(raw_data[scfi_start_idx + 3], scfi_data_start_col, scfi_data_end_col))
        
        # For "Compare With Last Week", the values are already calculated in the sheet.
        # We need to extract them and apply color based on the sign.
        compare_row_values = get_row_data_for_table(raw_data[scfi_start_idx + 4], scfi_data_start_col, scfi_data_end_col)
        processed_compare_values = []
        for val_str in compare_row_values:
            val_str = str(val_str).strip()
            color_class = "text-gray-700"
            if '▲' in val_str:
                color_class = "text-red-500"
            elif '▼' in val_str:
                color_class = "text-blue-500"
            processed_compare_values.append({"value": val_str, "color_class": color_class})

        scfi_rows.append({"Category": raw_data[scfi_start_idx + 4][0], "values": processed_compare_values})

        table_data["SCFI"] = {"headers": scfi_display_headers, "rows": scfi_rows}
    else:
        print("WARNING: SCFI section marker not found in Crawling_Data2. Skipping SCFI table.")

    # --- SCFI2 Table ---
    scfi2_start_idx = find_marker_row(raw_data, "SCFI2종합지수($/TEU)", column_index=0)
    if scfi2_start_idx != -1:
        scfi2_display_headers = [
            "Category", "Comprehensive Index", "USWC (Base port)", "USEC (Base port)",
            "Europe (Base port)", "Mediterranean (Base port)", "Southeast Asia (Singapore)",
            "Persian Gulf and Red Sea (Dubai)", "Australia/New Zealand (Melbourne)",
            "South America (Santos)", "West Japan (Base port)", "East Japan (Base port)",
            "Korea (Pusan)", "East/West Africa (Lagos)", "South Africa (Durban)"
        ]

        scfi2_current_row_idx = find_marker_row(raw_data, "Current Index", column_index=0, start_row=scfi2_start_idx)
        scfi2_previous_row_idx = find_marker_row(raw_data, "Previous Index", column_index=0, start_row=scfi2_start_idx)
        scfi2_compare_row_idx = find_marker_row(raw_data, "Compare With Last Week", column_index=0, start_row=scfi2_start_idx)

        scfi2_rows = []
        if all(idx != -1 for idx in [scfi2_current_row_idx, scfi2_previous_row_idx, scfi2_compare_row_idx]):
            scfi2_data_start_col = 1
            scfi2_data_end_col = 14 

            scfi2_rows.append([raw_data[scfi2_current_row_idx][0]] + get_row_data_for_table(raw_data[scfi2_current_row_idx], scfi2_data_start_col, scfi2_data_end_col))
            scfi2_rows.append([raw_data[scfi2_previous_row_idx][0]] + get_row_data_for_table(raw_data[scfi2_previous_row_idx], scfi2_data_start_col, scfi2_data_end_col))
            
            # For "Compare With Last Week", extract and apply color
            compare_row_values = get_row_data_for_table(raw_data[scfi2_compare_row_idx], scfi2_data_start_col, scfi2_data_end_col)
            processed_compare_values = []
            for val_str in compare_row_values:
                val_str = str(val_str).strip()
                color_class = "text-gray-700"
                if '▲' in val_str:
                    color_class = "text-red-500"
                elif '▼' in val_str:
                    color_class = "text-blue-500"
                processed_compare_values.append({"value": val_str, "color_class": color_class})
            scfi2_rows.append({"Category": raw_data[scfi2_compare_row_idx][0], "values": processed_compare_values})
            
            table_data["SCFI2"] = {"headers": scfi2_display_headers, "rows": scfi2_rows}
        else:
            print("WARNING: Incomplete SCFI2 data found in Crawling_Data2. Skipping SCFI2 table.")


    # --- CCFI Table ---
    ccfi_start_idx = find_marker_row(raw_data, "CCFI종합지수와 각 항로별(Point)", column_index=0)
    if ccfi_start_idx != -1:
        ccfi_display_headers = [
            "Category", "COMPOSITE INDEX", "JAPAN", "EUROPE", "W/C AMERICA", "E/C AMERIC",
            "KOREA", "SOUTHEAST", "MEDITERRANEAN", "AUSTRALIA/NEW ZEALAND",
            "SOUTH AFRICA", "SOUTH AMERICA", "WEST EAST AFRICA", "PERSIAN GULF/RED SEA"
        ]

        ccfi_current_row_idx = find_marker_row(raw_data, "Current Index", column_index=0, start_row=ccfi_start_idx)
        ccfi_previous_row_idx = find_marker_row(raw_data, "Previous Index", column_index=0, start_row=ccfi_start_idx)
        ccfi_weekly_growth_row_idx = find_marker_row(raw_data, "Weekly Growth (%)", column_index=0, start_row=ccfi_start_idx)

        ccfi_rows = []
        if all(idx != -1 for idx in [ccfi_current_row_idx, ccfi_previous_row_idx, ccfi_weekly_growth_row_idx]):
            ccfi_data_start_col = 1
            ccfi_data_end_col = 13 

            ccfi_rows.append([raw_data[ccfi_current_row_idx][0]] + get_row_data_for_table(raw_data[ccfi_current_row_idx], ccfi_data_start_col, ccfi_data_end_col))
            ccfi_rows.append([raw_data[ccfi_previous_row_idx][0]] + get_row_data_for_table(raw_data[ccfi_previous_row_idx], ccfi_data_start_col, ccfi_data_end_col))
            
            # For "Weekly Growth (%)", extract and apply color
            weekly_growth_values = get_row_data_for_table(raw_data[ccfi_weekly_growth_row_idx], ccfi_data_start_col, ccfi_data_end_col)
            processed_weekly_growth_values = []
            for val_str in weekly_growth_values:
                val_str = str(val_str).strip()
                color_class = "text-gray-700"
                # Check for percentage sign and parse value for color
                if '%' in val_str:
                    try:
                        numeric_val = float(val_str.replace('%', '').strip())
                        if numeric_val < 0:
                            color_class = "text-blue-500"
                        elif numeric_val > 0:
                            color_class = "text-red-500"
                    except ValueError:
                        pass # Keep default color if parsing fails
                processed_weekly_growth_values.append({"value": val_str, "color_class": color_class})
            ccfi_rows.append({"Category": raw_data[ccfi_weekly_growth_row_idx][0], "values": processed_weekly_growth_values})
            
            table_data["CCFI"] = {"headers": ccfi_display_headers, "rows": ccfi_rows}
        else:
            print("WARNING: Incomplete CCFI data found in Crawling_Data2. Skipping CCFI table.")

    # --- WCI Table ---
    wci_table_start_idx = find_marker_row(raw_data, "WCI종합지수와 각 항로별($/FEU)", column_index=0)
    if wci_table_start_idx != -1:
        wci_display_headers = [
            "Category", "Composite Index", "Shanghai-Rotterdam", "Rotterdam-Shanghai",
            "Shanghai-Genoa", "Shanghai-LosAngeles", "LosAngeles-Shanghai",
            "Shanghai-NewYork", "NewYork-Rotterdam", "Rotterdam-NewYork"
        ]

        wci_current_row_idx = find_marker_row(raw_data, "17-Jul-25", column_index=0, start_row=wci_table_start_idx)
        wci_weekly_row_idx = find_marker_row(raw_data, "Weekly(%)", column_index=0, start_row=wci_table_start_idx)
        wci_annual_row_idx = find_marker_row(raw_data, "Annual(%)", column_index=0, start_row=wci_table_start_idx)
        wci_previous_row_idx = find_marker_row(raw_data, "10-Jul-25", column_index=0, start_row=wci_table_start_idx)

        wci_rows = []
        if all(idx != -1 for idx in [wci_current_row_idx, wci_weekly_row_idx, wci_annual_row_idx, wci_previous_row_idx]):
            wci_data_start_col = 1
            wci_data_end_col = 9 

            wci_rows.append([raw_data[wci_current_row_idx][0]] + get_row_data_for_table(raw_data[wci_current_row_idx], wci_data_start_col, wci_data_end_col))
            
            # For percentage rows, extract and apply color
            weekly_values = get_row_data_for_table(raw_data[wci_weekly_row_idx], wci_data_start_col, wci_data_end_col)
            processed_weekly_values = []
            for val_str in weekly_values:
                val_str = str(val_str).strip()
                color_class = "text-gray-700"
                if '%' in val_str:
                    try:
                        numeric_val = float(val_str.replace('%', '').strip())
                        if numeric_val < 0:
                            color_class = "text-blue-500"
                        elif numeric_val > 0:
                            color_class = "text-red-500"
                    except ValueError:
                        pass
                processed_weekly_values.append({"value": val_str, "color_class": color_class})
            wci_rows.append({"Category": raw_data[wci_weekly_row_idx][0], "values": processed_weekly_values})

            annual_values = get_row_data_for_table(raw_data[wci_annual_row_idx], wci_data_start_col, wci_data_end_col)
            processed_annual_values = []
            for val_str in annual_values:
                val_str = str(val_str).strip()
                color_class = "text-gray-700"
                if '%' in val_str:
                    try:
                        numeric_val = float(val_str.replace('%', '').strip())
                        if numeric_val < 0:
                            color_class = "text-blue-500"
                        elif numeric_val > 0:
                            color_class = "text-red-500"
                    except ValueError:
                        pass
                processed_annual_values.append({"value": val_str, "color_class": color_class})
            wci_rows.append({"Category": raw_data[wci_annual_row_idx][0], "values": processed_annual_values})

            wci_rows.append([raw_data[wci_previous_row_idx][0]] + get_row_data_for_table(raw_data[wci_previous_row_idx], wci_data_start_col, wci_data_end_col))
            
            table_data["WCI"] = {"headers": wci_display_headers, "rows": wci_rows}
        else:
            print("WARNING: Incomplete WCI data found in Crawling_Data2. Skipping WCI table.")

    # --- IACI Table ---
    iaci_table_start_idx = find_marker_row(raw_data, "IACIdate", column_index=0)
    if iaci_table_start_idx != -1:
        iaci_display_headers = ["Date", "US$/40ft"]

        iaci_current_row_idx = find_marker_row(raw_data, "07/15/2025", column_index=0, start_row=iaci_table_start_idx)
        iaci_previous_row_idx = find_marker_row(raw_data, "06/30/2025", column_index=0, start_row=iaci_table_start_idx)

        iaci_rows = []
        if all(idx != -1 for idx in [iaci_current_row_idx, iaci_previous_row_idx]):
            iaci_data_start_col = 1
            iaci_data_end_col = 1 

            iaci_rows.append([raw_data[iaci_current_row_idx][0]] + get_row_data_for_table(raw_data[iaci_current_row_idx], iaci_data_start_col, iaci_data_end_col))
            iaci_rows.append([raw_data[iaci_previous_row_idx][0]] + get_row_data_for_table(raw_data[iaci_previous_row_idx], iaci_data_start_col, iaci_data_end_col))
            
            table_data["IACI_Table"] = {"headers": iaci_display_headers, "rows": iaci_rows}
        else:
            print("WARNING: Incomplete IACI data found in Crawling_Data2. Skipping IACI table.")

    # --- BLANK SAILING Table ---
    blank_sailing_table_start_idx = find_marker_row(raw_data, "BLANK SAILING", column_index=0)
    if blank_sailing_table_start_idx != -1:
        blank_sailing_display_headers = [
            "Index", "Gemini Cooperation", "MSC", "OCEAN Alliance", "Premier Alliance", "Others/Independent", "Total"
        ]

        blank_sailing_current_row_idx = find_marker_row(raw_data, "07/18/2025", column_index=0, start_row=blank_sailing_table_start_idx)
        blank_sailing_previous_row_idx = find_marker_row(raw_data, "07/11/2025", column_index=0, start_row=blank_sailing_table_start_idx)

        blank_sailing_rows = []
        if all(idx != -1 for idx in [blank_sailing_current_row_idx, blank_sailing_previous_row_idx]):
            blank_sailing_data_start_col = 1
            blank_sailing_data_end_col = 6 

            blank_sailing_rows.append([raw_data[blank_sailing_current_row_idx][0]] + get_row_data_for_table(raw_data[blank_sailing_current_row_idx], blank_sailing_data_start_col, blank_sailing_data_end_col))
            blank_sailing_rows.append([raw_data[blank_sailing_previous_row_idx][0]] + get_row_data_for_table(raw_data[blank_sailing_previous_row_idx], blank_sailing_data_start_col, blank_sailing_data_end_col))
            
            table_data["BLANK_SAILING_Table"] = {"headers": blank_sailing_display_headers, "rows": blank_sailing_rows}
        else:
            print("WARNING: Incomplete BLANK SAILING data found in Crawling_Data2. Skipping BLANK SAILING table.")

    # --- FBX Table ---
    fbx_table_start_idx = find_marker_row(raw_data, "FBX종합지수와 각 항로별($/FEU)", column_index=0)
    if fbx_table_start_idx != -1:
        fbx_display_headers = [
            "Date", "Global Container Freight Index", "China/East Asia - North America West Coast",
            "North America West Coast - China/East Asia", "China/East Asia - North America East Coast",
            "North America East Coast - China/East Asia", "China/East Asia - North Europe",
            "North Europe - China/East Asia", "China/East Asia - Mediterranean",
            "Mediterranean - China/East Asia", "North America East Coast - North Europe",
            "North Europe - North America East Coast", "Europe - South America East Coast",
            "Europe - South America West Coast"
        ]

        fbx_current_row_idx = find_marker_row(raw_data, "2025-07-18", column_index=0, start_row=fbx_table_start_idx)
        fbx_previous_row_idx = find_marker_row(raw_data, "2025-07-11", column_index=0, start_row=fbx_table_start_idx)

        fbx_rows = []
        if all(idx != -1 for idx in [fbx_current_row_idx, fbx_previous_row_idx]):
            fbx_data_start_col = 1
            fbx_data_end_col = 13 

            fbx_rows.append([raw_data[fbx_current_row_idx][0]] + get_row_data_for_table(raw_data[fbx_current_row_idx], fbx_data_start_col, fbx_data_end_col))
            fbx_rows.append([raw_data[fbx_previous_row_idx][0]] + get_row_data_for_table(raw_data[fbx_previous_row_idx], fbx_data_start_col, fbx_data_end_col))
            
            table_data["FBX"] = {"headers": fbx_display_headers, "rows": fbx_rows}
        else:
            print("WARNING: Incomplete FBX data found in Crawling_Data2. Skipping FBX table.")

    # --- XSI Table ---
    xsi_table_start_idx = find_marker_row(raw_data, "XSI각 항로별($/FEU)", column_index=0)
    if xsi_table_start_idx != -1:
        xsi_display_headers = [
            "Category", "Far East - N. Europe", "N. Europe - Far East",
            "Far East - USWC", "USWC - Far East", "Far East - SAEC",
            "N. Europe - USEC", "USEC - N. Europe", "N. Europe - SAEC"
        ]

        xsi_current_row_idx = find_marker_row(raw_data, "07-22-2025", column_index=0, start_row=xsi_table_start_idx)
        xsi_weekly_row_idx = find_marker_row(raw_data, "WoW(%)", column_index=0, start_row=xsi_table_start_idx)
        xsi_monthly_row_idx = find_marker_row(raw_data, "MoM(%)", column_index=0, start_row=xsi_table_start_idx)

        xsi_rows = []
        if all(idx != -1 for idx in [xsi_current_row_idx, xsi_weekly_row_idx, xsi_monthly_row_idx]):
            xsi_data_start_col = 1
            xsi_data_end_col = 8 

            xsi_rows.append([raw_data[xsi_current_row_idx][0]] + get_row_data_for_table(raw_data[xsi_current_row_idx], xsi_data_start_col, xsi_data_end_col))
            
            # For percentage rows, extract and apply color
            weekly_values = get_row_data_for_table(raw_data[xsi_weekly_row_idx], xsi_data_start_col, xsi_data_end_col)
            processed_weekly_values = []
            for val_str in weekly_values:
                val_str = str(val_str).strip()
                color_class = "text-gray-700"
                if '▲' in val_str:
                    color_class = "text-red-500"
                elif '▼' in val_str:
                    color_class = "text-blue-500"
                processed_weekly_values.append({"value": val_str, "color_class": color_class})
            xsi_rows.append({"Category": raw_data[xsi_weekly_row_idx][0], "values": processed_weekly_values})

            monthly_values = get_row_data_for_table(raw_data[xsi_monthly_row_idx], xsi_data_start_col, xsi_data_end_col)
            processed_monthly_values = []
            for val_str in monthly_values:
                val_str = str(val_str).strip()
                color_class = "text-gray-700"
                if '▲' in val_str:
                    color_class = "text-red-500"
                elif '▼' in val_str:
                    color_class = "text-blue-500"
                processed_monthly_values.append({"value": val_str, "color_class": color_class})
            xsi_rows.append({"Category": raw_data[xsi_monthly_row_idx][0], "values": processed_monthly_values})
            
            table_data["XSI"] = {"headers": xsi_display_headers, "rows": xsi_rows}
        else:
            print("WARNING: Incomplete XSI data found in Crawling_Data2. Skipping XSI table.")

    # --- MBCI Table ---
    mbci_table_start_idx = find_marker_row(raw_data, "MBCIIndex(종합지수), $/day(정기용선, Time charter)", column_index=0)
    if mbci_table_start_idx != -1:
        mbci_display_headers = ["Category", "Index(종합지수)", "$/day(정기용선, Time charter)"]

        mbci_current_row_idx = find_marker_row(raw_data, "Latest", column_index=0, start_row=mbci_table_start_idx)
        mbci_previous_row_idx = find_marker_row(raw_data, "2025-07-11", column_index=0, start_row=mbci_table_start_idx)

        mbci_rows = []
        if all(idx != -1 for idx in [mbci_current_row_idx, mbci_previous_row_idx]):
            mbci_data_start_col = 1
            mbci_data_end_col = 2 

            mbci_rows.append([raw_data[mbci_current_row_idx][0]] + get_row_data_for_table(raw_data[mbci_current_row_idx], mbci_data_start_col, mbci_data_end_col))
            mbci_rows.append([raw_data[mbci_previous_row_idx][0]] + get_row_data_for_table(raw_data[mbci_previous_row_idx], mbci_data_start_col, mbci_data_end_col))
            
            table_data["MBCI_Table"] = {"headers": mbci_display_headers, "rows": mbci_rows}
        else:
            print("WARNING: Incomplete MBCI data found in Crawling_Data2. Skipping MBCI table.")

    return table_data

if __name__ == "__main__":
    fetch_and_process_data()
