import gspread
import json
import os
import pandas as pd
import traceback
import re
import numpy as np # Import numpy for type checking

# Custom JSON encoder to handle NumPy types
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

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
OUTPUT_JSON_PATH = "data/crawling_data.json"

# --- Header Mapping Definitions ---
# This dictionary now defines the exact column indices in the Google Sheet
# and their corresponding final JSON key names for each section.
# This makes the mapping explicit and less prone to inference errors.
# The 'data_start_col_idx' is the 0-indexed column number where the data for this section starts.
# The 'data_cols_map' maps raw header names (from the main header row) to final JSON keys.
SECTION_COLUMN_MAPPINGS = {
    "KCCI": {
        "data_start_col_idx": 1, # KCCI data starts from '종합지수' at index 1
        "data_cols_map": {
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
            "동남아시아": "Southeast_Asia"
        }
    },
    "SCFI": {
        "data_start_col_idx": 17, # SCFI data starts from '종합지수' at index 17
        "data_cols_map": {
            "종합지수": "Composite_Index_1",
            "미주서안": "US_West_Coast_1",
            "미주동안": "US_East_Coast_1",
            "북유럽": "North_Europe",
            "지중해": "Mediterranean_1",
            "동남아시아": "Southeast_Asia_1",
            "중동": "Middle_East_1",
            "호주/뉴질랜드": "Australia_New_Zealand_SCFI",
            "남아메리카": "South_America_SCFI",
            "일본서안": "Japan_West_Coast_SCFI",
            "일본동안": "Japan_East_Coast_SCFI",
            "한국": "Korea_SCFI",
            "동부/서부 아프리카": "East_West_Africa_SCFI",
            "남아공": "South_Africa_SCFI"
        }
    },
    "WCI": {
        "data_start_col_idx": 34, # WCI data starts from '종합지수' at index 34 (Corrected from 33)
        "data_cols_map": {
            "종합지수": "Composite_Index_2",
            "상하이 → 로테르담": "Shanghai_Rotterdam_WCI",
            "로테르담 → 상하이": "Rotterdam_Shanghai_WCI",
            "상하이 → 제노바": "Shanghai_Genoa_WCI",
            "상하이 → 로스엔젤레스": "Shanghai_Los_Angeles_WCI",
            "로스엔젤레스 → 상하이": "Los_Angeles_Shanghai_WCI",
            "상하이 → 뉴욕": "Shanghai_New_York_WCI",
            "뉴욕 → 로테르담": "New_York_Rotterdam_WCI",
            "로테르담 → 뉴욕": "Rotterdam_New_York_WCI",
        }
    },
    "IACI": {
        "data_start_col_idx": 45, # IACI data starts from '종합지수' at index 45 (Corrected from 44)
        "data_cols_map": {
            "종합지수": "Composite_Index_3"
        }
    },
    "BLANK_SAILING": {
        "data_start_col_idx": 48, # BLANK_SAILING data starts from 'Gemini Cooperation' at index 48
        "data_cols_map": {
            "Gemini Cooperation": "Gemini_Cooperation_Blank_Sailing",
            "MSC": "MSC_Alliance_Blank_Sailing",
            "OCEAN Alliance": "OCEAN_Alliance_Blank_Sailing",
            "Premier Alliance": "Premier_Alliance_Blank_Sailing",
            "Others/Independent": "Others_Independent_Blank_Sailing",
            "Total": "Total_Blank_Sailings"
        }
    },
    "FBX": {
        "data_start_col_idx": 56, # FBX data starts from '종합지수' at index 56 (Corrected from 55)
        "data_cols_map": {
            "종합지수": "Composite_Index_4",
            "중국/동아시아 → 미주서안": "China_EA_US_West_Coast_FBX",
            "미주서안 → 중국/동아시아": "US_West_Coast_China_EA_FBX",
            "중국/동아시아 → 미주동안": "China_EA_US_East_Coast_FBX",
            "미주동안 → 중국/동아시아": "US_East_Coast_China_EA_FBX",
            "중국/동아시아 → 북유럽": "China_EA_North_Europe_FBX",
            "북유럽 → 중국/동아시아": "North_Europe_China_EA_FBX",
            "중국/동아시아 → 지중해": "China_EA_Mediterranean_FBX",
            "지중해 → 중국/동아시아": "Mediterranean_China_EA_FBX",
        }
    },
    "XSI": {
        "data_start_col_idx": 71, # XSI data starts from '동아시아 → 북유럽' at index 71 (Corrected from 69)
        "data_cols_map": {
            "동아시아 → 북유럽": "XSI_East_Asia_North_Europe",
            "북유럽 → 동아시아": "XSI_North_Europe_East_Asia",
            "동아시아 → 미주서안": "XSI_East_Asia_US_West_Coast",
            "미주서안 → 동아시아": "XSI_US_West_Coast_East_Asia",
            "동아시아 → 남미동안": "XSI_East_Asia_South_America_East_Coast",
            "북유럽 → 미주동안": "XSI_North_Europe_US_East_Coast",
            "미주동안 → 북유럽": "XSI_US_East_Coast_North_Europe",
            "북유럽 → 남미동안": "XSI_North_Europe_South_America_East_Coast"
        }
    },
    "MBCI": {
        "data_start_col_idx": 81, # MBCI data starts from 'MBCI' at index 81 (Corrected from 80)
        "data_cols_map": {
            "MBCI": "MBCI_MBCI_Value",
        }
    }
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
        
        # --- Fetch Main Chart Data ---
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        all_data = worksheet.get_all_values()

        print(f"DEBUG: Total rows fetched from Google Sheet (raw): {len(all_data)}")

        if not all_data:
            print("Error: No data fetched from the main chart sheet.")
            return

        # Find the main header row (the one containing '날짜' or 'Date' for the first section)
        main_header_row_index = -1
        for i, row in enumerate(all_data):
            # Check for '날짜' or 'date' in the row, case-insensitive and stripped
            if any(str(cell).strip().lower() in ["날짜", "date"] for cell in row):
                main_header_row_index = i
                break
        
        print(f"DEBUG: Main chart data header row index: {main_header_row_index}")

        if main_header_row_index == -1:
            print("Error: Could not find a suitable header row containing '날짜' or 'Date' in main chart data.")
            return

        # Get raw headers from the identified header row
        raw_headers_full = [str(h).strip() for h in all_data[main_header_row_index]]
        print(f"DEBUG: Raw headers from Google Sheet (full row): {raw_headers_full}")

        processed_chart_data_by_section = {}
        processed_table_data_by_section = {}

        # Store the universal date column data (always from the first column of the main data)
        universal_date_column = []
        if len(all_data) > main_header_row_index + 1:
            universal_date_column = [str(row[0]).strip() for row in all_data[main_header_row_index + 1:] if len(row) > 0 and str(row[0]).strip()]
        
        # Parse universal date column once
        parsed_universal_dates = pd.to_datetime(universal_date_column, errors='coerce')
        # Filter out NaT values and corresponding original date strings
        valid_universal_dates_mask = parsed_universal_dates.notna()
        universal_date_column_filtered = [universal_date_column[i] for i, is_valid in enumerate(valid_universal_dates_mask) if is_valid]
        parsed_universal_dates_filtered = parsed_universal_dates[valid_universal_dates_mask]

        # Create a DataFrame for universal dates to join with later
        universal_date_df = pd.DataFrame({
            'date': universal_date_column_filtered,
            'parsed_date': parsed_universal_dates_filtered
        }).sort_values(by='parsed_date', ascending=True)
        
        print(f"DEBUG: Universal Date Column (first 10 entries after parsing/filtering): {universal_date_df['date'].tolist()[:10]}")
        print(f"DEBUG: Universal Date DataFrame shape: {universal_date_df.shape}")


        # Iterate through each section and extract its specific data
        for section_key, details in SECTION_COLUMN_MAPPINGS.items():
            data_cols_map = details["data_cols_map"]
            
            cols_to_extract_indices = []
            section_df_columns = []

            # Find indices for data columns based on their raw header names, starting from data_start_col_idx
            for raw_header_name, final_json_key in data_cols_map.items():
                found_idx = -1
                # Search for the header starting from the section's defined start column
                for idx in range(details["data_start_col_idx"], len(raw_headers_full)):
                    if raw_headers_full[idx] == raw_header_name:
                        found_idx = idx
                        break
                
                if found_idx != -1:
                    cols_to_extract_indices.append(found_idx)
                    section_df_columns.append(final_json_key)
                else:
                    print(f"WARNING: Raw header '{raw_header_name}' not found in its expected section range for '{section_key}'. Skipping this column.")

            if not cols_to_extract_indices:
                print(f"WARNING: No valid data columns found for section {section_key}. Skipping chart and table data for this section.")
                processed_chart_data_by_section[section_key] = []
                processed_table_data_by_section[section_key] = {"headers": [], "rows": []}
                continue

            section_raw_rows = []
            for row_idx in range(main_header_row_index + 1, len(all_data)):
                row_data = all_data[row_idx]
                extracted_row = []
                for col_idx in cols_to_extract_indices:
                    if col_idx < len(row_data):
                        extracted_row.append(str(row_data[col_idx]).strip())
                    else:
                        extracted_row.append('')
                section_raw_rows.append(extracted_row)
            
            if not section_raw_rows:
                print(f"WARNING: No data rows found for section {section_key}. Skipping chart and table data for this section.")
                processed_chart_data_by_section[section_key] = []
                processed_table_data_by_section[section_key] = {"headers": [], "rows": []}
                continue

            # Create a DataFrame for this section's data columns
            if len(section_raw_rows[0]) != len(section_df_columns):
                 print(f"ERROR: Mismatch in column count for section {section_key}. Expected {len(section_df_columns)} but got {len(section_raw_rows[0])} in first row.")
                 print(f"DEBUG: section_df_columns: {section_df_columns}")
                 print(f"DEBUG: section_raw_rows[0]: {section_raw_rows[0]}")
                 processed_chart_data_by_section[section_key] = []
                 processed_table_data_by_section[section_key] = {"headers": [], "rows": []}
                 continue

            df_section_data = pd.DataFrame(section_raw_rows, columns=section_df_columns)
            print(f"DEBUG: Initial DataFrame for {section_key} shape: {df_section_data.shape}")
            print(f"DEBUG: Initial DataFrame for {section_key} head:\n{df_section_data.head()}")

            # Convert numeric columns for this section
            for col in section_df_columns:
                df_section_data[col] = pd.to_numeric(df_section_data[col].astype(str).str.replace(',', ''), errors='coerce')
            
            df_section_data = df_section_data.replace({pd.NA: None, float('nan'): None})

            # Join with the universal date DataFrame
            # Ensure that the length of df_section_data matches universal_date_df for proper alignment
            min_len = min(len(universal_date_df), len(df_section_data))
            df_section = universal_date_df.iloc[:min_len].copy()
            for col in df_section_data.columns:
                df_section[col] = df_section_data[col].iloc[:min_len].values

            print(f"DEBUG: DataFrame shape for {section_key} after joining dates: {df_section.shape}")
            print(f"DEBUG: DataFrame for {section_key} after joining dates head:\n{df_section.head()}")

            df_section['date'] = df_section['parsed_date'].dt.strftime('%Y-%m-%d')
            
            output_chart_cols = ['date'] + section_df_columns
            
            chart_data_records = []
            for record in df_section[output_chart_cols].to_dict(orient='records'):
                new_record = {}
                for k, v in record.items():
                    # Convert numpy types to native Python types
                    if isinstance(v, (np.integer, np.floating)):
                        new_record[k] = v.item() # Use .item() to get native Python scalar
                    elif pd.isna(v): # Check for pandas NaN/NaT
                        new_record[k] = None
                    else:
                        new_record[k] = v
                chart_data_records.append(new_record)

            processed_chart_data_by_section[section_key] = chart_data_records
            print(f"DEBUG: Processed chart data for {section_key} (first 3 entries): {processed_chart_data_by_section[section_key][:3]}")

            # Prepare table data for this section
            # Table headers should come from the original mapping keys (Korean names)
            table_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"] # Changed "날짜" to "항로" for consistency
            table_rows_data = []

            if not df_section.empty:
                latest_row_data_only = df_section.iloc[-1].to_dict()
                second_latest_row_data_only = df_section.iloc[-2].to_dict() if len(df_section) > 1 else {}

                for raw_header_name, final_json_key in data_cols_map.items():
                    current_index_val = latest_row_data_only.get(final_json_key)
                    previous_index_val = second_latest_row_data_only.get(final_json_key)
                    
                    # Explicitly convert numpy numeric types to native Python types for table data
                    if isinstance(current_index_val, (np.integer, np.floating)):
                        current_index_val = current_index_val.item()
                    elif pd.isna(current_index_val):
                        current_index_val = None

                    if isinstance(previous_index_val, (np.integer, np.floating)):
                        previous_index_val = previous_index_val.item()
                    elif pd.isna(previous_index_val):
                        previous_index_val = None

                    weekly_change = None
                    if current_index_val is not None and previous_index_val is not None and previous_index_val != 0:
                        change_value = current_index_val - previous_index_val
                        change_percentage = (change_value / previous_index_val) * 100
                        color_class = "text-gray-700"
                        if change_value > 0:
                            color_class = "text-red-500"
                        elif change_value < 0:
                            color_class = "text-blue-500"
                        
                        weekly_change = {
                            "value": f"{change_value:.2f}",
                            "percentage": f"{change_percentage:.2f}%",
                            "color_class": color_class
                        }
                    
                    table_rows_data.append({
                        "route": raw_header_name, # Use the original raw header name (Korean) for the table route
                        "current_index": current_index_val,
                        "previous_index": previous_index_val,
                        "weekly_change": weekly_change
                    })
            
            processed_table_data_by_section[section_key] = {
                "headers": table_headers,
                "rows": table_rows_data
            }
            print(f"DEBUG: Processed table data for {section_key} (first 3 entries): {processed_table_data_by_section[section_key]['rows'][:3]}")


        # --- Fetch Weather Data ---
        weather_worksheet = spreadsheet.worksheet(WEATHER_WORKSHEET_NAME)
        weather_data_raw = weather_worksheet.get_all_values()
        
        current_weather = {}
        if len(weather_data_raw) >= 9: # Check if enough rows for current weather
            current_weather['LA_WeatherStatus'] = weather_data_raw[0][1] if len(weather_data_raw[0]) > 1 else None
            current_weather['LA_WeatherIcon'] = weather_data_raw[1][1] if len(weather_data_raw[1]) > 1 else None
            # Ensure numeric conversion for temperature, humidity, etc.
            current_weather['LA_Temperature'] = float(weather_data_raw[2][1]) if len(weather_data_raw[2]) > 1 and weather_data_raw[2][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Humidity'] = float(weather_data_raw[3][1]) if len(weather_data_raw[3]) > 1 and weather_data_raw[3][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_WindSpeed'] = float(weather_data_raw[4][1]) if len(weather_data_raw[4]) > 1 and weather_data_raw[4][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Pressure'] = float(weather_data_raw[5][1]) if len(weather_data_raw[5]) > 1 and weather_data_raw[5][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Visibility'] = float(weather_data_raw[6][1]) if len(weather_data_raw[6]) > 1 and weather_data_raw[6][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Sunrise'] = weather_data_raw[7][1] if len(weather_data_raw[7]) > 1 else None
            current_weather['LA_Sunset'] = weather_data_raw[8][1] if len(weather_data_raw[8]) > 1 else None
            current_weather['LA_FineDust'] = None # Placeholder for fine dust if not in sheet

        forecast_weather = []
        if len(weather_data_raw) > 12: # Check if forecast data exists (starts from row 12, index 11)
            for row in weather_data_raw[11:]: # From row 12 onwards
                if len(row) >= 5 and row[0]: # Ensure date and basic info exist
                    forecast_day = {
                        'date': row[0],
                        'min_temp': float(row[1]) if row[1] and row[1].replace('.', '', 1).isdigit() else None,
                        'max_temp': float(row[2]) if row[2] and row[2].replace('.', '', 1).isdigit() else None,
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
        # Columns D (index 3) and E (index 4)
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

        # --- Combine all data into a single dictionary for JSON output ---
        final_output_data = {
            "chart_data": processed_chart_data_by_section,
            "table_data": processed_table_data_by_section, # Include table data
            "weather_data": {
                "current": current_weather,
                "forecast": forecast_weather
            },
            "exchange_rates": exchange_rates
        }

        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_output_data, f, ensure_ascii=False, indent=4, cls=NpEncoder) # Use NpEncoder here

        print(f"All data successfully saved to '{OUTPUT_JSON_PATH}'.")
        # Print sample of each section's data
        for section_key, data_list in processed_chart_data_by_section.items():
            print(f"Sample of saved chart data for {section_key} (first 3 entries): {data_list[:3]}")

    except Exception as e:
        print(f"Error during data processing: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_process_data()
