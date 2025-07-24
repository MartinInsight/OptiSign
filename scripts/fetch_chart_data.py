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
WEATHER_WORKSHEET_NAME = "LA날씨" # New: Weather sheet name
EXCHANGE_RATE_WORKSHEET_NAME = "환율" # New: Exchange rate sheet name
OUTPUT_JSON_PATH = "data/crawling_data.json"

# --- Header Mapping Definitions ---
# This dictionary now defines the exact column indices in the Google Sheet
# and their corresponding final JSON key names for each section.
# This makes the mapping explicit and less prone to inference errors.
# The 'date_col_idx' is the 0-indexed column number for the date in that section.
# The 'data_cols_map' maps raw header names (or derived names) to final JSON keys.
SECTION_COLUMN_MAPPINGS = {
    "KCCI": {
        "date_col_idx": 0, # Column A
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
        "date_col_idx": 16, # Column Q
        "data_cols_map": {
            "Comprehensive Index": "Composite_Index_1",
            "Europe (Base port)": "North_Europe",
            "Mediterranean (Base port)": "Mediterranean_1",
            "USWC (Base port)": "US_West_Coast_1",
            "USEC (Base port)": "US_East_Coast_1",
            "Persian Gulf and Red Sea (Dubai)": "Middle_East_1",
            "Australia/New Zealand (Melbourne)": "Australia_New_Zealand_SCFI",
            "East/West Africa (Lagos)": "East_West_Africa_SCFI",
            "South Africa (Durban)": "South_Africa_SCFI",
            "West Japan (Base port)": "Japan_West_Coast_SCFI",
            "East Japan (Base port)": "Japan_East_Coast_SCFI",
            "Southeast Asia (Singapore)": "Southeast_Asia_1",
            "Korea (Pusan)": "Korea_SCFI",
            "Central/South America West Coast(Manzanillo)": "South_America_SCFI"
        }
    },
    "WCI": {
        "date_col_idx": 32, # Column AG
        "data_cols_map": {
            "Composite Index": "Composite_Index_2",
            "Shanghai-Rotterdam": "Shanghai_Rotterdam_WCI",
            "Rotterdam-Shanghai": "Rotterdam_Shanghai_WCI",
            "Shanghai-Genoa": "Shanghai_Genoa_WCI",
            "Shanghai-LosAngeles": "Shanghai_Los_Angeles_WCI",
            "LosAngeles-Shanghai": "Los_Angeles_Shanghai_WCI",
            "Shanghai-NewYork": "Shanghai_New_York_WCI",
            "NewYork-Rotterdam": "New_York_Rotterdam_WCI",
            "Rotterdam-NewYork": "Rotterdam_New_York_WCI",
            # Assuming these are the correct raw headers for WCI's additional columns if they exist
            "Europe - South America East Coast": "Europe_South_America_East_Coast_WCI",
            "Europe - South America West Coast": "Europe_South_America_West_Coast_WCI"
        }
    },
    "IACI": {
        "date_col_idx": 43, # Column AR
        "data_cols_map": {
            "US$/40ft": "Composite_Index_3" # Assuming this is the raw header for IACI value
        }
    },
    "BLANK_SAILING": {
        "date_col_idx": 46, # Column AU
        "data_cols_map": {
            "Gemini Cooperation": "Gemini_Cooperation_Blank_Sailing",
            "MSC": "MSC_Alliance_Blank_Sailing",
            "OCEAN Alliance": "OCEAN_Alliance_Blank_Sailing",
            "Premier Alliance": "Premier_Alliance_Blank_Sailing",
            "Others/Independent": "Others_Independent_Blank_Sailing",
            "Total": "Total_Blank_Sailings" # If 'Total' is a column in sheet
        }
    },
    "FBX": {
        "date_col_idx": 54, # Column BC
        "data_cols_map": {
            "Global Container Freight Index": "Composite_Index_4",
            "China/East Asia - North America West Coast": "China_EA_US_West_Coast_FBX",
            "North America West Coast - China/East Asia": "US_West_Coast_China_EA_FBX",
            "China/East Asia - North America East Coast": "China_EA_US_East_Coast_FBX",
            "North America East Coast - China/East Asia": "US_East_Coast_China_EA_FBX",
            "China/East Asia - North Europe": "China_EA_North_Europe_FBX",
            "North Europe - China/East Asia": "North_Europe_China_EA_FBX",
            "China/East Asia - Mediterranean": "China_EA_Mediterranean_FBX",
            "Mediterranean - China/East Asia": "Mediterranean_China_EA_FBX",
            # Add placeholders for other FBX routes if they exist in the sheet
            "North America East Coast - North Europe": "North_America_East_Coast_North_Europe_FBX",
            "North Europe - North America East Coast": "North_Europe_North_America_East_Coast_FBX",
            "Europe - South America East Coast": "Europe_South_America_East_Coast_FBX",
            "Europe - South America West Coast": "Europe_South_America_West_Coast_FBX"
        }
    },
    "XSI": {
        "date_col_idx": 68, # Column BQ (Corrected from BR based on visual inspection)
        "data_cols_map": {
            "Far East - N. Europe": "XSI_East_Asia_North_Europe",
            "N. Europe - Far East": "XSI_North_Europe_East_Asia",
            "Far East - USWC": "XSI_East_Asia_US_West_Coast",
            "USWC - Far East": "XSI_US_West_Coast_East_Asia",
            "Far East - SAEC": "XSI_East_Asia_South_America_East_Coast",
            "N. Europe - USEC": "XSI_North_Europe_US_East_Coast",
            "USEC - N. Europe": "XSI_US_East_Coast_North_Europe",
            "N. Europe - SAEC": "XSI_North_Europe_South_America_East_Coast"
        }
    },
    "MBCI": {
        "date_col_idx": 78, # Column CA (Corrected from CB based on visual inspection)
        "data_cols_map": {
            "Index(종합지수)": "MBCI_MBCI_Value",
            # "$/day(정기용선, Time charter)": This is a table-only value, not for chart, so it's excluded from data_cols_map
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

        # Iterate through each section and extract its specific data
        for section_key, details in SECTION_COLUMN_MAPPINGS.items():
            date_col_idx = details["date_col_idx"]
            data_cols_map = details["data_cols_map"]
            
            # Prepare columns to extract for this section
            cols_to_extract_indices = [date_col_idx]
            # Find indices for data columns based on their raw header names
            raw_header_to_col_idx = {raw_headers_full[i]: i for i in range(len(raw_headers_full))}

            # This will store the final column names for the DataFrame of this section
            section_df_columns = ["date"] # The date column will always be named 'date' in the DataFrame

            # Collect data columns and their final names for this section
            for raw_header_name, final_json_key in data_cols_map.items():
                if raw_header_name in raw_header_to_col_idx:
                    cols_to_extract_indices.append(raw_header_to_col_idx[raw_header_name])
                    section_df_columns.append(final_json_key)
                else:
                    print(f"WARNING: Raw header '{raw_header_name}' not found for section '{section_key}'. Skipping this column.")

            # Extract raw data for the specific columns of this section
            section_raw_rows = []
            for row_idx in range(main_header_row_index + 1, len(all_data)):
                row_data = all_data[row_idx]
                extracted_row = []
                for col_idx in cols_to_extract_indices:
                    if col_idx < len(row_data):
                        extracted_row.append(str(row_data[col_idx]).strip())
                    else:
                        extracted_row.append('') # Pad with empty string if row is too short
                section_raw_rows.append(extracted_row)
            
            if not section_raw_rows:
                print(f"WARNING: No data rows found for section {section_key}. Skipping chart and table data for this section.")
                processed_chart_data_by_section[section_key] = []
                processed_table_data_by_section[section_key] = {"headers": [], "rows": []}
                continue

            # Create a DataFrame for this section
            # Ensure the number of columns matches the extracted_row length
            if len(section_raw_rows[0]) != len(section_df_columns):
                 print(f"ERROR: Mismatch in column count for section {section_key}. Expected {len(section_df_columns)} but got {len(section_raw_rows[0])} in first row.")
                 print(f"DEBUG: section_df_columns: {section_df_columns}")
                 print(f"DEBUG: section_raw_rows[0]: {section_raw_rows[0]}")
                 processed_chart_data_by_section[section_key] = []
                 processed_table_data_by_section[section_key] = {"headers": [], "rows": []}
                 continue

            df_section = pd.DataFrame(section_raw_rows, columns=section_df_columns)
            print(f"DEBUG: Initial DataFrame for {section_key} shape: {df_section.shape}")
            print(f"DEBUG: Initial DataFrame for {section_key} head:\n{df_section.head()}")

            # Clean and parse dates for THIS section
            # The date column is always named 'date' in this DataFrame
            df_section['date'] = df_section['date'].astype(str).str.strip()
            
            # Log all original date strings for this section before parsing attempt
            print(f"DEBUG: All original date strings for {section_key} before parsing ({len(df_section['date'])} entries): {df_section['date'].tolist()[:10]}")

            df_section['parsed_date'] = pd.to_datetime(df_section['date'], errors='coerce')
            
            # Log unparseable dates for this section
            unparseable_dates_series = df_section.loc[df_section['parsed_date'].isna(), 'date']
            num_unparseable_dates = unparseable_dates_series.count() # Count non-empty unparseable strings
            if num_unparseable_dates > 0:
                print(f"WARNING: {num_unparseable_dates} dates could not be parsed for {section_key} and will be dropped. All unparseable date strings: {unparseable_dates_series.tolist()}")

            df_section.dropna(subset=['parsed_date'], inplace=True) # Drop rows where date parsing failed for this section
            print(f"DEBUG: DataFrame shape for {section_key} after date parsing and dropna: {df_section.shape}")

            # Convert numeric columns for this section (excluding the 'date' and 'parsed_date' columns)
            cols_to_convert_to_numeric = [col for col in section_df_columns if col != "date"]
            for col in cols_to_convert_to_numeric:
                df_section[col] = pd.to_numeric(df_section[col].astype(str).str.replace(',', ''), errors='coerce')
            
            df_section = df_section.replace({pd.NA: None, float('nan'): None})

            # Sort and format date
            df_section = df_section.sort_values(by='parsed_date', ascending=True)
            df_section['date'] = df_section['parsed_date'].dt.strftime('%Y-%m-%d')
            
            # Select final columns for chart output (only 'date' and the data columns)
            output_chart_cols = ['date'] + cols_to_convert_to_numeric
            processed_chart_data_by_section[section_key] = df_section[output_chart_cols].to_dict(orient='records')
            print(f"DEBUG: Processed chart data for {section_key} (first 3 entries): {processed_chart_data_by_section[section_key][:3]}")

            # Prepare table data for this section
            # Table headers should come from the original mapping keys (Korean/English names)
            table_headers = ["날짜" if section_key == "KCCI" else "Date", "Current Index", "Previous Index", "Weekly Change"]
            table_rows_data = []

            # Get the latest row for current/previous index and weekly change
            if not df_section.empty:
                latest_row = df_section.iloc[-1]
                second_latest_row = df_section.iloc[-2] if len(df_section) > 1 else None

                for raw_header_name, final_json_key in data_cols_map.items():
                    current_index_val = latest_row.get(final_json_key)
                    previous_index_val = second_latest_row.get(final_json_key) if second_latest_row is not None else None
                    
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
                    
                    # Ensure 'route' key is present for table rendering
                    table_rows_data.append({
                        "route": raw_header_name, # Use the original raw header name for the table route
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
            json.dump(final_output_data, f, ensure_ascii=False, indent=4)

        print(f"All data successfully saved to '{OUTPUT_JSON_PATH}'.")
        # Print sample of each section's data
        for section_key, data_list in processed_chart_data_by_section.items():
            print(f"Sample of saved chart data for {section_key} (first 3 entries): {data_list[:3]}")

    except Exception as e:
        print(f"Error during data processing: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_process_data()
