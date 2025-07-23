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
        
        # --- Fetch Main Chart Data ---
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        all_data = worksheet.get_all_values()

        if not all_data:
            print("Error: No data fetched from the main chart sheet.")
            return

        main_header_row_index = -1
        for i in range(1, len(all_data)): # Start from index 1 (Row 2 in Google Sheet)
            row = all_data[i]
            if any(cell.strip().lower() == "date" for cell in row):
                main_header_row_index = i
                break

        print(f"DEBUG: Main chart data header row index: {main_header_row_index}")

        if main_header_row_index == -1:
            print("Error: Could not find the main header row containing 'date' in main chart data.")
            return

        raw_headers_original = [h.strip().replace('"', '') for h in all_data[main_header_row_index]]
        
        final_column_names = []
        current_section_prefix = ""
        empty_col_counter = 0
        seen_final_names_set = set()

        for col_idx, h_orig in enumerate(raw_headers_original):
            cleaned_h_orig = h_orig.strip().replace('"', '')
            final_name_candidate = cleaned_h_orig

            if col_idx == 44 and cleaned_h_orig == "종합지수": # Column AS (index 44) for "종합지수" is IACI
                final_name_candidate = "IACI_Composite_Index"
                print(f"DEBUG: Explicitly renaming column at index {col_idx} from '{cleaned_h_orig}' to '{final_name_candidate}' (IACI).")
            else:
                found_section_marker_in_sequence = False
                for i in range(len(SECTION_MARKER_SEQUENCE)):
                    marker_string, marker_prefix_base = SECTION_MARKER_SEQUENCE[i]
                    if cleaned_h_orig == marker_string:
                        current_section_prefix = f"{marker_prefix_base}_"
                        if marker_prefix_base == "BLANK_SAILING":
                            final_name_candidate = "Date_Blank_Sailing"
                            current_section_prefix = ""
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

        print(f"DEBUG: Final DataFrame column names after mapping: {final_column_names}")

        data_rows_raw = all_data[main_header_row_index + 1:]
        
        processed_data_rows = []
        num_expected_cols = len(final_column_names)
        for i, row in enumerate(data_rows_raw):
            cleaned_row = [str(cell) if cell is not None else '' for cell in row]
            if len(cleaned_row) < num_expected_cols:
                padded_row = cleaned_row + [''] * (num_expected_cols - len(cleaned_row))
                processed_data_rows.append(padded_row)
            elif len(cleaned_row) > num_expected_cols:
                truncated_row = cleaned_row[:num_expected_cols]
                processed_data_rows.append(truncated_row)
            else:
                processed_data_rows.append(cleaned_row)

        df_final = pd.DataFrame(processed_data_rows, columns=final_column_names)
        
        if 'IACI_Composite_Index' not in df_final.columns:
            df_final['IACI_Composite_Index'] = None

        cols_to_drop = [col for col in df_final.columns if col.startswith('_EMPTY_COL_')]
        if cols_to_drop:
            print(f"DEBUG: Dropping empty columns: {cols_to_drop}")
            df_final.drop(columns=cols_to_drop, inplace=True, errors='ignore')

        df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce')
        df_final.dropna(subset=['date'], inplace=True)
        df_final = df_final.sort_values(by='date', ascending=True)
        df_final['date'] = df_final['date'].dt.strftime('%Y-%m-%d')

        if df_final['date'].empty:
            print("Warning: After all date processing, the 'date' column is empty. Charts might not display correctly.")

        numeric_cols = [col for col in df_final.columns if col != 'date']
        for col in numeric_cols:
            df_final[col] = pd.to_numeric(df_final[col].astype(str).str.replace(',', ''), errors='coerce')
        
        df_final = df_final.replace({pd.NA: None, float('nan'): None})

        processed_chart_data = df_final.to_dict(orient='records')

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

        # --- Combine all data into a single dictionary for JSON output ---
        final_output_data = {
            "chart_data": processed_chart_data,
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
        print(f"Sample of saved chart data (first 3 entries): {processed_chart_data[:3]}")

    except Exception as e:
        print(f"Error during data processing: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_process_data()
