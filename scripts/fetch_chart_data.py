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
WORKSHEET_NAME = "Crawling_Data" # Assuming the data is still in "Crawling_Data" sheet
WEATHER_WORKSHEET_NAME = "LA날씨" # New: Weather sheet name
EXCHANGE_RATE_WORKSHEET_NAME = "환율" # New: Exchange rate sheet name
OUTPUT_JSON_PATH = "data/crawling_data.json"

# --- Header Mapping Definitions for Chart Data (Historical Series) ---
# These define the column ranges and their corresponding JSON keys for the historical chart data.
SECTION_COLUMN_MAPPINGS = {
    "KCCI": {
        "data_start_col_idx": 1, # KCCI data starts from '종합지수' at index 1
        "data_end_col_idx": 14, # Last KCCI data column is '동남아시아' at index 14
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
        "data_end_col_idx": 30, # Last SCFI data column is '남아공' at index 30
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
        "data_start_col_idx": 33, # WCI data starts from '종합지수' at index 33
        "data_end_col_idx": 41, # Last WCI data column is '로테르담 → 뉴욕' at index 41
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
        "data_start_col_idx": 45, # IACI data starts from '종합지수' at index 45
        "data_end_col_idx": 45, # Last IACI data column is '종합지수' at index 45
        "data_cols_map": {
            "종합지수": "Composite_Index_3"
        }
    },
    "BLANK_SAILING": {
        "data_start_col_idx": 48, # BLANK_SAILING data starts from 'Gemini Cooperation' at index 48
        "data_end_col_idx": 53, # Last BLANK_SAILING data column is 'Total' at index 53
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
        "data_start_col_idx": 56, # FBX data starts from '종합지수' at index 56 (Corrected from 57)
        "data_end_col_idx": 65, # Last FBX data column is '지중해 → 중국/동아시아' at index 65
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
        "data_start_col_idx": 70, # XSI data starts from '동아시아 → 북유럽' at index 70 (Corrected from 71)
        "data_end_col_idx": 77, # Last XSI data column is '북유럽 → 남미동안' at index 77 (Corrected from 78)
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
        "data_start_col_idx": 81, # MBCI data starts from 'Index(종합지수), $/day(정기용선, Time ' at index 81 (Corrected from 82)
        "data_end_col_idx": 81, # Last MBCI data column is 'Index(종합지수), $/day(정기용선, Time ' at index 81
        "data_cols_map": {
            "Index(종합지수), $/day(정기용선, Time ": "MBCI_MBCI_Value", # Corrected full header name
        }
    }
}

# --- Specific Cell Mappings for Table Data (Current, Previous, Weekly Change) ---
# These define the exact 0-indexed row and column for fetching table summary data.
# The 'data_col_headers' will be used to find the corresponding column indices in raw_headers_full.
TABLE_DATA_CELL_MAPPINGS = {
    "KCCI": {
        "current_index_row": 2, # A3 (0-indexed row 2)
        "previous_index_row": 3, # A4 (0-indexed row 3)
        "weekly_change_row": 4, # A5 (0-indexed row 4)
        "data_col_headers": ["종합지수", "미주서안", "미주동안", "유럽", "지중해", "중동", "호주", "남미동안", "남미서안", "남아프리카", "서아프리카", "중국", "일본", "동남아시아"]
    },
    "SCFI": {
        "current_index_row": 8, # A9
        "previous_index_row": 9, # A10
        "weekly_change_row": 10, # A11
        "data_col_headers": ["종합지수", "미주서안", "미주동안", "북유럽", "지중해", "동남아시아", "중동", "호주/뉴질랜드", "남아메리카", "일본서안", "일본동안", "한국", "동부/서부 아프리카", "남아공"]
    },
    "WCI": {
        "current_index_row": 20, # A21
        "previous_index_row": 21, # A22
        "weekly_change_row": 22, # A23
        "data_col_headers": ["종합지수", "상하이 → 로테르담", "로테르담 → 상하이", "상하이 → 제노바", "상하이 → 로스엔젤레스", "로스엔젤레스 → 상하이", "상하이 → 뉴욕", "뉴욕 → 로테르담", "로테르담 → 뉴욕"]
    },
    "IACI": {
        "current_index_row": 26, # A27
        "previous_index_row": 27, # A28
        "weekly_change_row": 28, # A29
        "data_col_headers": ["종합지수"]
    },
    "BLANK_SAILING": {
        "current_index_row": 32, # A33
        "previous_index_row": 33, # A34 (Using A34 for previous data for the table)
        "weekly_change_row": None, # No explicit weekly change row provided for Blank Sailing table, will calculate
        "data_col_headers": ["Gemini Cooperation", "MSC", "OCEAN Alliance", "Premier Alliance", "Others/Independent", "Total"]
    },
    "FBX": {
        "current_index_row": 40, # A41
        "previous_index_row": 41, # A42
        "weekly_change_row": 42, # A43
        "data_col_headers": ["종합지수", "중국/동아시아 → 미주서안", "미주서안 → 중국/동아시아", "중국/동아시아 → 미주동안", "미주동안 → 중국/동아시아", "중국/동아시아 → 북유럽", "북유럽 → 중국/동아시아", "중국/동아시아 → 지중해", "지중해 → 중국/동아시아"]
    },
    "XSI": {
        "current_index_row": 46, # A47
        "previous_index_row": 47, # A48
        "weekly_change_row": 48, # A49
        "data_col_headers": ["동아시아 → 북유럽", "북유럽 → 동아시아", "동아시아 → 미주서안", "미주서안 → 동아시아", "동아시아 → 남미동안", "북유럽 → 미주동안", "미주동안 → 북유럽", "북유럽 → 남미동안"]
    },
    "MBCI": {
        "current_index_row": 58, # A59
        "previous_index_row": 59, # A60
        "weekly_change_row": 60, # A61
        "data_col_headers": ["Index(종합지수), $/day(정기용선, Time "] # Corrected full header name
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


        # Iterate through each section and extract its specific data for CHARTS
        for section_key, details in SECTION_COLUMN_MAPPINGS.items():
            data_cols_map = details["data_cols_map"]
            
            cols_to_extract_indices = []
            section_df_columns = []

            # Find indices for data columns based on their raw header names, within the defined chart data range
            for raw_header_name, final_json_key in data_cols_map.items():
                found_idx = -1
                # Search for the header within the defined section's chart data range
                for idx_in_full_headers in range(details["data_start_col_idx"], details["data_end_col_idx"] + 1):
                    if idx_in_full_headers < len(raw_headers_full) and str(raw_headers_full[idx_in_full_headers]).strip() == str(raw_header_name).strip():
                        found_idx = idx_in_full_headers
                        break # Found it, break and move to next header
                
                if found_idx != -1:
                    cols_to_extract_indices.append(found_idx)
                    section_df_columns.append(final_json_key)
                else:
                    print(f"WARNING: Chart header '{raw_header_name}' not found in its expected section range for '{section_key}'. Skipping this column for chart data.")

            print(f"DEBUG: For section {section_key} (Chart Data): cols_to_extract_indices = {cols_to_extract_indices}, section_df_columns = {section_df_columns}")


            if not cols_to_extract_indices:
                print(f"WARNING: No valid data columns found for section {section_key} chart. Skipping chart data for this section.")
                processed_chart_data_by_section[section_key] = []
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
                print(f"WARNING: No data rows found for section {section_key} chart. Skipping chart data for this section.")
                processed_chart_data_by_section[section_key] = []
                continue

            # Create a DataFrame for this section's data columns
            if len(section_raw_rows[0]) != len(section_df_columns):
                 print(f"ERROR: Mismatch in column count for section {section_key} chart. Expected {len(section_df_columns)} but got {len(section_raw_rows[0])} in first row.")
                 print(f"DEBUG: section_df_columns (chart): {section_df_columns}")
                 print(f"DEBUG: section_raw_rows[0] (chart): {section_raw_rows[0]}")
                 processed_chart_data_by_section[section_key] = []
                 continue

            df_section_data = pd.DataFrame(section_raw_rows, columns=section_df_columns)
            print(f"DEBUG: Initial DataFrame for {section_key} chart shape: {df_section_data.shape}")
            print(f"DEBUG: Initial DataFrame for {section_key} chart head:\n{df_section_data.head()}")

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

            # --- Prepare table data for this section using direct cell references ---
            table_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
            table_rows_data = []

            table_details = TABLE_DATA_CELL_MAPPINGS.get(section_key)
            if table_details:
                current_row_idx = table_details["current_index_row"]
                previous_row_idx = table_details["previous_index_row"]
                weekly_change_row_idx = table_details["weekly_change_row"]
                data_col_headers_for_table = table_details["data_col_headers"]

                # Find the actual column indices for table data based on their headers in raw_headers_full
                table_cols_indices = []
                for header in data_col_headers_for_table:
                    try:
                        # Find the index of the header in the full raw headers list
                        col_idx = raw_headers_full.index(str(header).strip())
                        table_cols_indices.append(col_idx)
                    except ValueError:
                        print(f"WARNING: Table header '{header}' not found in raw_headers_full for '{section_key}'. Skipping this column for table data.")
                        table_cols_indices.append(None) # Add None to maintain alignment if a header is missing

                for i, route_name in enumerate(data_col_headers_for_table):
                    col_idx_for_data = table_cols_indices[i]
                    if col_idx_for_data is None: # Skip if header was not found
                        continue

                    current_index_val = None
                    previous_index_val = None
                    weekly_change = None

                    # Fetch current index
                    if current_row_idx < len(all_data) and col_idx_for_data < len(all_data[current_row_idx]):
                        val = str(all_data[current_row_idx][col_idx_for_data]).strip().replace(',', '')
                        current_index_val = float(val) if val and val.replace('.', '', 1).isdigit() else None

                    # Fetch previous index
                    if previous_row_idx is not None and previous_row_idx < len(all_data) and col_idx_for_data < len(all_data[previous_row_idx]):
                        val = str(all_data[previous_row_idx][col_idx_for_data]).strip().replace(',', '')
                        previous_index_val = float(val) if val and val.replace('.', '', 1).isdigit() else None
                    
                    # Fetch weekly change or calculate it
                    if weekly_change_row_idx is not None and weekly_change_row_idx < len(all_data) and col_idx_for_data < len(all_data[weekly_change_row_idx]):
                        val = str(all_data[weekly_change_row_idx][col_idx_for_data]).strip().replace(',', '')
                        # Weekly change might be a direct value or a string like "X (Y%)"
                        match = re.match(r'([+\-]?\d+(\.\d+)?)\s*\(([-+]?\d+(\.\d+)?%)\)', val)
                        if match:
                            change_value = float(match.group(1))
                            change_percentage_str = match.group(3)
                            color_class = "text-gray-700"
                            if change_value > 0:
                                color_class = "text-red-500"
                            elif change_value < 0:
                                color_class = "text-blue-500"
                            weekly_change = {
                                "value": f"{change_value:.2f}",
                                "percentage": change_percentage_str,
                                "color_class": color_class
                            }
                        elif val and (val.replace('.', '', 1).replace('-', '', 1).isdigit() or (val.endswith('%') and val[:-1].replace('.', '', 1).replace('-', '', 1).isdigit())):
                            # If it's just a number or a percentage, assume it's the value and calculate percentage if possible
                            try:
                                change_val_only = float(val.replace('%', ''))
                                color_class = "text-gray-700"
                                if change_val_only > 0:
                                    color_class = "text-red-500"
                                elif change_val_only < 0:
                                    color_class = "text-blue-500"
                                weekly_change = {
                                    "value": f"{change_val_only:.2f}",
                                    "percentage": f"{change_val_only:.2f}%" if '%' not in val else val, # Preserve % if already there
                                    "color_class": color_class
                                }
                                # If we have current and previous, try to get percentage more accurately
                                if current_index_val is not None and previous_index_val is not None and previous_index_val != 0:
                                    calculated_change_percentage = ((current_index_val - previous_index_val) / previous_index_val) * 100
                                    weekly_change["percentage"] = f"{calculated_change_percentage:.2f}%"
                            except ValueError:
                                weekly_change = None # Cannot parse
                        else:
                            weekly_change = None # Not a recognized format
                    elif weekly_change_row_idx is None and current_index_val is not None and previous_index_val is not None and previous_index_val != 0:
                        # Calculate weekly change if no explicit row is provided (e.g., Blank Sailing)
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
                        "route": route_name, # Use the original raw header name (Korean) for the table route
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
