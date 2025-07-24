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

# Name of the worksheets containing the data
WORKSHEET_NAME_CHARTS = "Crawling_Data"
WORKSHEET_NAME_TABLES = "Crawling_Data2" # New worksheet for table data
WEATHER_WORKSHEET_NAME = "LA날씨"
EXCHANGE_RATE_WORKSHEET_NAME = "환율"
OUTPUT_JSON_PATH = "data/crawling_data.json"

# --- Header Mapping Definitions for Chart Data (Historical Series from Crawling_Data) ---
# These define the column ranges and their corresponding JSON keys for the historical chart data.
# The 'date_col_idx' specifies the 0-indexed column for the date for THIS section.
# The 'data_start_col_idx' and 'data_end_col_idx' are 0-indexed for the actual numeric data.
SECTION_COLUMN_MAPPINGS = {
    "KCCI": {
        "date_col_idx": 0, # A열은 날짜
        "data_start_col_idx": 1, # B열은 종합지수
        "data_end_col_idx": 14, # O열은 동남아시아
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
        "date_col_idx": 16, # Q열은 날짜
        "data_start_col_idx": 17, # R열은 종합지수
        "data_end_col_idx": 30, # AE열은 남아공
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
        "date_col_idx": 32, # AG열은 날짜
        "data_start_col_idx": 33, # AH열은 종합지수
        "data_end_col_idx": 41, # AP열은 로테르담 → 뉴욕
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
        "date_col_idx": 43, # AR열은 날짜
        "data_start_col_idx": 44, # AS열은 종합지수
        "data_end_col_idx": 44, # AS열
        "data_cols_map": {
            "종합지수": "Composite_Index_3"
        }
    },
    "BLANK_SAILING": {
        "date_col_idx": 46, # AU열은 날짜
        "data_start_col_idx": 47, # AV열은 Index
        "data_end_col_idx": 52, # BA열은 Total
        "data_cols_map": {
            "Index": "Index_Blank_Sailing", # 'Index' is the actual header here
            "Gemini Cooperation": "Gemini_Cooperation_Blank_Sailing",
            "MSC": "MSC_Alliance_Blank_Sailing",
            "OCEAN Alliance": "OCEAN_Alliance_Blank_Sailing",
            "Premier Alliance": "Premier_Alliance_Blank_Sailing",
            "Others/Independent": "Others_Independent_Blank_Sailing",
            "Total": "Total_Blank_Sailings"
        }
    },
    "FBX": {
        "date_col_idx": 54, # BC열은 날짜
        "data_start_col_idx": 55, # BD열은 종합지수
        "data_end_col_idx": 67, # BP열은 유럽 → 남미서안
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
            "미주동안 → 북유럽": "US_East_Coast_North_Europe_FBX",
            "북유럽 → 미주동안": "North_Europe_US_East_Coast_FBX",
            "유럽 → 남미동안": "Europe_South_America_East_Coast_FBX",
            "유럽 → 남미서안": "Europe_South_America_West_Coast_FBX",
        }
    },
    "XSI": {
        "date_col_idx": 69, # BR열은 날짜
        "data_start_col_idx": 70, # BS열은 동아시아 → 북유럽
        "data_end_col_idx": 77, # BZ열은 북유럽 → 남미동안
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
        "date_col_idx": 79, # CB열은 날짜
        "data_start_col_idx": 80, # CC열은 MBCI
        "data_end_col_idx": 80, # CC열
        "data_cols_map": {
            "MBCI": "MBCI_MBCI_Value", # Corrected header name for chart data
        }
    }
}

# --- Specific Cell Mappings for Table Data (Current, Previous, Weekly Change from Crawling_Data2) ---
# These define the exact 0-indexed row and column for fetching table summary data.
# The 'route_names' will be used for the table's "항로" column, matching chart legends.
TABLE_DATA_CELL_MAPPINGS = {
    "KCCI": {
        "current_date_cell": (2,0), # A3 (row 2, col 0)
        "current_index_cols_range": (1, 14), # B3:O3 (cols 1 to 14)
        "previous_date_cell": (3,0), # A4 (row 3, col 0)
        "previous_index_cols_range": (1, 14), # B4:O4 (cols 1 to 14)
        "weekly_change_cols_range": (1, 14), # B5:O5 (cols 1 to 14)
        "route_names": ["종합지수", "미주서안", "미주동안", "유럽", "지중해", "중동", "호주", "남미동안", "남미서안", "남아프리카", "서아프리카", "중국", "일본", "동남아시아"]
    },
    "SCFI": {
        "current_date_cell": (8,0), # A9
        "current_index_cols_range": (1, 14), # B9:O9
        "previous_date_cell": (9,0), # A10
        "previous_index_cols_range": (1, 14), # B10:O10
        "weekly_change_cols_range": (1, 14), # B11:O11
        "route_names": ["종합지수", "미주서안", "미주동안", "북유럽", "지중해", "동남아시아", "중동", "호주/뉴질랜드", "남아메리카", "일본서안", "일본동안", "한국", "동부/서부 아프리카", "남아공"]
    },
    "WCI": {
        "current_date_cell": (20,0), # A21
        "current_index_cols_range": (1, 9), # B21:J21
        "previous_date_cell": (21,0), # A22
        "previous_index_cols_range": (1, 9), # B22:J22
        "weekly_change_cols_range": (1, 9), # B23:J23 (Corrected from O23 as per B21:J21 data range)
        "route_names": ["종합지수", "상하이 → 로테르담", "로테르담 → 상하이", "상하이 → 제노바", "상하이 → 로스엔젤레스", "로스엔젤레스 → 상하이", "상하이 → 뉴욕", "뉴욕 → 로테르담", "로테르담 → 뉴욕"]
    },
    "IACI": {
        "current_date_cell": (26,0), # A27
        "current_index_cols_range": (1, 1), # B27
        "previous_date_cell": (27,0), # A28
        "previous_index_cols_range": (1, 1), # B28
        "weekly_change_cols_range": (1, 1), # B29
        "route_names": ["종합지수"]
    },
    "BLANK_SAILING": {
        "current_date_cell": (32,0), # A33
        "current_index_cols_range": (1, 6), # B33:G33
        "previous_date_cells_and_ranges": [ # For Blank Sailing, multiple previous dates/data rows
            {"date_cell": (33,0), "data_range": (1, 6)}, # A34, B34:G34
            {"date_cell": (34,0), "data_range": (1, 6)}, # A35, B35:G35
            {"date_cell": (35,0), "data_range": (1, 6)}, # A36, B36:G36
            {"date_cell": (36,0), "data_range": (1, 6)}, # A37, B37:G37
        ],
        "weekly_change_cols_range": None, # No explicit weekly change row provided, will calculate from current/previous
        "route_names": ["Gemini Cooperation", "MSC", "OCEAN Alliance", "Premier Alliance", "Others/Independent", "Total"]
    },
    "FBX": {
        "current_date_cell": (40,0), # A41
        "current_index_cols_range": (1, 13), # B41:N41
        "previous_date_cell": (41,0), # A42
        "previous_index_cols_range": (1, 13), # B42:N42
        "weekly_change_cols_range": (1, 13), # B43:N43
        "route_names": ["종합지수", "중국/동아시아 → 미주서안", "미주서안 → 중국/동아시아", "중국/동아시아 → 미주동안", "미주동안 → 중국/동아시아", "중국/동아시아 → 북유럽", "북유럽 → 중국/동아시아", "중국/동아시아 → 지중해", "지중해 → 중국/동아시아", "미주동안 → 북유럽", "북유럽 → 미주동안", "유럽 → 남미동안", "유럽 → 남미서안"]
    },
    "XSI": {
        "current_date_cell": (46,0), # A47
        "current_index_cols_range": (1, 8), # B47:I47
        "previous_date_cell": (47,0), # A48
        "previous_index_cols_range": (1, 8), # B48:I48 (Corrected from N48 as per B47:I47 data range)
        "weekly_change_cols_range": (1, 8), # B49:I49 (Corrected from N49 as per B47:I47 data range)
        "route_names": ["동아시아 → 북유럽", "북유럽 → 동아시아", "동아시아 → 미주서안", "미주서안 → 동아시아", "동아시아 → 남미동안", "북유럽 → 미주동안", "미주동안 → 북유럽", "북유럽 → 남미동안"]
    },
    "MBCI": {
        "current_date_cell": (58,0), # A59
        "current_index_cols_range": (6, 6), # G59
        "previous_date_cell": (59,0), # A60
        "previous_index_cols_range": (6, 6), # G60
        "weekly_change_cols_range": (6, 6), # G61
        "route_names": ["Index(종합지수), $/day(정기용선, Time "]
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
        
        # --- Fetch Main Chart Data from Crawling_Data sheet ---
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet_charts = spreadsheet.worksheet(WORKSHEET_NAME_CHARTS)
        all_data_charts = worksheet_charts.get_all_values()

        print(f"DEBUG: Total rows fetched from '{WORKSHEET_NAME_CHARTS}' (raw): {len(all_data_charts)}")

        if not all_data_charts:
            print(f"Error: No data fetched from the '{WORKSHEET_NAME_CHARTS}' sheet.")
            return

        # Find the main header row for chart data (the one containing '날짜' or 'Date' for the first section)
        # Assuming the headers are consistently on row 2 (index 1) as per user's description.
        main_header_row_index_charts = 1 
        
        # Get raw headers from the identified header row for charts
        raw_headers_full_charts = [str(h).strip() for h in all_data_charts[main_header_row_index_charts]]
        print(f"DEBUG: Raw headers from '{WORKSHEET_NAME_CHARTS}' (full row): {raw_headers_full_charts}")

        processed_chart_data_by_section = {}
        processed_table_data_by_section = {}

        # Iterate through each section and extract its specific data for CHARTS
        for section_key, details in SECTION_COLUMN_MAPPINGS.items():
            date_col_idx = details["date_col_idx"]
            data_start_col_idx = details["data_start_col_idx"]
            data_end_col_idx = details["data_end_col_idx"]
            data_cols_map = details["data_cols_map"]
            
            section_chart_data = []
            
            # Prepare column names for the DataFrame
            section_df_columns = ["date"] + list(data_cols_map.values())

            # Extract data rows starting from the row after the header
            for row_idx in range(main_header_row_index_charts + 1, len(all_data_charts)):
                row_data = all_data_charts[row_idx]
                
                current_record = {}
                
                # Extract date for this section
                if date_col_idx < len(row_data):
                    date_str = str(row_data[date_col_idx]).strip()
                    if date_str: # Only add if date string is not empty
                        current_record["date"] = date_str
                    else:
                        # If date column is empty, skip this row for this section's chart data
                        continue
                else:
                    continue # Skip if date column index is out of bounds for the row

                # Extract numeric data columns for this section
                extracted_numeric_data = []
                actual_numeric_headers = [] # To store actual headers found in the sheet for mapping

                # Collect actual headers from the raw_headers_full_charts for the numeric data range
                for col_idx in range(data_start_col_idx, data_end_col_idx + 1):
                    if col_idx < len(raw_headers_full_charts):
                        actual_numeric_headers.append(str(raw_headers_full_charts[col_idx]).strip())
                    else:
                        actual_numeric_headers.append(None) # Placeholder for missing header

                # Map actual headers to desired JSON keys and extract data
                for i, header_in_sheet in enumerate(actual_numeric_headers):
                    if header_in_sheet in data_cols_map:
                        col_idx = data_start_col_idx + i
                        if col_idx < len(row_data):
                            val = str(row_data[col_idx]).strip().replace(',', '')
                            current_record[data_cols_map[header_in_sheet]] = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                        else:
                            current_record[data_cols_map[header_in_sheet]] = None
                    else:
                        # If a header in the range is not in data_cols_map, it's unexpected, but we should handle it
                        # For now, we'll just skip it for the JSON output, or set to None if it's a critical column
                        print(f"WARNING: Header '{header_in_sheet}' from sheet range for {section_key} chart is not in data_cols_map. Skipping or setting to None.")
                        # If it's a critical column, you might want to add:
                        # current_record[f"unknown_col_{col_idx}"] = None 
            
                section_chart_data.append(current_record)
            
            # Convert to DataFrame for easier processing (e.g., sorting, handling NaNs)
            # Ensure all expected columns are present, even if some rows don't have them
            df_section = pd.DataFrame(section_chart_data)
            
            if 'date' in df_section.columns:
                df_section['parsed_date'] = pd.to_datetime(df_section['date'], errors='coerce')
                df_section = df_section[df_section['parsed_date'].notna()] # Filter out rows with invalid dates
                df_section = df_section.sort_values(by='parsed_date', ascending=True)
                df_section['date'] = df_section['parsed_date'].dt.strftime('%Y-%m-%d') # Standardize date format
                df_section = df_section.drop(columns=['parsed_date'])
            
            # Fill missing columns with None if any are not present in the dataframe after initial creation
            for col_name in section_df_columns:
                if col_name not in df_section.columns:
                    df_section[col_name] = None

            # Convert numeric columns to appropriate types and handle NaN/NaT
            for col in list(data_cols_map.values()):
                if col in df_section.columns:
                    df_section[col] = pd.to_numeric(df_section[col], errors='coerce')
                    df_section[col] = df_section[col].replace({np.nan: None}) # Replace numpy NaN with Python None

            processed_chart_data_by_section[section_key] = df_section.to_dict(orient='records')
            print(f"DEBUG: Processed chart data for {section_key} (first 3 entries): {processed_chart_data_by_section[section_key][:3]}")


        # --- Fetch Table Data from Crawling_Data2 sheet ---
        worksheet_tables = spreadsheet.worksheet(WORKSHEET_NAME_TABLES)
        all_data_tables = worksheet_tables.get_all_values()

        print(f"DEBUG: Total rows fetched from '{WORKSHEET_NAME_TABLES}' (raw): {len(all_data_tables)}")

        if not all_data_tables:
            print(f"Error: No data fetched from the '{WORKSHEET_NAME_TABLES}' sheet. Table data will be empty.")

        # Prepare table data for each section using direct cell references
        for section_key, table_details in TABLE_DATA_CELL_MAPPINGS.items():
            table_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
            table_rows_data = []

            # Handle Blank Sailing with multiple previous date/data rows
            if section_key == "BLANK_SAILING" and "previous_date_cells_and_ranges" in table_details:
                current_row_idx = table_details["current_date_cell"][0]
                current_cols_start, current_cols_end = table_details["current_index_cols_range"]
                route_names = table_details["route_names"]
                
                current_data_row = all_data_tables[current_row_idx] if current_row_idx < len(all_data_tables) else []

                # Collect all historical data for Blank Sailing to calculate changes
                blank_sailing_historical_data = []
                
                # Add current data
                current_bs_entry = {"date": (all_data_tables[current_row_idx][table_details["current_date_cell"][1]] if current_row_idx < len(all_data_tables) and table_details["current_date_cell"][1] < len(all_data_tables[current_row_idx]) else "")}
                for i in range(len(route_names)):
                    col_idx = current_cols_start + i
                    if col_idx <= current_cols_end and col_idx < len(current_data_row):
                        val = str(current_data_row[col_idx]).strip().replace(',', '')
                        current_bs_entry[route_names[i]] = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                blank_sailing_historical_data.append(current_bs_entry)

                # Add previous data rows
                for prev_entry_details in table_details["previous_date_cells_and_ranges"]:
                    prev_row_idx = prev_entry_details["date_cell"][0]
                    prev_cols_start, prev_cols_end = prev_entry_details["data_range"]
                    
                    if prev_row_idx < len(all_data_tables):
                        prev_data_row = all_data_tables[prev_row_idx]
                        prev_bs_entry = {"date": (all_data_tables[prev_row_idx][prev_entry_details["date_cell"][1]] if prev_row_idx < len(all_data_tables) and prev_entry_details["date_cell"][1] < len(all_data_tables[prev_row_idx]) else "")}
                        for i in range(len(route_names)):
                            col_idx = prev_cols_start + i
                            if col_idx <= prev_cols_end and col_idx < len(prev_data_row):
                                val = str(prev_data_row[col_idx]).strip().replace(',', '')
                                prev_bs_entry[route_names[i]] = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                        blank_sailing_historical_data.append(prev_bs_entry)
                
                # Sort historical data by date to ensure correct previous value calculation
                blank_sailing_historical_data.sort(key=lambda x: pd.to_datetime(x['date'], errors='coerce', dayfirst=False) if x['date'] else pd.Timestamp.min)

                # Now populate table_rows_data for Blank Sailing
                if len(blank_sailing_historical_data) >= 2:
                    latest_bs_data = blank_sailing_historical_data[-1]
                    second_latest_bs_data = blank_sailing_historical_data[-2]

                    for route_name in route_names:
                        current_index_val = latest_bs_data.get(route_name)
                        previous_index_val = second_latest_bs_data.get(route_name)
                        
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
                            "route": route_name,
                            "current_index": current_index_val,
                            "previous_index": previous_index_val,
                            "weekly_change": weekly_change
                        })
            else: # For all other sections
                current_row_idx = table_details["current_date_cell"][0]
                previous_row_idx = table_details["previous_date_cell"][0]
                weekly_change_row_idx_info = table_details["weekly_change_cols_range"] # This is now a tuple (start_col, end_col) or None
                
                current_cols_start, current_cols_end = table_details["current_index_cols_range"]
                previous_cols_start, previous_cols_end = table_details["previous_index_cols_range"]
                
                weekly_change_cols_start, weekly_change_cols_end = (None, None)
                if weekly_change_row_idx_info is not None:
                    weekly_change_cols_start, weekly_change_cols_end = weekly_change_row_idx_info

                route_names = table_details["route_names"]

                # Ensure all data rows exist before attempting to access
                if current_row_idx >= len(all_data_tables) or \
                   previous_row_idx >= len(all_data_tables) or \
                   (weekly_change_row_idx_info is not None and weekly_change_row_idx_info[0] >= len(all_data_tables)):
                    print(f"WARNING: Not enough rows in '{WORKSHEET_NAME_TABLES}' for section {section_key} table data. Skipping.")
                    processed_table_data_by_section[section_key] = {"headers": table_headers, "rows": []}
                    continue

                current_data_row = all_data_tables[current_row_idx]
                previous_data_row = all_data_tables[previous_row_idx]
                weekly_change_data_row = all_data_tables[weekly_change_row_idx_info[0]] if weekly_change_row_idx_info is not None else None # Use first col of range for row index

                num_data_points = len(route_names)

                for i in range(num_data_points):
                    route_name = route_names[i]
                    
                    current_index_val = None
                    previous_index_val = None
                    weekly_change = None

                    # Fetch current index
                    col_idx_current = current_cols_start + i
                    if col_idx_current <= current_cols_end and col_idx_current < len(current_data_row):
                        val = str(current_data_row[col_idx_current]).strip().replace(',', '')
                        current_index_val = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None

                    # Fetch previous index
                    col_idx_previous = previous_cols_start + i
                    if col_idx_previous <= previous_cols_end and col_idx_previous < len(previous_data_row):
                        val = str(previous_data_row[col_idx_previous]).strip().replace(',', '')
                        previous_index_val = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                    
                    # Fetch weekly change or calculate it
                    if weekly_change_data_row is not None:
                        col_idx_weekly_change = weekly_change_cols_start + i
                        if col_idx_weekly_change <= weekly_change_cols_end and col_idx_weekly_change < len(weekly_change_data_row):
                            val = str(weekly_change_data_row[col_idx_weekly_change]).strip().replace(',', '')
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
                                try:
                                    change_val_only = float(val.replace('%', ''))
                                    color_class = "text-gray-700"
                                    if change_val_only > 0:
                                        color_class = "text-red-500"
                                    elif change_val_only < 0:
                                        color_class = "text-blue-500"
                                    weekly_change = {
                                        "value": f"{change_val_only:.2f}",
                                        "percentage": f"{change_val_only:.2f}%" if '%' not in val else val,
                                        "color_class": color_class
                                    }
                                    if current_index_val is not None and previous_index_val is not None and previous_index_val != 0:
                                        calculated_change_percentage = ((current_index_val - previous_index_val) / previous_index_val) * 100
                                        weekly_change["percentage"] = f"{calculated_change_percentage:.2f}%"
                                except ValueError:
                                    weekly_change = None
                            else:
                                weekly_change = None
                        else: # Calculate weekly change if no explicit row is provided (e.g., Blank Sailing for some calculation)
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
