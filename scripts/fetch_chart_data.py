import gspread
import json
import os
import pandas as pd
import traceback
import re
from datetime import datetime
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
# The keys in 'data_cols_map' MUST exactly match the headers in the Google Sheet's row 2.
# The values in 'data_cols_map' are the desired final JSON keys, following the IndexName_RouteName format.
SECTION_COLUMN_MAPPINGS = {
    "KCCI": {
        "date_col_idx": 0, # A열은 날짜
        "data_start_col_idx": 1, # B열은 종합지수
        "data_end_col_idx": 14, # O열은 동남아시아
        "data_cols_map": {
            "종합지수(Point)와 그 외 항로별($/FEU)": "KCCI_Date", # Corrected date header to exact sheet header
            "종합지수": "KCCI_Composite_Index",
            "미주서안": "KCCI_US_West_Coast",
            "미주동안": "KCCI_US_East_Coast",
            "유럽": "KCCI_Europe",
            "지중해": "KCCI_Mediterranean",
            "중동": "KCCI_Middle_East",
            "호주": "KCCI_Australia",
            "남미동안": "KCCI_South_America_East_Coast",
            "남미서안": "KCCI_South_America_West_Coast",
            "남아프리카": "KCCI_South_Africa",
            "서아프리카": "KCCI_West_Africa",
            "중국": "KCCI_China",
            "일본": "KCCI_Japan",
            "동남아시아": "KCCI_Southeast_Asia"
        }
    },
    "SCFI": {
        "date_col_idx": 15, # Q열은 날짜
        "data_start_col_idx": 17, # R열은 종합지수
        "data_end_col_idx": 30, # AE열은 남아공
        "data_cols_map": {
            "": "SCFI_Date", # Corrected date header to empty string as per console log
            "종합지수": "SCFI_Composite_Index",
            "미주서안": "SCFI_US_West_Coast",
            "미주동안": "SCFI_US_East_Coast",
            "북유럽": "SCFI_North_Europe",
            "지중해": "SCFI_Mediterranean",
            "동남아시아": "SCFI_Southeast_Asia",
            "중동": "SCFI_Middle_East",
            "호주/뉴질랜드": "SCFI_Australia_New_Zealand",
            "남아메리카": "SCFI_South_America",
            "일본서안": "SCFI_Japan_West_Coast",
            "일본동안": "SCFI_Japan_East_Coast",
            "한국": "SCFI_Korea",
            "동부/서부 아프리카": "SCFI_East_West_Africa",
            "남아공": "SCFI_South_Africa"
        }
    },
    "WCI": {
        "date_col_idx": 32, # AG열은 날짜
        "data_start_col_idx": 33, # AH열은 종합지수
        "data_end_col_idx": 41, # AP열은 로테르담 → 뉴욕
        "data_cols_map": {
            "종합지수와 각 항로별($/FEU)": "WCI_Date", # Corrected date header to exact sheet header
            "종합지수": "WCI_Composite_Index",
            "상하이 → 로테르담": "WCI_Shanghai_Rotterdam",
            "로테르담 → 상하이": "WCI_Rotterdam_Shanghai",
            "상하이 → 제노바": "WCI_Shanghai_Genoa",
            "상하이 → 로스엔젤레스": "WCI_Shanghai_Los_Angeles",
            "로스엔젤레스 → 상하이": "WCI_Los_Angeles_Shanghai",
            "상하이 → 뉴욕": "WCI_Shanghai_New_York",
            "뉴욕 → 로테르담": "WCI_New_York_Rotterdam",
            "로테르담 → 뉴욕": "WCI_Rotterdam_New_York",
        }
    },
    "IACI": {
        "date_col_idx": 43, # AR열은 날짜
        "data_start_col_idx": 44, # AS열은 종합지수
        "data_end_col_idx": 44, # AS열
        "data_cols_map": {
            "date": "IACI_Date", # Corrected date header to exact sheet header
            "종합지수": "IACI_Composite_Index"
        }
    },
    "BLANK_SAILING": {
        "date_col_idx": 46, # AU열은 날짜
        "data_start_col_idx": 47, # AV열은 Index
        "data_end_col_idx": 52, # BA열은 Total
        "data_cols_map": {
            "Index": "BLANK_SAILING_Date", # Corrected date header to exact sheet header
            "Gemini Cooperation": "BLANK_SAILING_Gemini_Cooperation",
            "MSC": "BLANK_SAILING_MSC", # Changed to just MSC, without Alliance
            "OCEAN Alliance": "BLANK_SAILING_OCEAN_Alliance",
            "Premier Alliance": "BLANK_SAILING_Premier_Alliance",
            "Others/Independent": "BLANK_SAILING_Others_Independent",
            "Total": "BLANK_SAILING_Total"
        }
    },
    "FBX": {
        "date_col_idx": 54, # BC열은 날짜
        "data_start_col_idx": 55, # BD열은 종합지수
        "data_end_col_idx": 67, # BP열은 유럽 → 남미서안
        "data_cols_map": {
            "종합지수와 각 항로별($/FEU)": "FBX_Date", # Date header
            # These keys MUST match the actual headers in the Google Sheet's row 2.
            # Values are the desired JSON keys, prefixed with "FBX_".
            "글로벌 컨테이너 운임 지수": "FBX_Composite_Index",
            "중국/동아시아 → 미주서안": "FBX_China_EA_US_West_Coast",
            "미주서안 → 중국/동아시아": "FBX_US_West_Coast_China_EA",
            "중국/동아시아 → 미주동안": "FBX_China_EA_US_East_Coast",
            "미주동안 → 중국/동아시아": "FBX_US_East_Coast_China_EA",
            "중국/동아시아 → 북유럽": "FBX_China_EA_North_Europe",
            "북유럽 → 중국/동아시아": "FBX_North_Europe_China_EA",
            "중국/동아시아 → 지중해": "FBX_China_EA_Mediterranean",
            "지중해 → 중국/동아시아": "FBX_Mediterranean_China_EA",
            "미주동안 → 북유럽": "FBX_US_East_Coast_North_Europe",
            "북유럽 → 미주동안": "FBX_North_Europe_US_East_Coast",
            "유럽 → 남미동안": "FBX_Europe_South_America_East_Coast",
            "유럽 → 남미서안": "FBX_Europe_South_America_West_Coast",
        }
    },
    "XSI": {
        "date_col_idx": 69, # BR열은 날짜
        "data_start_col_idx": 70, # BS열은 동아시아 → 북유럽
        "data_end_col_idx": 77, # BZ열은 북유럽 → 남미동안
        "data_cols_map": {
            "각 항로별($/FEU)": "XSI_Date", # Date header
            # These keys MUST match the actual headers in the Google Sheet's row 2.
            # Values are the desired JSON keys, prefixed with "XSI_".
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
        "data_end_col_idx": 80, # Assuming H column is also data
        "data_cols_map": {
            "Index(종합지수)": "MBCI_Date", # Date header
            "MBCI": "MBCI_Value", # Changed to MBCI_Value (no double prefix)
        }
    }
}

# Global dictionary to map original sheet headers to their final JSON keys
# This will be populated once from SECTION_COLUMN_MAPPINGS
# This map is now primarily for debugging and reference, as column renaming is done per section DataFrame.
ORIGINAL_HEADER_TO_FINAL_KEY_MAP = {}
for section_key, details in SECTION_COLUMN_MAPPINGS.items():
    for original_header, final_key in details["data_cols_map"].items():
        ORIGINAL_HEADER_TO_FINAL_KEY_MAP[original_header] = final_key


def fetch_and_process_data():
    """
    Google Sheet에서 데이터를 가져와 처리하고 JSON 파일로 저장합니다.
    컬럼형 데이터, 날씨 데이터, 환율 데이터를 처리합니다.
    """
    if not SPREADSHEET_ID or not GOOGLE_CREDENTIAL_JSON:
        print("오류: SPREADSHEET_ID 또는 GOOGLE_CREDENTIAL_JSON 환경 변수가 설정되지 않았습니다.")
        if not SPREADSHEET_ID:
            print("이유: SPREADSHEET_ID가 None입니다.")
        if not GOOGLE_CREDENTIAL_JSON:
            print("이유: GOOGLE_CREDENTIAL_JSON이 None입니다.")
        return

    try:
        credentials_dict = json.loads(GOOGLE_CREDENTIAL_JSON)
        gc = gspread.service_account_from_dict(credentials_dict)
        
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)

        # --- Fetch Main Chart Data (from Crawling_Data) ---
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME_CHARTS)
        all_data_charts = worksheet.get_all_values()

        print(f"DEBUG: Total rows fetched from Google Sheet (raw): {len(all_data_charts)}")

        if not all_data_charts:
            print("Error: No data fetched from the main chart sheet.")
            return

        # The header row is explicitly stated as row 2 (index 1)
        main_header_row_index = 1 
        if len(all_data_charts) <= main_header_row_index:
            print(f"Error: '{WORKSHEET_NAME_CHARTS}' sheet does not have enough rows for header at index {main_header_row_index}.")
            return

        raw_headers_full_charts = [str(h).strip().replace('"', '') for h in all_data_charts[main_header_row_index]]
        print(f"DEBUG: '{WORKSHEET_NAME_CHARTS}'에서 가져온 원본 헤더 (전체 행): {raw_headers_full_charts}")

        # Create a DataFrame from all raw data, using the raw headers as column names.
        # Pandas will handle rows with fewer columns by filling with NaN if needed.
        # It will also allow access to columns with duplicate names (e.g., df['종합지수'] will return a DataFrame with all such columns).
        data_rows_for_df = all_data_charts[main_header_row_index + 1:]
        df_raw_full = pd.DataFrame(data_rows_for_df, columns=raw_headers_full_charts)
        print(f"DEBUG: Raw full DataFrame shape with original headers: {df_raw_full.shape}")

        processed_chart_data_by_section = {}

        # Process each section for chart data
        for section_key, details in SECTION_COLUMN_MAPPINGS.items():
            date_col_idx_in_raw = details["date_col_idx"]
            data_start_col_idx_in_raw = details["data_start_col_idx"]
            data_end_col_idx_in_raw = details["data_end_col_idx"]
            
            # 1. Identify the raw column indices for this specific section
            raw_column_indices_for_section = [date_col_idx_in_raw] + list(range(data_start_col_idx_in_raw, data_end_col_idx_in_raw + 1))
            
            # Filter out any indices that are beyond the actual number of columns in raw_headers_full_charts
            valid_raw_column_indices = [idx for idx in raw_column_indices_for_section if idx < len(raw_headers_full_charts)]

            if not valid_raw_column_indices:
                print(f"WARNING: No valid column indices found for section {section_key}. Skipping chart data processing for this section.")
                processed_chart_data_by_section[section_key] = []
                continue

            # 2. Extract the section's data from the full raw DataFrame using integer-location (iloc)
            # This ensures we get the columns by their position, regardless of duplicate names.
            df_section_raw_cols = df_raw_full.iloc[:, valid_raw_column_indices].copy()
            
            # 3. Get the actual raw header names corresponding to the extracted columns
            actual_raw_headers_in_section_df = [raw_headers_full_charts[idx] for idx in valid_raw_column_indices]
            df_section_raw_cols.columns = actual_raw_headers_in_section_df # Assign these raw headers to the temporary DataFrame

            print(f"DEBUG: {section_key} - Raw columns in section DataFrame before renaming: {df_section_raw_cols.columns.tolist()}")

            # 4. Create a renaming map from the actual raw headers in df_section_raw_cols to the desired JSON keys
            rename_map = {}
            # Iterate through the data_cols_map for this section to build the rename map
            for original_header_from_map, final_json_key in details["data_cols_map"].items():
                # Check if this original_header_from_map is actually in the current section's raw headers
                # (This handles cases where a generic map entry might not apply to a specific section's extracted columns)
                if original_header_from_map in actual_raw_headers_in_section_df:
                    rename_map[original_header_from_map] = final_json_key
                else:
                    print(f"WARNING: Header '{original_header_from_map}' from SECTION_COLUMN_MAPPINGS for {section_key} was not found in the extracted raw columns. It will not be renamed.")

            print(f"DEBUG: {section_key} - Constructed rename_map: {rename_map}")

            # 5. Rename the columns of the section DataFrame to the desired JSON keys
            df_section = df_section_raw_cols.rename(columns=rename_map)
            print(f"DEBUG: {section_key} - Columns in section DataFrame after renaming: {df_section.columns.tolist()}")

            # 6. Identify the date column and data columns using their *final JSON keys*
            # The first key in data_cols_map is always the date column's original header.
            date_original_header = list(details["data_cols_map"].keys())[0]
            date_col_final_name = details["data_cols_map"][date_original_header]
            
            # Collect all final JSON keys for data columns in this section (excluding the date column's final key)
            section_data_col_final_names = [
                final_json_key for original_header, final_json_key in details["data_cols_map"].items()
                if original_header != date_original_header
            ]
            
            # Ensure the date column exists after renaming
            if date_col_final_name not in df_section.columns:
                print(f"ERROR: Date column '{date_col_final_name}' not found in section {section_key} after renaming. Skipping.")
                processed_chart_data_by_section[section_key] = []
                continue

            # 7. Clean and parse dates
            df_section[date_col_final_name] = df_section[date_col_final_name].astype(str).str.strip()
            df_section['parsed_date'] = pd.to_datetime(df_section[date_col_final_name], errors='coerce')
            
            unparseable_dates_series = df_section[df_section['parsed_date'].isna()][date_col_final_name]
            num_unparseable_dates = unparseable_dates_series.count()
            if num_unparseable_dates > 0:
                print(f"WARNING: {num_unparseable_dates} dates could not be parsed for {section_key}. Sample unparseable date strings: {unparseable_dates_series.head().tolist()}")

            df_section.dropna(subset=['parsed_date'], inplace=True)
            print(f"DEBUG: DataFrame shape for {section_key} after date parsing and dropna: {df_section.shape}")

            # 8. Convert numeric columns
            for col_final_name in section_data_col_final_names:
                if col_final_name in df_section.columns: # Ensure column exists before converting
                    df_section[col_final_name] = pd.to_numeric(df_section[col_final_name].astype(str).str.replace(',', ''), errors='coerce')
                else:
                    print(f"WARNING: Data column '{col_final_name}' not found in section {section_key} after renaming. It might not be included in the output.")
            
            df_section = df_section.replace({pd.NA: None, float('nan'): None})

            # 9. Sort and format date for final output
            df_section = df_section.sort_values(by='parsed_date', ascending=True)
            df_section['date'] = df_section['parsed_date'].dt.strftime('%Y-%m-%d')
            
            # 10. Select final columns for output
            output_cols = ['date'] + section_data_col_final_names
            existing_output_cols = [col for col in output_cols if col in df_section.columns]
            
            processed_chart_data_by_section[section_key] = df_section[existing_output_cols].to_dict(orient='records')
            print(f"DEBUG: {section_key}의 처리된 차트 데이터 (처음 3개 항목): {processed_chart_data_by_section[section_key][:3]}")
            print(f"DEBUG: {section_key}의 처리된 차트 데이터 (마지막 3개 항목): {processed_chart_data_by_section[section_key][-3:]}")


        # --- Crawling_Data2 시트에서 테이블 데이터 가져오기 ---
        worksheet_tables = spreadsheet.worksheet(WORKSHEET_NAME_TABLES)
        all_data_tables = worksheet_tables.get_all_values()

        print(f"디버그: '{WORKSHEET_NAME_TABLES}'에서 가져온 총 행 수 (원본): {len(all_data_tables)}")

        if not all_data_tables:
            print(f"오류: '{WORKSHEET_NAME_TABLES}' 시트에서 데이터를 가져오지 못했습니다. 테이블 데이터가 비어 있습니다.")

        # 각 섹션의 테이블 데이터를 직접 셀 참조를 사용하여 준비
        processed_table_data = {} # Initialize here
        for section_key, table_details in TABLE_DATA_CELL_MAPPINGS.items():
            table_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
            table_rows_data = []

            # 여러 이전 날짜/데이터 행이 있는 Blank Sailing 처리
            if section_key == "BLANK_SAILING" and "previous_date_cells_and_ranges" in table_details:
                current_row_idx = table_details["current_date_cell"][0]
                current_cols_start, current_cols_end = table_details["current_index_cols_range"]
                route_names = table_details["route_names"]
                
                current_data_row = all_data_tables[current_row_idx] if current_row_idx < len(all_data_tables) else []

                # 변경 사항 계산을 위해 Blank Sailing의 모든 과거 데이터 수집
                blank_sailing_historical_data = []
                
                # 현재 데이터 추가
                current_bs_entry = {"date": (all_data_tables[current_row_idx][table_details["current_date_cell"][1]] if current_row_idx < len(all_data_tables) and table_details["current_date_cell"][1] < len(all_data_tables[current_row_idx]) else "")}
                for i in range(len(route_names)):
                    col_idx = current_cols_start + i
                    if col_idx <= current_cols_end and col_idx < len(current_data_row):
                        val = str(current_data_row[col_idx]).strip().replace(',', '')
                        current_bs_entry[route_names[i]] = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                blank_sailing_historical_data.append(current_bs_entry)

                # 이전 데이터 행 추가
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
                
                # 올바른 이전 값 계산을 위해 날짜별로 과거 데이터 정렬
                blank_sailing_historical_data.sort(key=lambda x: pd.to_datetime(x['date'], errors='coerce', dayfirst=False) if x['date'] else pd.Timestamp.min)

                # 이제 Blank Sailing의 table_rows_data 채우기
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
                            "route": f"{section_key}_{route_name}", # Prefix with section key
                            "current_index": current_index_val,
                            "previous_index": previous_index_val,
                            "weekly_change": weekly_change
                        })
            else: # 다른 모든 섹션의 경우
                current_row_idx = table_details["current_date_cell"][0]
                previous_row_idx = table_details["previous_date_cell"][0]
                weekly_change_row_idx_info = table_details["weekly_change_cols_range"] # 이제 튜플 (start_col, end_col) 또는 None

                current_cols_start, current_cols_end = table_details["current_index_cols_range"]
                previous_cols_start, previous_cols_end = table_details["previous_index_cols_range"]
                
                weekly_change_cols_start, weekly_change_cols_end = (None, None)
                if weekly_change_row_idx_info is not None:
                    weekly_change_cols_start, weekly_change_cols_end = weekly_change_row_idx_info

                route_names = table_details["route_names"]

                # 접근하기 전에 모든 데이터 행이 있는지 확인
                if current_row_idx >= len(all_data_tables) or \
                   previous_row_idx >= len(all_data_tables) or \
                   (weekly_change_row_idx_info is not None and weekly_change_row_idx_info[0] >= len(all_data_tables)):
                    print(f"경고: '{WORKSHEET_NAME_TABLES}'에 섹션 {section_key}의 테이블 데이터에 충분한 행이 없습니다. 건너뜁니다.")
                    processed_table_data[section_key] = {"headers": table_headers, "rows": []}
                    continue

                current_data_row = all_data_tables[current_row_idx]
                previous_data_row = all_data_tables[previous_row_idx]
                weekly_change_data_row = all_data_tables[weekly_change_row_idx_info[0]] if weekly_change_row_idx_info is not None else None # 행 인덱스에 범위의 첫 번째 컬럼 사용

                num_data_points = len(route_names)

                for i in range(num_data_points):
                    route_name = route_names[i]
                    
                    current_index_val = None
                    previous_index_val = None
                    weekly_change = None

                    # 현재 인덱스 가져오기
                    col_idx_current = current_cols_start + i
                    if col_idx_current <= current_cols_end and col_idx_current < len(current_data_row):
                        val = str(current_data_row[col_idx_current]).strip().replace(',', '')
                        current_index_val = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None

                    # 이전 인덱스 가져오기
                    col_idx_previous = previous_cols_start + i
                    if col_idx_previous <= previous_cols_end and col_idx_previous < len(previous_data_row):
                        val = str(previous_data_row[col_idx_previous]).strip().replace(',', '')
                        previous_index_val = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                    
                    # 주간 변화 가져오기 또는 계산
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
                        else: # 명시적인 행이 제공되지 않으면 주간 변화 계산 (예: Blank Sailing의 일부 계산)
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
                        "route": f"{section_key}_{route_name}", # 테이블 항로에 원본 헤더 이름(한국어) 사용
                        "current_index": current_index_val,
                        "previous_index": previous_index_val,
                        "weekly_change": weekly_change
                    })
            
            processed_table_data[section_key] = {
                "headers": table_headers,
                "rows": table_rows_data
            }
            print(f"디버그: {section_key}의 처리된 테이블 데이터 (처음 3개 항목): {processed_table_data[section_key]['rows'][:3]}")


        # --- 날씨 데이터 가져오기 ---
        weather_worksheet = spreadsheet.worksheet(WEATHER_WORKSHEET_NAME)
        weather_data_raw = weather_worksheet.get_all_values()
        
        current_weather = {}
        if len(weather_data_raw) >= 9: # 현재 날씨에 충분한 행이 있는지 확인
            current_weather['LA_WeatherStatus'] = weather_data_raw[0][1] if len(weather_data_raw[0]) > 1 else None
            current_weather['LA_WeatherIcon'] = weather_data_raw[1][1] if len(weather_data_raw[1]) > 1 else None
            # 온도, 습도 등을 숫자로 변환
            current_weather['LA_Temperature'] = float(weather_data_raw[2][1]) if len(weather_data_raw[2]) > 1 and weather_data_raw[2][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Humidity'] = float(weather_data_raw[3][1]) if len(weather_data_raw[3]) > 1 and weather_data_raw[3][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_WindSpeed'] = float(weather_data_raw[4][1]) if len(weather_data_raw[4]) > 1 and weather_data_raw[4][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Pressure'] = float(weather_data_raw[5][1]) if len(weather_data_raw[5]) > 1 and weather_data_raw[5][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Visibility'] = float(weather_data_raw[6][1]) if len(weather_data_raw[6]) > 1 and weather_data_raw[6][1].replace('.', '', 1).isdigit() else None
            current_weather['LA_Sunrise'] = weather_data_raw[7][1] if len(weather_data_raw[7]) > 1 else None
            current_weather['LA_Sunset'] = weather_data_raw[8][1] if len(weather_data_raw[8]) > 1 else None
            current_weather['LA_FineDust'] = None # 시트에 없는 미세먼지 플레이스홀더

        forecast_weather = []
        if len(weather_data_raw) > 12: # 예보 데이터가 있는지 확인 (12행, 인덱스 11부터 시작)
            for row in weather_data_raw[11:]: # 12행부터
                if len(row) >= 5 and row[0]: # 날짜 및 기본 정보가 있는지 확인
                    forecast_day = {
                        'date': row[0],
                        'min_temp': float(row[1]) if row[1] and row[1].replace('.', '', 1).isdigit() else None,
                        'max_temp': float(row[2]) if row[2] and row[2].replace('.', '', 1).isdigit() else None,
                        'status': row[3],
                        'icon': row[4] # 아이콘 이름 또는 경로 가정
                    }
                    forecast_weather.append(forecast_day)
        
        print(f"DEBUG: Current Weather Data: {current_weather}")
        print(f"DEBUG: Forecast Weather Data (first 3): {forecast_weather[:3]}")

        # --- 환율 데이터 가져오기 ---
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

# Helper to safely get a cell value, returning empty string if out of bounds.
def get_cell_value(data, row_idx, col_idx):
    """Safely retrieves a cell value, returning empty string if out of bounds."""
    try:
        # Attempt to access data[row_idx][col_idx] directly
        # This will raise IndexError if row_idx or col_idx is out of bounds
        # It will raise TypeError if data[row_idx] is not a list/tuple
        value = data[row_idx][col_idx]
        return str(value).strip()
    except (IndexError, TypeError):
        return ''

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
    # KCCI (날짜 표기 형식: Current Index (2025-07-21), Previous Index (2025-07-14))
    # Current date: A3 (row 2, col 0)
    # Current Index data: B3:O3 (row 2, cols 1-14)
    # Previous date: A4 (row 3, col 0)
    # Previous Index data: B4:O4 (cols 1 to 14)
    
    kcci_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    kcci_rows = []

    kcci_current_date_raw_label = get_cell_value(raw_data, 2, 0) # A3
    kcci_previous_date_raw_label = get_cell_value(raw_data, 3, 0) # A4

    # Extract date from "Current Index (YYYY-MM-DD)" and "Previous Index (YYYY-MM-DD)"
    current_date_match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", kcci_current_date_raw_label)
    previous_date_match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", kcci_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(1), "%Y-%m-%d")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(1), "%Y-%m-%d")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    kcci_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    kcci_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # KCCI routes and their corresponding 0-indexed column in the sheet
    kcci_routes_data_cols = {
        "종합지수": 1, # B column
        "미주서안": 2, # C column
        "미주동안": 3, # D column
        "유럽": 4, # E column
        "지중해": 5, # F column
        "중동": 6, # G column
        "호주": 7, # H column
        "남미동안": 8, # I column
        "남미서안": 9, # J column
        "남아프리카": 10, # K column
        "서아프리카": 11, # L column
        "중국": 12, # M column
        "일본": 13, # N column
        "동남아시아": 14 # O column
    }

    for route_name, col_idx in kcci_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 2, col_idx) # B3 to O3
        previous_val = get_cell_value(raw_data, 3, col_idx) # B4 to O4
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        kcci_rows.append({
            "route": f"KCCI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["KCCI"] = {"headers": kcci_display_headers, "rows": kcci_rows}


    # --- SCFI Table ---
    # SCFI (날짜 표기 형식: Current Index (2025-07-18), Previous Index (2025-07-11))
    # Current date: A9 (row 8, col 0)
    # Current Index data: B9:O9 (row 8, cols 1-14)
    # Previous date: A10 (row 9, col 0)
    # Previous Index data: B10:O10 (row 9, cols 1-14)

    scfi_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    scfi_rows = []

    scfi_current_date_raw_label = get_cell_value(raw_data, 8, 0) # A9
    scfi_previous_date_raw_label = get_cell_value(raw_data, 9, 0) # A10

    current_date_match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", scfi_current_date_raw_label)
    previous_date_match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", scfi_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(1), "%Y-%m-%d")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(1), "%Y-%m-%d")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    scfi_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    scfi_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # SCFI routes and their corresponding 0-indexed column in the sheet - FIXED KEY-VALUE PAIRS
    scfi_routes_data_cols = {
        "종합지수": 1, # B column
        "유럽 (기본항)": 2, # C column
        "지중해 (기본항)": 3, # D column
        "미주서안 (기본항)": 4, # E column
        "미주동안 (기본항)": 5, # F column
        "페르시아만/홍해 (두바이)": 6, # G column
        "호주/뉴질랜드 (멜버른)": 7, # H column
        "동/서 아프리카 (라고스)": 8, # I column
        "남아프리카 (더반)": 9, # J column
        "서일본 (기본항)": 10, # K column
        "동일본 (기본항)": 11, # L column
        "동남아시아 (싱가포르)": 12, # M column
        "한국 (부산)": 13, # N column
        "중남미서안 (만사니요)": 14 # O column
    }

    for route_name, col_idx in scfi_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 8, col_idx) # B9 to O9
        previous_val = get_cell_value(raw_data, 9, col_idx) # B10 to O10
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        scfi_rows.append({
            "route": f"SCFI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["SCFI"] = {"headers": scfi_display_headers, "rows": scfi_rows}


    # --- WCI Table ---
    # WCI (날짜표기형식: 7/17/2025, 7/10/2025)
    # Current date: A21 (row 20, col 0)
    # Current Index data: B21:J21 (row 20, cols 1-9)
    # Previous date: A22 (row 21, col 0)
    # Previous Index data: B22:J22 (row 21, cols 1-9)

    wci_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    wci_rows = []

    wci_current_date_raw_label = get_cell_value(raw_data, 20, 0) # A21
    wci_previous_date_raw_label = get_cell_value(raw_data, 21, 0) # A22

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", wci_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", wci_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    wci_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    wci_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # WCI routes and their corresponding 0-indexed column in the sheet - FIXED KEY-VALUE PAIRS
    wci_routes_data_cols = {
        "종합지수": 1, # B column
        "상하이 → 로테르담": 2, # C column
        "로테르담 → 상하이": 3, # D column
        "상하이 → 제노바": 4, # E column
        "상하이 → 로스엔젤레스": 5, # F column
        "로스엔젤레스 → 상하이": 6, # G column
        "상하이 → 뉴욕": 7, # H column
        "뉴욕 → 로테르담": 8, # I column
        "로테르담 → 뉴욕": 9 # J column
    }

    for route_name, col_idx in wci_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 20, col_idx) # B21 to J21
        previous_val = get_cell_value(raw_data, 21, col_idx) # B22 to J22
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        wci_rows.append({
            "route": f"WCI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["WCI"] = {"headers": wci_display_headers, "rows": wci_rows}


    # --- IACI Table ---
    # IACI (날짜 표기 형식: 7/15/2025, 6/30/2025)
    # Current date: A27 (row 26, col 0)
    # Current Index data: B27 (row 26, col 1)
    # Previous date: A28 (row 27, col 0)
    # Previous Index data: B28 (row 27, col 1)

    iaci_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    iaci_rows = []

    iaci_current_date_raw_label = get_cell_value(raw_data, 26, 0) # A27
    iaci_previous_date_raw_label = get_cell_value(raw_data, 27, 0) # A28

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", iaci_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", iaci_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    iaci_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    iaci_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # IACI routes and their corresponding 0-indexed column in the sheet - FIXED KEY-VALUE PAIRS
    iaci_routes_data_cols = {
        "종합지수": 1 # B column
    }

    for route_name, col_idx in iaci_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 26, col_idx) # B27
        previous_val = get_cell_value(raw_data, 27, col_idx) # B28
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        iaci_rows.append({
            "route": f"IACI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["IACI"] = {"headers": iaci_display_headers, "rows": iaci_rows}


    # --- BLANK SAILING Table ---
    # BLANK SAILING (날짜 표기 형식: 7/18/2025, 7/11/2025, 7/4/2025, 6/27/2025, 6/20/2025)
    # Current date: A33 (row 32, col 0)
    # Current Index data: B33:G33 (row 32, cols 1-6)
    # Previous date_1: A34 (row 33, col 0)
    # Previous Index data_1: B34:G34 (row 33, cols 1-6)

    blank_sailing_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    blank_sailing_rows = []

    blank_sailing_current_date_raw_label = get_cell_value(raw_data, 32, 0) # A33
    blank_sailing_previous_date_raw_label = get_cell_value(raw_data, 33, 0) # A34

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", blank_sailing_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", blank_sailing_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    blank_sailing_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    blank_sailing_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # BLANK SAILING routes and their corresponding 0-indexed column in the sheet - FIXED KEY-VALUE PAIRS
    blank_sailing_routes_data_cols = {
        "Gemini Cooperation": 1, # B column
        "MSC": 2, # C column
        "OCEAN Alliance": 3, # D column
        "Premier Alliance": 4, # E column
        "Others/Independent": 5, # F column
        "Total": 6 # G column
    }

    for route_name, col_idx in blank_sailing_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 32, col_idx) # B33 to G33
        previous_val = get_cell_value(raw_data, 33, col_idx) # B34 to G34
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        blank_sailing_rows.append({
            "route": f"BLANK_SAILING_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["BLANK_SAILING"] = {"headers": blank_sailing_display_headers, "rows": blank_sailing_rows}


    # --- FBX Table ---
    # FBX (날짜 표기 형식: 7/18/2025, 7/11/2025)
    # Current date: A41 (row 40, col 0)
    # Current Index data: B41:N41 (row 40, cols 1-13)
    # Previous date: A42 (row 41, col 0)
    # Previous Index data: B42:N41 (row 41, cols 1-13)

    fbx_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    fbx_rows = []

    fbx_current_date_raw_label = get_cell_value(raw_data, 40, 0) # A41
    fbx_previous_date_raw_label = get_cell_value(raw_data, 41, 0) # A42

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", fbx_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", fbx_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    fbx_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    fbx_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # FBX routes and their corresponding 0-indexed column in the sheet - FIXED KEY-VALUE PAIRS
    fbx_routes_data_cols = {
        "글로벌 컨테이너 운임 지수": 1, # B column
        "중국/동아시아 → 미주서안": 2, # C column
        "미주서안 → 중국/동아시아": 3, # D column
        "중국/동아시아 → 미주동안": 4, # E column
        "미주동안 → 중국/동아시아": 5, # F column
        "중국/동아시아 → 북유럽": 6, # G column
        "북유럽 → 중국/동아시아": 7, # H column
        "중국/동아시아 → 지중해": 8, # I column
        "지중해 → 중국/동아시아": 9, # J column
        "미주동안 → 북유럽": 10, # K column
        "북유럽 → 미주동안": 11, # L column
        "유럽 → 남미동안": 12, # M column
        "유럽 → 남미서안": 13 # N column
    }

    for route_name, col_idx in fbx_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 40, col_idx) # B41 to N41
        previous_val = get_cell_value(raw_data, 41, col_idx) # B42 to N42
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        fbx_rows.append({
            "route": f"FBX_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["FBX"] = {"headers": fbx_display_headers, "rows": fbx_rows}


    # --- XSI Table ---
    # XSI (날짜 표기 형식: 7/23/2025, 7/16/2025)
    # Current date: A47 (row 46, col 0)
    # Current Index data: B47:I47 (row 46, cols 1-8)
    # Previous date: A48 (row 47, col 0)
    # Previous Index data: B48:N48 (row 47, cols 1-13)

    xsi_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    xsi_rows = []

    xsi_current_date_raw_label = get_cell_value(raw_data, 46, 0) # A47
    xsi_previous_date_raw_label = get_cell_value(raw_data, 47, 0) # A48

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", xsi_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", xsi_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    xsi_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    xsi_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    xsi_routes_data_cols = {
        "동아시아 → 북유럽": {"current_col": 1, "previous_col": 1}, # B47, B48
        "북유럽 → 동아시아": {"current_col": 2, "previous_col": 2}, # C47, C48
        "동아시아 → 미주서안": {"current_col": 3, "previous_col": 3}, # D47, D48
        "미주서안 → 동아시아": {"current_col": 4, "previous_col": 4}, # E47, E48
        "동아시아 → 남미동안": {"current_col": 5, "previous_col": 5}, # F47, F48
        "북유럽 → 미주동안": {"current_col": 6, "previous_col": 6}, # G47, G48
        "미주동안 → 북유럽": {"current_col": 7, "previous_col": 7}, # H47, H48
        "북유럽 → 남미동안": {"current_col": 8, "previous_col": 8}  # I47, I48
    }
    # Note: User specified Previous Index data: B48:N48. However, Current Index data is B47:I47.
    # To maintain consistency in "route" mapping and calculation, I will use the corresponding column for previous data.
    # If a route has current data in col X, its previous data will be taken from col X in the previous row.
    # The range B48:N48 might contain additional data not directly corresponding to the routes in B47:I47.
    # If the user intends to show more previous data, the table structure would need a more complex change.
    # For now, I will align previous_col with current_col for each route.

    for route_name, cols in xsi_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 46, cols["current_col"]) # Current data from row 46 (B-I)
        previous_val = get_cell_value(raw_data, 47, cols["previous_col"]) # Previous data from row 47 (B-I)
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        xsi_rows.append({
            "route": f"XSI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["XSI"] = {"headers": xsi_display_headers, "rows": xsi_rows}


    # --- MBCI Table ---
    # MBCI (날짜 표기 형식: 7/18/2025, 7/11/2025)
    # Current date: A59 (row 58, col 0)
    # Current Index data: G59 (row 58, col 6)
    # Previous date: A60 (row 59, col 0)
    # Previous Index data: G60 (row 59, col 6)

    mbci_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    mbci_rows = []

    mbci_current_date_raw_label = get_cell_value(raw_data, 58, 0) # A59
    mbci_previous_date_raw_label = get_cell_value(raw_data, 59, 0) # A60

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", mbci_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", mbci_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    mbci_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    mbci_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    mbci_routes_data_cols = {
        "MBCI": {"current_col": 6, "previous_col": 6}, # G59, G60
        "Index(종합지수)": {"current_col": 7, "previous_col": 7} # H59, H60
    }

    for route_name, cols in mbci_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 58, cols["current_col"]) # Current data from row 58 (G, H)
        previous_val = get_cell_value(raw_data, 59, cols["previous_col"]) # Previous data from row 59 (G, H)
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        mbci_rows.append({
            "route": f"MBCI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["MBCI"] = {"headers": mbci_display_headers, "rows": mbci_rows}


    return table_data

if __name__ == "__main__":
    fetch_and_process_data()
