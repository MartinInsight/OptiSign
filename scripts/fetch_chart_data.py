import gspread
import json
import os
import pandas as pd
import traceback
import re
from datetime import datetime
import numpy as np # Import numpy for type checking

# 새로 분리된 날씨 데이터 가져오기 함수 임포트
from la_weather_fetcher import fetch_la_weather_data

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
# WEATHER_WORKSHEET_NAME = "LA날씨" # 이제 la_weather_fetcher.py에서 관리
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

# --- Header Mapping Definitions for Table Data (from Crawling_Data2) ---
# These define the cell ranges for current, previous, and weekly change data for the tables.
# Row and column indices are 0-indexed.
TABLE_DATA_CELL_MAPPINGS = {
    "KCCI": {
        "current_date_cell": (0, 1), # B1 (날짜)
        "current_index_cols_range": (1, 14), # B2-O2 (종합지수 ~ 동남아시아)
        "previous_date_cell": (0, 15), # P1 (이전 날짜)
        "previous_index_cols_range": (1, 15), # P2-AD2 (이전 종합지수 ~ 이전 동남아시아)
        "weekly_change_cols_range": (2, 15), # P3-AD3 (주간 변화)
        "route_names": [
            "종합지수", "미주서안", "미주동안", "유럽", "지중해", "중동",
            "호주", "남미동안", "남미서안", "남아프리카", "서아프리카",
            "중국", "일본", "동남아시아"
        ]
    },
    "SCFI": {
        "current_date_cell": (0, 16), # Q1 (날짜)
        "current_index_cols_range": (1, 29), # Q2-AE2 (종합지수 ~ 남아공)
        "previous_date_cell": (0, 30), # AF1 (이전 날짜)
        "previous_index_cols_range": (1, 30), # AF2-AT2 (이전 종합지수 ~ 이전 남아공)
        "weekly_change_cols_range": (2, 30), # AF3-AT3 (주간 변화)
        "route_names": [
            "종합지수", "미주서안", "미주동안", "북유럽", "지중해", "동남아시아",
            "중동", "호주/뉴질랜드", "남아메리카", "일본서안", "일본동안",
            "한국", "동부/서부 아프리카", "남아공"
        ]
    },
    "WCI": {
        "current_date_cell": (0, 31), # AF1 (날짜)
        "current_index_cols_range": (1, 39), # AF2-AN2 (종합지수 ~ 로테르담 → 뉴욕)
        "previous_date_cell": (0, 40), # AO1 (이전 날짜)
        "previous_index_cols_range": (1, 40), # AO2-AW2 (이전 종합지수 ~ 이전 로테르담 → 뉴욕)
        "weekly_change_cols_range": (2, 40), # AO3-AW3 (주간 변화)
        "route_names": [
            "종합지수", "상하이 → 로테르담", "로테르담 → 상하이", "상하이 → 제노바",
            "상하이 → 로스엔젤레스", "로스엔젤레스 → 상하이", "상하이 → 뉴욕",
            "뉴욕 → 로테르담", "로테르담 → 뉴욕"
        ]
    },
    "IACI": {
        "current_date_cell": (0, 41), # AX1 (날짜)
        "current_index_cols_range": (1, 41), # AX2 (종합지수)
        "previous_date_cell": (0, 42), # AY1 (이전 날짜)
        "previous_index_cols_range": (1, 42), # AY2 (이전 종합지수)
        "weekly_change_cols_range": (2, 42), # AY3 (주간 변화)
        "route_names": ["종합지수"]
    },
    "BLANK_SAILING": {
        "current_date_cell": (0, 43), # AZ1 (날짜)
        "current_index_cols_range": (1, 48), # AZ2-BE2 (Gemini Cooperation ~ Total)
        # Blank Sailing은 여러 과거 날짜를 가지고 있으므로, 이전 날짜 및 데이터 범위를 목록으로 정의합니다.
        "previous_date_cells_and_ranges": [
            {"date_cell": (0, 49), "data_range": (1, 54)}, # BF1 (날짜), BF2-BK2 (데이터)
            {"date_cell": (0, 55), "data_range": (1, 60)}, # BL1 (날짜), BL2-BQ2 (데이터)
            {"date_cell": (0, 61), "data_range": (1, 66)}, # BR1 (날짜), BR2-BW2 (데이터)
            {"date_cell": (0, 67), "data_range": (1, 72)}, # BX1 (날짜), BX2-CC2 (데이터)
        ],
        "route_names": [
            "Gemini Cooperation", "MSC", "OCEAN Alliance",
            "Premier Alliance", "Others/Independent", "Total"
        ]
    },
    "FBX": {
        "current_date_cell": (0, 73), # CD1 (날짜)
        "current_index_cols_range": (1, 85), # CD2-CP2 (글로벌 컨테이너 운임 지수 ~ 유럽 → 남미서안)
        "previous_date_cell": (0, 86), # CQ1 (이전 날짜)
        "previous_index_cols_range": (1, 86), # CQ2-DC2 (이전 글로벌 컨테이너 운임 지수 ~ 이전 유럽 → 남미서안)
        "weekly_change_cols_range": (2, 86), # CQ3-DC3 (주간 변화)
        "route_names": [
            "글로벌 컨테이너 운임 지수", "중국/동아시아 → 미주서안", "미주서안 → 중국/동아시아",
            "중국/동아시아 → 미주동안", "미주동안 → 중국/동아시아", "중국/동아시아 → 북유럽",
            "북유럽 → 중국/동아시아", "중국/동아시아 → 지중해", "지중해 → 중국/동아시아",
            "미주동안 → 북유럽", "북유럽 → 미주동안", "유럽 → 남미동안", "유럽 → 남미서안"
        ]
    },
    "XSI": {
        "current_date_cell": (0, 87), # DD1 (날짜)
        "current_index_cols_range": (1, 94), # DD2-DK2 (동아시아 → 북유럽 ~ 북유럽 → 남미동안)
        "previous_date_cell": (0, 95), # DL1 (이전 날짜)
        "previous_index_cols_range": (1, 95), # DL2-DS2 (이전 동아시아 → 북유럽 ~ 이전 북유럽 → 남미동안)
        "weekly_change_cols_range": (2, 95), # DL3-DS3 (주간 변화)
        "route_names": [
            "동아시아 → 북유럽", "북유럽 → 동아시아", "동아시아 → 미주서안",
            "미주서안 → 동아시아", "동아시아 → 남미동안", "북유럽 → 미주동안",
            "미주동안 → 북유럽", "북유럽 → 남미동안"
        ]
    },
    "MBCI": {
        "current_date_cell": (0, 96), # DT1 (날짜)
        "current_index_cols_range": (1, 96), # DT2 (MBCI)
        "previous_date_cell": (0, 97), # DU1 (이전 날짜)
        "previous_index_cols_range": (1, 97), # DU2 (이전 MBCI)
        "weekly_change_cols_range": (2, 97), # DU3 (주간 변화)
        "route_names": ["MBCI"]
    }
}


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


        # --- 날씨 데이터 가져오기 (분리된 함수 사용) ---
        weather_data = fetch_la_weather_data(spreadsheet)
        current_weather = weather_data.get("current_weather", {})
        forecast_weather = weather_data.get("forecast_weather", [])

        # --- 환율 데이터 가져오기 ---
        exchange_rate_worksheet = spreadsheet.worksheet(EXCHANGE_RATE_WORKSHEET_NAME)
        exchange_rate_data_raw = exchange_rate_worksheet.get_all_values()

        exchange_rate = {}
        if len(exchange_rate_data_raw) >= 2: # 헤더와 최소 한 개의 데이터 행이 있는지 확인
            # 헤더는 첫 번째 행 (인덱스 0)
            exchange_headers = [h.strip() for h in exchange_rate_data_raw[0]]
            # 데이터는 두 번째 행 (인덱스 1)
            exchange_values = exchange_rate_data_raw[1]

            # 각 통화에 대해 데이터 처리
            for i, header in enumerate(exchange_headers):
                if i < len(exchange_values):
                    val = exchange_values[i].strip().replace(',', '')
                    exchange_rate[header] = float(val) if val and val.replace('.', '', 1).isdigit() else None
        
        print(f"DEBUG: Exchange Rate Data: {exchange_rate}")

        # --- 모든 데이터를 단일 JSON 객체로 통합 ---
        final_output_data = {
            "chart_data": processed_chart_data_by_section,
            "table_data": processed_table_data,
            "weather_data": {
                "current": current_weather,
                "forecast": forecast_weather
            },
            "exchange_rate": exchange_rate
        }

        # JSON 파일 저장
        output_dir = os.path.dirname(OUTPUT_JSON_PATH)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"DEBUG: Created directory: {output_dir}")

        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_output_data, f, ensure_ascii=False, indent=4, cls=NpEncoder)
        print(f"데이터가 성공적으로 '{OUTPUT_JSON_PATH}'에 저장되었습니다.")

    except Exception as e:
        print(f"데이터를 가져오거나 처리하는 중 오류 발생: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_process_data()
