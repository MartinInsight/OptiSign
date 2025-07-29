import gspread
import json
import os
import pandas as pd
import traceback
import re
from datetime import datetime
import numpy as np
import sys

# 현재 스크립트의 디렉토리를 sys.path에 추가하여 로컬 모듈을 찾을 수 있도록 함
script_dir = os.path.dirname(__file__)
if script_dir not in sys.path:
    sys.path.append(script_dir)

from fetch_la_weather_data import fetch_la_weather_data
from fetch_exchange_data import fetch_exchange_data

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDENTIAL_JSON = os.environ.get("GOOGLE_CREDENTIAL_JSON")

print(f"DEBUG: SPREADSHEET_ID from environment: {SPREADSHEET_ID}")
print(f"DEBUG: GOOGLE_CREDENTIAL_JSON from environment (first 50 chars): {GOOGLE_CREDENTIAL_JSON[:50] if GOOGLE_CREDENTIAL_JSON else 'None'}")

WORKSHEET_NAME_CHARTS = "Crawling_Data"
WORKSHEET_NAME_TABLES = "Crawling_Data2"
OUTPUT_JSON_PATH = "data/crawling_data.json"

SECTION_COLUMN_MAPPINGS = {
    "KCCI": {
        "section_name_cell": (0, 0), # A1 (row 0, col 0)
        "date_col_idx": 0, # A열 (data starts from A3, header from A2)
        "data_start_col_idx": 1, # B열
        "data_end_col_idx": 14, # O열
        "sub_headers_map": { # Headers from row 2 (index 1)
            "종합지수(Point)와 그 외 항로별($/FEU)": "Date", # A2
            "종합지수": "Composite_Index", # B2
            "미주서안": "US_West_Coast", # C2
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
        "section_name_cell": (0, 16), # Q1
        "date_col_idx": 16, # Q열
        "data_start_col_idx": 17, # R열
        "data_end_col_idx": 30, # AE열
        "sub_headers_map": { # Headers from row 2 (index 1)
            "종합지수($/TEU), 미주항로별($/FEU), 그 외 항로별($/TEU)": "Date", # Q2 셀의 실제 헤더로 수정
            "종합지수": "Composite_Index", # R2
            "미주서안": "US_West_Coast",
            "미주동안": "US_East_Coast",
            "북유럽": "North_Europe",
            "지중해": "Mediterranean",
            "동남아시아": "Southeast_Asia",
            "중동": "Middle_East",
            "호주/뉴질랜드": "Australia_New_Zealand",
            "남아메리카": "South_America",
            "일본서안": "Japan_West_Coast",
            "일본동안": "Japan_East_Coast",
            "한국": "Korea",
            "동부/서부 아프리카": "East_West_Africa",
            "남아공": "South_Africa"
        }
    },
    "WCI": {
        "section_name_cell": (0, 32), # AG1
        "date_col_idx": 32, # AG열
        "data_start_col_idx": 33, # AH열
        "data_end_col_idx": 41, # AP열
        "sub_headers_map": { # Headers from row 2 (index 1)
            "종합지수와 각 항로별($/FEU)": "Date", # AG2
            "종합지수": "Composite_Index",
            "상하이 → 로테르담": "Shanghai_Rotterdam",
            "로테르담 → 상하이": "Rotterdam_Shanghai",
            "상하이 → 제노바": "Shanghai_Genoa",
            "상하이 → 로스엔젤레스": "Los_Angeles_Shanghai",
            "로스엔젤레스 → 상하이": "Los_Angeles_Shanghai",
            "상하이 → 뉴욕": "Shanghai_New_York",
            "뉴욕 → 로테르담": "New_York_Rotterdam",
            "로테르담 → 뉴욕": "Rotterdam_New_York",
        }
    },
    "IACI": {
        "section_name_cell": (0, 43), # AR1
        "date_col_idx": 43, # AR열
        "data_start_col_idx": 44, # AS열
        "data_end_col_idx": 44, # AS열
        "sub_headers_map": { # Headers from row 2 (index 1)
            "date": "Date", # AR2
            "종합지수": "Composite_Index" # AS2
        }
    },
    "BLANK_SAILING": {
        "section_name_cell": (0, 46), # AU1
        "date_col_idx": 46, # AU열
        "data_start_col_idx": 47, # AV열
        "data_end_col_idx": 52, # BA열
        "sub_headers_map": { # Headers from row 2 (index 1)
            "Index": "Date", # AU2
            "Gemini Cooperation": "Gemini_Cooperation",
            "MSC": "MSC",
            "OCEAN Alliance": "OCEAN_Alliance",
            "Premier Alliance": "Premier_Alliance",
            "Others/Independent": "Others_Independent",
            "Total": "Total"
        }
    },
    "FBX": {
        "section_name_cell": (0, 54), # BC1
        "date_col_idx": 54, # BC열
        "data_start_col_idx": 55, # BD열
        "data_end_col_idx": 67, # BP열
        "sub_headers_map": { # Headers from row 2 (index 1)
            "종합지수와 각 항로별($/FEU)": "Date", # BC2
            "종합지수": "Composite_Index", # BD2
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
        }
    },
    "XSI": {
        "section_name_cell": (0, 69), # BR1
        "date_col_idx": 69, # BR열
        "data_start_col_idx": 70, # BS열
        "data_end_col_idx": 77, # BZ열
        "sub_headers_map": { # Headers from row 2 (index 1)
            "각 항로별($/FEU)": "Date", # BR2
            "동아시아 → 북유럽": "East_Asia_North_Europe",
            "북유럽 → 동아시아": "North_Europe_East_Asia",
            "동아시아 → 미주서안": "East_Asia_US_West_Coast",
            "미주서안 → 동아시아": "US_West_Coast_East_Asia",
            "동아시아 → 남미동안": "East_Asia_South_America_East_Coast",
            "북유럽 → 미주동안": "North_Europe_US_East_Coast",
            "미주동안 → 북유럽": "US_East_Coast_North_Europe",
            "북유럽 → 남미동안": "North_Europe_South_America_East_Coast"
        }
    },
    "MBCI": {
        "section_name_cell": (0, 79), # CB1
        "date_col_idx": 79, # CB열
        "data_start_col_idx": 80, # CC열
        "data_end_col_idx": 80, # CC열
        "sub_headers_map": { # Headers from row 2 (index 1)
            "Index(종합지수)": "Date", # CB2
            "MBCI": "Value", # CC2
        }
    }
}

TABLE_DATA_CELL_MAPPINGS = {
    "KCCI": {
        "current_date_cell": (2, 0), # A3
        "current_index_cols_range": (1, 14), # B3:O3
        "previous_date_cell": (3, 0), # A4
        "previous_index_cols_range": (1, 14), # B4:O4
        "weekly_change_row_idx": 4, # B5:O5 (행 인덱스만 지정)
        "route_names": [
            "종합지수", "미주서안", "미주동안", "유럽", "지중해", "중동",
            "호주", "남미동안", "남미서안", "남아프리카", "서아프리카",
            "중국", "일본", "동남아시아"
        ]
    },
    "SCFI": {
        "current_date_cell": (8, 0), # A9
        "current_index_cols_range": (1, 14), # B9:O9
        "previous_date_cell": (9, 0), # A10
        "previous_index_cols_range": (1, 14), # B10:O10
        "weekly_change_row_idx": 10, # B11:O11
        "route_names": [
            "종합지수", "미주서안", "미주동안", "북유럽", "지중해", "동남아시아",
            "중동", "호주/뉴질랜드", "남아메리카", "일본서안", "일본동안",
            "한국", "동부/서부 아프리카", "남아공"
        ]
    },
    "WCI": {
        "current_date_cell": (20, 0), # A21
        "current_index_cols_range": (1, 9), # B21:J21
        "previous_date_cell": (21, 0), # A22
        "previous_index_cols_range": (1, 9), # B22:J22
        "weekly_change_row_idx": 22, # B23:J23 (O23 대신 J23으로 수정하여 데이터 열 범위와 일치시킴)
        "route_names": [
            "종합지수", "상하이 → 로테르담", "로테르담 → 상하이", "상하이 → 제노바",
            "상하이 → 로스엔젤레스", "로스엔젤레스 → 상하이", "상하이 → 뉴욕",
            "뉴욕 → 로테르담", "로테르담 → 뉴욕"
        ]
    },
    "IACI": {
        "current_date_cell": (26, 0), # A27
        "current_index_cols_range": (1, 1), # B27
        "previous_date_cell": (27, 0), # A28
        "previous_index_cols_range": (1, 1), # B28
        "weekly_change_row_idx": 28, # B29
        "route_names": ["종합지수"]
    },
    "BLANK_SAILING": {
        "current_date_cell": (32, 0), # A33
        "current_index_cols_range": (1, 6), # B33:G33
        "previous_entries": [ # 여러 이전 데이터 지점 처리
            {"date_cell": (33, 0), "data_range": (1, 6)}, # A34, B34:G34
            {"date_cell": (34, 0), "data_range": (1, 6)}, # A35, B35:G35
            {"date_cell": (35, 0), "data_range": (1, 6)}, # A36, B36:G36
            {"date_cell": (36, 0), "data_range": (1, 6)}, # A37, B37:G37
        ],
        "route_names": [
            "Gemini Cooperation", "MSC", "OCEAN Alliance",
            "Premier Alliance", "Others/Independent", "Total"
        ]
    },
    "FBX": {
        "current_date_cell": (40, 0), # A41
        "current_index_cols_range": (1, 13), # B41:N41
        "previous_date_cell": (41, 0), # A42
        "previous_index_cols_range": (1, 13), # B42:N42
        "weekly_change_row_idx": 42, # B43:N43
        "route_names": [
            "종합지수", # "글로벌 컨테이너 운임 지수" 대신 실제 헤더 "종합지수" 사용
            "중국/동아시아 → 미주서안", "미주서안 → 중국/동아시아",
            "중국/동아시아 → 미주동안", "미주동안 → 중국/동아시아", "중국/동아시아 → 북유럽",
            "북유럽 → 중국/동아시아", "중국/동아시아 → 지중해", "지중해 → 중국/동아시아",
            "미주동안 → 북유럽", "북유럽 → 미주동안", "유럽 → 남미동안", "유럽 → 남미서안"
        ]
    },
    "XSI": {
        "current_date_cell": (46, 0), # A47
        "current_index_cols_range": (1, 8), # B47:I47
        "previous_date_cell": (47, 0), # A48
        "previous_index_cols_range": (1, 8), # B48:I48 (N48 대신 I48로 수정하여 데이터 열 범위와 일치시킴)
        "weekly_change_row_idx": 48, # B49:I49 (N49 대신 I49로 수정하여 데이터 열 범위와 일치시킴)
        "route_names": [
            "동아시아 → 북유럽", "북유럽 → 동아시아", "동아시아 → 미주서안",
            "미주서안 → 동아시아", "동아시아 → 남미동안", "북유럽 → 미주동안",
            "미주동안 → 북유럽", "북유럽 → 남미동안"
        ]
    },
    "MBCI": {
        "current_date_cell": (58, 0), # A59
        "current_index_cols_range": (6, 6), # G59
        "previous_date_cell": (59, 0), # A60
        "previous_index_cols_range": (6, 6), # G60
        "weekly_change_row_idx": 60, # G61
        "route_names": ["MBCI"]
    }
}


def fetch_and_process_data():
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

        worksheet = spreadsheet.worksheet(WORKSHEET_NAME_CHARTS)
        all_data_charts = worksheet.get_all_values()

        print(f"DEBUG: Total rows fetched from Google Sheet (raw): {len(all_data_charts)}")

        if not all_data_charts:
            print("Error: No data fetched from the main chart sheet.")
            return

        # 헤더는 2행(0-인덱스 기준 1)에 있습니다.
        main_header_row_index = 1 
        if len(all_data_charts) <= main_header_row_index:
            print(f"Error: '{WORKSHEET_NAME_CHARTS}' sheet does not have enough rows for header at index {main_header_row_index}.")
            return

        raw_headers_full_charts = [str(h).strip().replace('"', '') for h in all_data_charts[main_header_row_index]]
        print(f"DEBUG: '{WORKSHEET_NAME_CHARTS}'에서 가져온 원본 헤더 (전체 행): {raw_headers_full_charts}")

        # 데이터는 3행(0-인덱스 기준 2)부터 시작합니다.
        data_rows_for_df = all_data_charts[main_header_row_index + 1:]
        df_raw_full = pd.DataFrame(data_rows_for_df, columns=raw_headers_full_charts)
        print(f"DEBUG: Raw full DataFrame shape with original headers: {df_raw_full.shape}")

        processed_chart_data_by_section = {}

        for section_key, details in SECTION_COLUMN_MAPPINGS.items():
            date_col_idx_in_raw = details["date_col_idx"]
            data_start_col_idx_in_raw = details["data_start_col_idx"]
            data_end_col_idx_in_raw = details["data_end_col_idx"]
            sub_headers_map = details["sub_headers_map"] # New: get sub_headers_map

            raw_column_indices_for_section = [date_col_idx_in_raw] + list(range(data_start_col_idx_in_raw, data_end_col_idx_in_raw + 1))
            
            valid_raw_column_indices = [idx for idx in raw_column_indices_for_section if idx < len(raw_headers_full_charts)]

            if not valid_raw_column_indices:
                print(f"WARNING: No valid column indices found for section {section_key}. Skipping chart data processing for this section.")
                processed_chart_data_by_section[section_key] = []
                continue

            # 선택된 원본 열만 포함하는 DataFrame 생성
            df_section_raw_cols = df_raw_full.iloc[:, valid_raw_column_indices].copy()
            
            # 선택된 열의 실제 헤더 이름을 사용하여 DataFrame 열 이름 설정
            actual_raw_headers_in_section_df = [raw_headers_full_charts[idx] for idx in valid_raw_column_indices]
            df_section_raw_cols.columns = actual_raw_headers_in_section_df

            print(f"DEBUG: {section_key} - Raw columns in section DataFrame before renaming: {df_section_raw_cols.columns.tolist()}")

            rename_map = {}
            for original_sub_header, generic_name in sub_headers_map.items():
                if original_sub_header in actual_raw_headers_in_section_df:
                    rename_map[original_sub_header] = f"{section_key}_{generic_name}" # Prepend section_key
                else:
                    print(f"WARNING: Sub-header '{original_sub_header}' from sub_headers_map for {section_key} was not found in the extracted raw columns. It will not be renamed.")

            print(f"DEBUG: {section_key} - Constructed rename_map: {rename_map}")

            df_section = df_section_raw_cols.rename(columns=rename_map)
            print(f"DEBUG: {section_key} - Columns in section DataFrame after renaming: {df_section.columns.tolist()}")

            # 날짜 열의 최종 이름은 이제 "SECTION_KEY_Date" 형식
            date_col_final_name = f"{section_key}_Date"
            
            # 데이터 열의 최종 이름도 "SECTION_KEY_GenericName" 형식
            section_data_col_final_names = [
                f"{section_key}_{generic_name}" for original_sub_header, generic_name in sub_headers_map.items()
                if generic_name != "Date" # Exclude the date column's generic name
            ]
            
            if date_col_final_name not in df_section.columns:
                print(f"ERROR: Date column '{date_col_final_name}' not found in section {section_key} after renaming. Skipping.")
                processed_chart_data_by_section[section_key] = []
                continue

            df_section[date_col_final_name] = df_section[date_col_final_name].astype(str).str.strip()
            # 날짜 파싱 시 여러 형식 시도 (MM/DD/YYYY, YYYY-MM-DD, YYYY.MM.DD)
            df_section['parsed_date'] = pd.to_datetime(df_section[date_col_final_name], errors='coerce', dayfirst=False)
            
            unparseable_dates_series = df_section[df_section['parsed_date'].isna()][date_col_final_name]
            num_unparseable_dates = unparseable_dates_series.count()
            if num_unparseable_dates > 0:
                print(f"WARNING: {num_unparseable_dates} dates could not be parsed for {section_key}. Sample unparseable date strings: {unparseable_dates_series.head().tolist()}")

            df_section.dropna(subset=['parsed_date'], inplace=True)
            print(f"DEBUG: DataFrame shape for {section_key} after date parsing and dropna: {df_section.shape}")

            for col_final_name in section_data_col_final_names:
                if col_final_name in df_section.columns:
                    # pd.to_numeric()를 호출하기 전에 Series에 .str 접근자를 사용하는 것은 올바르지 않습니다.
                    # 이미 .astype(str)로 문자열로 변환되었으므로, .str.replace()를 직접 호출할 수 있습니다.
                    # 하지만, to_numeric은 문자열을 직접 처리할 수 있으므로 .str.replace()를 먼저 적용하는 것이 좋습니다.
                    df_section[col_final_name] = df_section[col_final_name].astype(str).str.replace(',', '', regex=False) # regex=False 추가
                    df_section[col_final_name] = pd.to_numeric(df_section[col_final_name], errors='coerce')
                else:
                    print(f"WARNING: Data column '{col_final_name}' not found in section {section_key} after renaming. It might not be included in the output.")
            
            df_section = df_section.replace({pd.NA: None, float('nan'): None})

            df_section = df_section.sort_values(by='parsed_date', ascending=True)
            df_section['date'] = df_section['parsed_date'].dt.strftime('%Y-%m-%d')
            
            output_cols = ['date'] + section_data_col_final_names
            existing_output_cols = [col for col in output_cols if col in df_section.columns]
            
            processed_chart_data_by_section[section_key] = df_section[existing_output_cols].to_dict(orient='records')
            print(f"DEBUG: {section_key}의 처리된 차트 데이터 (처음 3개 항목): {processed_chart_data_by_section[section_key][:3]}")
            print(f"DEBUG: {section_key}의 처리된 차트 데이터 (마지막 3개 항목): {processed_chart_data_by_section[section_key][-3:]}")


        worksheet_tables = spreadsheet.worksheet(WORKSHEET_NAME_TABLES)
        all_data_tables = worksheet_tables.get_all_values()

        print(f"디버그: '{WORKSHEET_NAME_TABLES}'에서 가져온 총 행 수 (원본): {len(all_data_tables)}")

        if not all_data_tables:
            print(f"오류: '{WORKSHEET_NAME_TABLES}' 시트에서 데이터를 가져오지 못했습니다. 테이블 데이터가 비어 있습니다.")

        processed_table_data = {}
        for section_key, table_details in TABLE_DATA_CELL_MAPPINGS.items():
            print(f"DEBUG: Processing table section: {section_key}") # 추가된 디버그 로그
            table_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
            table_rows_data = []

            # BLANK_SAILING 섹션은 특별 처리
            if section_key == "BLANK_SAILING" and "previous_entries" in table_details:
                blank_sailing_historical_data = []
                
                # 현재 데이터 처리
                current_row_idx = table_details["current_date_cell"][0]
                current_date_col_idx = table_details["current_date_cell"][1]
                current_cols_start, current_cols_end = table_details["current_index_cols_range"]
                route_names = table_details["route_names"]
                
                if current_row_idx < len(all_data_tables):
                    current_data_row = all_data_tables[current_row_idx]
                    current_bs_entry = {"date": (current_data_row[current_date_col_idx] if current_date_col_idx < len(current_data_row) else "")}
                    for i, route_name in enumerate(route_names):
                        col_idx = current_cols_start + i
                        if col_idx <= current_cols_end and col_idx < len(current_data_row):
                            val = str(current_data_row[col_idx]).strip().replace(',', '')
                            current_bs_entry[route_name] = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                    blank_sailing_historical_data.append(current_bs_entry)

                # 이전 데이터 처리
                for prev_entry_details in table_details["previous_entries"]:
                    prev_row_idx = prev_entry_details["date_cell"][0]
                    prev_date_col_idx = prev_entry_details["date_cell"][1]
                    prev_cols_start, prev_cols_end = prev_entry_details["data_range"]
                    
                    if prev_row_idx < len(all_data_tables):
                        prev_data_row = all_data_tables[prev_row_idx]
                        prev_bs_entry = {"date": (prev_data_row[prev_date_col_idx] if prev_date_col_idx < len(prev_data_row) else "")}
                        for i, route_name in enumerate(route_names):
                            col_idx = prev_cols_start + i
                            if col_idx <= prev_cols_end and col_idx < len(prev_data_row):
                                val = str(prev_data_row[col_idx]).strip().replace(',', '')
                                prev_bs_entry[route_name] = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                        blank_sailing_historical_data.append(prev_bs_entry)
                
                # 날짜 파싱 및 정렬 (MM/DD/YYYY 또는 YYYY-MM/DD)
                # BLANK_SAILING 날짜 형식은 '7/18/2025' 이므로 %m/%d/%Y 사용
                blank_sailing_historical_data.sort(key=lambda x: datetime.strptime(x['date'], '%m/%d/%Y') if x['date'] else datetime.min)

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
                            "route": f"{section_key}_{route_name}",
                            "current_index": current_index_val,
                            "previous_index": previous_index_val,
                            "weekly_change": weekly_change
                        })
                else:
                    # 데이터가 충분하지 않을 때의 처리 (기존 로직 유지)
                    print(f"경고: BLANK_SAILING 섹션에 테이블 데이터 생성에 충분한 기록이 없습니다.")
                    for route_name in route_names:
                        table_rows_data.append({
                            "route": f"{section_key}_{route_name}",
                            "current_index": None,
                            "previous_index": None,
                            "weekly_change": None
                        })

            else: # BLANK_SAILING을 제외한 일반 섹션 처리
                current_row_idx = table_details["current_date_cell"][0]
                previous_row_idx = table_details["previous_date_cell"][0]
                weekly_change_row_idx = table_details.get("weekly_change_row_idx") # weekly_change_cols_range 대신 weekly_change_row_idx 사용

                current_date_col_idx = table_details["current_date_cell"][1]
                previous_date_col_idx = table_details["previous_date_cell"][1]

                current_cols_start, current_cols_end = table_details["current_index_cols_range"]
                previous_cols_start, previous_cols_end = table_details["previous_index_cols_range"]
                
                weekly_change_cols_start, weekly_change_cols_end = (None, None)
                if weekly_change_row_idx is not None:
                    # weekly_change_row_idx는 행 인덱스만 포함하므로, 열 범위는 current_index_cols_range와 동일하게 가정
                    weekly_change_cols_start = current_cols_start
                    weekly_change_cols_end = current_cols_end


                route_names = table_details["route_names"]

                if current_row_idx >= len(all_data_tables) or \
                   previous_row_idx >= len(all_data_tables) or \
                   (weekly_change_row_idx is not None and weekly_change_row_idx >= len(all_data_tables)):
                    print(f"경고: '{WORKSHEET_NAME_TABLES}'에 섹션 {section_key}의 테이블 데이터에 충분한 행이 없습니다. 건너_ㅂ니다.")
                    processed_table_data[section_key] = {"headers": table_headers, "rows": []}
                    continue

                current_data_row = all_data_tables[current_row_idx]
                previous_data_row = all_data_tables[previous_row_idx]
                weekly_change_data_row = all_data_tables[weekly_change_row_idx] if weekly_change_row_idx is not None else None

                num_data_points = len(route_names)

                for i in range(num_data_points):
                    route_name = route_names[i]
                    print(f"DEBUG:   Route: {route_name}") # 추가된 디버그 로그
                    
                    current_index_val = None
                    previous_index_val = None
                    weekly_change = None

                    col_idx_current = current_cols_start + i
                    if col_idx_current < len(current_data_row): # col_idx_current <= current_cols_end 조건은 이미 current_cols_end가 num_data_points에 맞춰져 있다고 가정
                        val = str(current_data_row[col_idx_current]).strip().replace(',', '')
                        print(f"DEBUG:     Raw current value: '{val}'") # 추가된 디버그 로그
                        current_index_val = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                    else:
                        print(f"DEBUG:     Raw current value: N/A (Column index {col_idx_current} out of bounds for current_data_row length {len(current_data_row)})")

                    col_idx_previous = previous_cols_start + i
                    if col_idx_previous < len(previous_data_row): # col_idx_previous <= previous_cols_end 조건은 이미 previous_cols_end가 num_data_points에 맞춰져 있다고 가정
                        val = str(previous_data_row[col_idx_previous]).strip().replace(',', '')
                        print(f"DEBUG:     Raw previous value: '{val}'") # 추가된 디버그 로그
                        previous_index_val = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                    else:
                        print(f"DEBUG:     Raw previous value: N/A (Column index {col_idx_previous} out of bounds for previous_data_row length {len(previous_data_row)})")
                    
                    if weekly_change_data_row is not None:
                        col_idx_weekly_change = weekly_change_cols_start + i
                        if col_idx_weekly_change < len(weekly_change_data_row): # col_idx_weekly_change <= weekly_change_cols_end 조건은 이미 weekly_change_cols_end가 num_data_points에 맞춰져 있다고 가정
                            val = str(weekly_change_data_row[col_idx_weekly_change]).strip().replace(',', '')
                            print(f"DEBUG:     Raw weekly change value: '{val}'") # 추가된 디버그 로그
                            
                            # Weekly Change 값을 파싱하는 로직 개선
                            change_value = None
                            change_percentage_str = None
                            color_class = "text-gray-700"

                            # (값 (퍼센트%)) 형식 파싱
                            match = re.match(r'([+\-]?\d+(\.\d+)?)\s*\(([-+]?\d+(\.\d+)?%)\)', val)
                            if match:
                                change_value = float(match.group(1))
                                change_percentage_str = match.group(3)
                            else:
                                # 값만 있거나 퍼센트만 있는 경우
                                try:
                                    if val.endswith('%'):
                                        change_percentage_str = val
                                        # change_value_only = float(val[:-1]) # % 제거 후 숫자 변환 (이 값은 사용되지 않으므로 제거)
                                        if current_index_val is not None and previous_index_val is not None and previous_index_val != 0:
                                            change_value = current_index_val - previous_index_val
                                    else:
                                        change_value = float(val)
                                        if current_index_val is not None and previous_index_val is not None and previous_index_val != 0:
                                            calculated_percentage = ((current_index_val - previous_index_val) / previous_index_val) * 100
                                            change_percentage_str = f"{calculated_percentage:.2f}%"
                                except ValueError:
                                    pass # 파싱 실패, None 유지

                            if change_value is not None:
                                if change_value > 0:
                                    color_class = "text-red-500"
                                elif change_value < 0:
                                    color_class = "text-blue-500"
                                weekly_change = {
                                    "value": f"{change_value:.2f}",
                                    "percentage": change_percentage_str if change_percentage_str else "N/A",
                                    "color_class": color_class
                                }
                            elif change_percentage_str is not None: # 값이 없어도 퍼센트만 있을 경우
                                weekly_change = {
                                    "value": "N/A",
                                    "percentage": change_percentage_str,
                                    "color_class": color_class
                                }
                            else:
                                weekly_change = None # 파싱된 유효한 데이터가 없는 경우
                        else:
                            print(f"DEBUG:     Raw weekly change value: N/A (Column index {col_idx_weekly_change} out of bounds for weekly_change_data_row length {len(weekly_change_data_row)})")
                    else:
                        weekly_change = None # weekly_change_data_row가 없거나 열 인덱스 범위 밖인 경우

                    # weekly_change_data_row가 None인 경우 (즉, weekly_change_row_idx가 설정되지 않은 경우)
                    # current_index_val과 previous_index_val을 기반으로 계산
                    if weekly_change is None and current_index_val is not None and previous_index_val is not None and previous_index_val != 0:
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
                    
                    print(f"DEBUG:     Parsed current: {current_index_val}, Previous: {previous_index_val}, Weekly Change: {weekly_change}") # 추가된 디버그 로그
                    table_rows_data.append({
                        "route": f"{section_key}_{route_name}",
                        "current_index": current_index_val,
                        "previous_index": previous_index_val,
                        "weekly_change": weekly_change
                    })
            
            processed_table_data[section_key] = {
                "headers": table_headers,
                "rows": table_rows_data
            }
            print(f"디버그: {section_key}의 처리된 테이블 데이터 (처음 3개 항목): {processed_table_data[section_key]['rows'][:3]}")


        weather_data = fetch_la_weather_data(spreadsheet)
        current_weather = weather_data.get("current_weather", {})
        forecast_weather = weather_data.get("forecast_weather", [])

        exchange_rate = fetch_exchange_data(spreadsheet)
        
        final_output_data = {
            "chart_data": processed_chart_data_by_section,
            "table_data": processed_table_data,
            "weather_data": {
                "current": current_weather,
                "forecast": forecast_weather
            },
            "exchange_rate": exchange_rate
        }

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
