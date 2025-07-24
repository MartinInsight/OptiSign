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
            "날짜": "date", # 각 섹션의 날짜 컬럼 명시
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
            "날짜": "date", # 각 섹션의 날짜 컬럼 명시
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
            "날짜": "date", # 각 섹션의 날짜 컬럼 명시
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
            "날짜": "date", # 각 섹션의 날짜 컬럼 명시
            "종합지수": "Composite_Index_3"
        }
    },
    "BLANK_SAILING": {
        "date_col_idx": 46, # AU열은 날짜
        "data_start_col_idx": 47, # AV열은 Index
        "data_end_col_idx": 53, # BB열은 Total (수정됨)
        "data_cols_map": {
            "날짜": "date", # 각 섹션의 날짜 컬럼 명시
            "Index": "Index_Blank_Sailing", # 'Index' is the actual header here
            "Gemini Cooperation": "Gemini_Cooperation_Blank_Sailing",
            "MSC": "MSC_Alliance_Blank_Sailing",
            "OCEAN Alliance": "OCEAN_Alliance_Blank_Sailing",
            "Premier Alliance": "Premier_Alliance_Blank_Sailing",
            "Others/Independent": "Others_Independent_Blank_Sailing",
            "Total": "Total_Blank_Sailings" # 'Total' 컬럼 다시 추가
        }
    },
    "FBX": {
        "date_col_idx": 54, # BC열은 날짜
        "data_start_col_idx": 55, # BD열은 종합지수
        "data_end_col_idx": 67, # BP열은 유럽 → 남미서안
        "data_cols_map": {
            "날짜": "date", # 각 섹션의 날짜 컬럼 명시
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
            "날짜": "date", # 각 섹션의 날짜 컬럼 명시
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
            "날짜": "date", # 각 섹션의 날짜 컬럼 명시
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
        
        # --- Crawling_Data 시트에서 메인 차트 데이터 가져오기 ---
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet_charts = spreadsheet.worksheet(WORKSHEET_NAME_CHARTS)
        all_data_charts = worksheet_charts.get_all_values()

        print(f"디버그: '{WORKSHEET_NAME_CHARTS}'에서 가져온 총 행 수 (원본): {len(all_data_charts)}")

        if not all_data_charts:
            print(f"오류: '{WORKSHEET_NAME_CHARTS}' 시트에서 데이터를 가져오지 못했습니다.")
            return

        # 사용자님의 최신 설명에 따라 차트 데이터의 메인 헤더 행 찾기:
        # "crawling_data시트는 공통적으로 2행에 헤더가 있음. 날짜 위의 헤더는 지수값의 설명으로 되어있음... 나머지 2행의 헤더는 각 항로명으로 되어있음."
        # 이는 2행(인덱스 1)에 그룹 헤더가 있고, 3행(인덱스 2)에 '날짜', '종합지수'와 같은 실제 데이터 헤더가 있음을 의미합니다.
        main_header_row_index_charts = 1 # 2행 (0-indexed: 1)
        
        # 식별된 헤더 행에서 차트의 원본 헤더 가져오기
        raw_headers_full_charts = [str(h).strip() for h in all_data_charts[main_header_row_index_charts]]
        print(f"디버그: '{WORKSHEET_NAME_CHARTS}'에서 가져온 원본 헤더 (전체 행): {raw_headers_full_charts}")

        processed_chart_data_by_section = {}
        processed_table_data_by_section = {}

        # 각 섹션을 반복하며 차트용 특정 데이터 추출
        for section_key, details in SECTION_COLUMN_MAPPINGS.items():
            date_col_idx = details["date_col_idx"]
            data_start_col_idx = details["data_start_col_idx"]
            data_end_col_idx = details["data_end_col_idx"]
            data_cols_map = details["data_cols_map"]
            
            section_chart_data = []
            
            # DataFrame을 위한 컬럼 이름 준비
            # 'date'는 data_cols_map에 포함되어 있으므로 별도로 추가할 필요 없음
            expected_df_columns = list(data_cols_map.values())

            # 헤더 다음 행부터 데이터 행 추출 시작
            for row_idx in range(main_header_row_index_charts + 1, len(all_data_charts)): # 3행(인덱스 2)부터 시작
                row_data = all_data_charts[row_idx]
                
                current_record = {}
                
                # 이 섹션의 날짜 추출
                if date_col_idx < len(row_data):
                    date_str = str(row_data[date_col_idx]).strip()
                    if date_str: # 날짜 문자열이 비어 있지 않은 경우에만 추가
                        current_record["date"] = date_str
                    else:
                        # 날짜 컬럼이 비어 있으면, 이 행은 이 섹션의 차트 데이터에서 건너뜀
                        continue
                else:
                    continue # 행에 날짜 컬럼 인덱스가 범위를 벗어나면 건너뜀

                # 이 섹션의 숫자 데이터 컬럼 추출
                # data_cols_map의 예상 헤더를 반복 (날짜 제외)
                # 이제 raw_headers_full_charts.index()를 사용하지 않고,
                # data_start_col_idx를 기준으로 상대적 위치를 사용합니다.
                
                # data_cols_map에서 '날짜' 키를 제외한 데이터 컬럼의 순서
                data_cols_map_keys_for_data = [k for k in data_cols_map.keys() if k != "날짜"]

                for i, raw_header_name in enumerate(data_cols_map_keys_for_data):
                    final_json_key = data_cols_map[raw_header_name]
                    
                    # 실제 시트 컬럼 인덱스는 섹션의 시작 인덱스 + data_cols_map 내의 상대적 위치
                    col_idx_in_sheet = data_start_col_idx + i

                    # 컬럼 인덱스가 정의된 섹션 범위 내에 있는지 확인
                    if col_idx_in_sheet <= data_end_col_idx:
                        if col_idx_in_sheet < len(row_data):
                            val = str(row_data[col_idx_in_sheet]).strip().replace(',', '')
                            current_record[final_json_key] = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                        else:
                            current_record[final_json_key] = None
                            print(f"경고: '{section_key}' 섹션의 '{raw_header_name}' 데이터 컬럼 (인덱스 {col_idx_in_sheet})이 행 데이터 범위를 벗어났습니다. None으로 설정합니다.")
                    else:
                        # 이 경우는 data_cols_map의 정의가 data_end_col_idx를 초과할 때 발생할 수 있습니다.
                        # 사용자 정의가 정확하다고 가정하면 이 경고는 발생하지 않아야 합니다.
                        print(f"경고: '{section_key}' 차트의 헤더 '{raw_header_name}'에 대한 계산된 시트 인덱스 {col_idx_in_sheet}가 정의된 데이터 범위({data_start_col_idx}-{data_end_col_idx}) 밖에 있습니다. 건너뜁니다.")
            
                section_chart_data.append(current_record)
            
            # DataFrame으로 변환하여 더 쉬운 처리 (예: 정렬, NaN 처리)
            # 초기 생성 후 일부 행에 없는 경우에도 예상되는 모든 컬럼이 있는지 확인
            df_section = pd.DataFrame(section_chart_data)
            
            if 'date' in df_section.columns:
                df_section['parsed_date'] = pd.to_datetime(df_section['date'], errors='coerce')
                df_section = df_section[df_section['parsed_date'].notna()] # 유효하지 않은 날짜가 있는 행 필터링
                df_section = df_section.sort_values(by='parsed_date', ascending=True)
                df_section['date'] = df_section['parsed_date'].dt.strftime('%Y-%m-%d') # 날짜 형식 표준화
                df_section = df_section.drop(columns=['parsed_date'])
            
            # 초기 생성 후 DataFrame에 없는 컬럼이 있으면 None으로 채우기
            for col_name in expected_df_columns:
                if col_name not in df_section.columns:
                    df_section[col_name] = None

            # 숫자 컬럼을 적절한 유형으로 변환하고 NaN/NaT 처리
            for col in list(data_cols_map.values()):
                if col != 'date' and col in df_section.columns: # 'date' 컬럼은 숫자로 변환하지 않음
                    df_section[col] = pd.to_numeric(df_section[col], errors='coerce')
                    df_section[col] = df_section[col].replace({np.nan: None}) # numpy NaN을 Python None으로 대체

            processed_chart_data_by_section[section_key] = df_section.to_dict(orient='records')
            print(f"디버그: {section_key}의 처리된 차트 데이터 (처음 3개 항목): {processed_chart_data_by_section[section_key][:3]}")


        # --- Crawling_Data2 시트에서 테이블 데이터 가져오기 ---
        worksheet_tables = spreadsheet.worksheet(WORKSHEET_NAME_TABLES)
        all_data_tables = worksheet_tables.get_all_values()

        print(f"디버그: '{WORKSHEET_NAME_TABLES}'에서 가져온 총 행 수 (원본): {len(all_data_tables)}")

        if not all_data_tables:
            print(f"오류: '{WORKSHEET_NAME_TABLES}' 시트에서 데이터를 가져오지 못했습니다. 테이블 데이터가 비어 있습니다.")

        # 각 섹션의 테이블 데이터를 직접 셀 참조를 사용하여 준비
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
                            "route": route_name,
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
                    processed_table_data_by_section[section_key] = {"headers": table_headers, "rows": []}
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
                        "route": route_name, # 테이블 항로에 원본 헤더 이름(한국어) 사용
                        "current_index": current_index_val,
                        "previous_index": previous_index_val,
                        "weekly_change": weekly_change
                    })
            
            processed_table_data_by_section[section_key] = {
                "headers": table_headers,
                "rows": table_rows_data
            }
            print(f"디버그: {section_key}의 처리된 테이블 데이터 (처음 3개 항목): {processed_table_data_by_section[section_key]['rows'][:3]}")


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
        
        print(f"디버그: 현재 날씨 데이터: {current_weather}")
        print(f"디버그: 예보 날씨 데이터 (처음 3개): {forecast_weather[:3]}")

        # --- 환율 데이터 가져오기 ---
        exchange_rate_worksheet = spreadsheet.worksheet(EXCHANGE_RATE_WORKSHEET_NAME)
        exchange_rate_data_raw = exchange_rate_worksheet.get_all_values()

        exchange_rates = []
        # D2:E24에 날짜와 환율이 포함되어 있다고 가정, 따라서 1행(D2)부터 시작하여 2개 컬럼 가져오기
        # 컬럼 D(인덱스 3)와 E(인덱스 4)
        if len(exchange_rate_data_raw) > 1: # 헤더와 데이터가 있는지 확인
            for row_idx in range(1, len(exchange_rate_data_raw)):
                row = exchange_rate_data_raw[row_idx]
                if len(row) > 4 and row[3] and row[4]: # D와 E 컬럼이 존재하고 비어 있지 않은지 확인
                    try:
                        date_str = row[3].strip()
                        rate_val = float(row[4].strip().replace(',', '')) # 콤마 제거 및 float으로 변환
                        exchange_rates.append({'date': date_str, 'rate': rate_val})
                    except ValueError:
                        print(f"경고: {row_idx+1}행의 환율 데이터를 파싱할 수 없습니다: {row}")
                        continue
        
        # 차트용으로 환율을 날짜별로 정렬
        exchange_rates.sort(key=lambda x: pd.to_datetime(x['date'], errors='coerce'))

        print(f"디버그: 환율 데이터 (처음 3개): {exchange_rates[:3]}")

        # --- 모든 데이터를 JSON 출력용 단일 딕셔너리로 결합 ---
        final_output_data = {
            "chart_data": processed_chart_data_by_section,
            "table_data": processed_table_data_by_section, # 테이블 데이터 포함
            "weather_data": {
                "current": current_weather,
                "forecast": forecast_weather
            },
            "exchange_rates": exchange_rates
        }

        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_output_data, f, ensure_ascii=False, indent=4, cls=NpEncoder) # 여기에 NpEncoder 사용

        print(f"모든 데이터가 '{OUTPUT_JSON_PATH}'에 성공적으로 저장되었습니다.")
        # 각 섹션 데이터 샘플 출력
        for section_key, data_list in processed_chart_data_by_section.items():
            print(f"저장된 {section_key} 차트 데이터 샘플 (처음 3개 항목): {data_list[:3]}")

    except Exception as e:
        print(f"데이터 처리 중 오류 발생: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_process_data()
