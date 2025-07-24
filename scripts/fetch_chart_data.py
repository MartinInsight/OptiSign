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
SECTION_COLUMN_MAPPINGS = {
    "KCCI": {
        "date_col_idx": 0, # A열은 날짜
        "data_start_col_idx": 1, # B열은 종합지수
        "data_end_col_idx": 14, # O열은 동남아시아
        "data_cols_map": {
            "종합지수(Point)와 그 외 항로별($/FEU)": "KCCI_Date", # Corrected date header to exact sheet header
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
            "": "SCFI_Date", # Corrected date header to empty string as per console log
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
            "종합지수와 각 항로별($/FEU)": "WCI_Date", # Corrected date header to exact sheet header
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
            "date": "IACI_Date", # Corrected date header to exact sheet header
            "종합지수": "Composite_Index_3"
        }
    },
    "BLANK_SAILING": {
        "date_col_idx": 46, # AU열은 날짜
        "data_start_col_idx": 47, # AV열은 Index
        "data_end_col_idx": 52, # BA열은 Total (수정됨, BB가 아닌 BA로)
        "data_cols_map": {
            "Index": "Blank_Sailing_Date", # Corrected date header to exact sheet header
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
            "종합지수와 각 항로별($/FEU)": "FBX_Date", # Corrected date header to exact sheet header
            "글로벌 컨테이너 운임 지수": "Composite_Index_4",
            "중국/동아시아 → 미주서안": "China_EA_US_West_Coast_FBX",
            "미주서안 → 중국/동아시아": "US_West_Coast_China_EA_FBX",
            "중국/동아시아 → 미주동안": "China_EA_US_East_Coast_FBX",
            "미주동안 → 중국/동아시아": "US_East_Coast_China_EA_FBX",
            "중국/동아시아 → 북유럽": "China_EA_North_Europe_FBX",
            "북유럽 → 중국/동아시아": "North_Europe_China_EA_FBX",
            "중국/동아시아 → 지중해": "China_EA_Mediterranean_FBX",
            "지중해 → 중국/동아시아": "Mediterranean_China_EA_FBX",
            "미주동안 → 북유럽": "US_East_Coast_North_Europe_FBX", # Reverted _1 suffix as per console log
            "북유럽 → 미주동안": "North_Europe_US_East_Coast_FBX", # Reverted _1 suffix as per console log
            "유럽 → 남미동안": "Europe_South_America_East_Coast_FBX", # Reverted _1 suffix as per console log
            "유럽 → 남미서안": "Europe_South_America_West_Coast_FBX", # Reverted _1 suffix as per console log
        }
    },
    "XSI": {
        "date_col_idx": 69, # BR열은 날짜
        "data_start_col_idx": 70, # BS열은 동아시아 → 북유럽
        "data_end_col_idx": 77, # BZ열은 북유럽 → 남미동안
        "data_cols_map": {
            "각 항로별($/FEU)": "XSI_Date", # Corrected date header to exact sheet header
            "동아시아 → 북유럽": "XSI_East_Asia_North_Europe",
            "북유럽 → 동아시아": "XSI_North_Europe_East_Asia",
            "동아시아 → 미주서안": "XSI_East_Asia_US_West_Coast",
            "미주서안 → 동아시아": "XSI_US_West_Coast_East_Asia",
            "동아시아 → 남미동안": "XSI_East_Asia_South_America_East_Coast",
            "북유럽 → 미주동안": "XSI_North_Europe_US_East_Coast", # Reverted _1 suffix as per console log
            "미주동안 → 북유럽": "XSI_US_East_Coast_North_Europe", # Reverted _1 suffix as per console log
            "북유럽 → 남미동안": "XSI_North_Europe_South_America_East_Coast"
        }
    },
    "MBCI": {
        "date_col_idx": 79, # CB열은 날짜
        "data_start_col_idx": 80, # CC열은 MBCI
        "data_end_col_idx": 80, # Corrected: Only 'MBCI' is a data column, H column is empty
        "data_cols_map": {
            "Index(종합지수), $/day(정기용선, Time charter)": "MBCI_Date", # Corrected date header to exact sheet header
            "MBCI": "MBCI_MBCI_Value",
            # Removed "$/day(정기용선, Time charter)" from data_cols_map as it's part of the date header at col 79
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
        "route_names": ["종합지수", "유럽 (기본항)", "지중해 (기본항)", "미주서안 (기본항)", "미주동안 (기본항)", "페르시아만/홍해 (두바이)", "호주/뉴질랜드 (멜버른)", "동/서 아프리카 (라고스)", "남아프리카 (더반)", "서일본 (기본항)", "동일본 (기본항)", "동남아시아 (싱가포르)", "한국 (부산)", "중남미서안 (만사니요)"]
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
        "route_names": ["글로벌 컨테이너 운임 지수", "중국/동아시아 → 미주서안", "미주서안 → 중국/동아시아", "중국/동아시아 → 미주동안", "미주동안 → 중국/동아시아", "중국/동아시아 → 북유럽", "북유럽 → 중국/동아시아", "중국/동아시아 → 지중해", "지중해 → 중국/동아시아", "미주동안 → 북유럽", "북유럽 → 미주동안", "유럽 → 남미동안", "유럽 → 남미서안"]
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
        "current_index_cols_range": (6, 7), # G59:H59 (MBCI and $/day)
        "previous_date_cell": (59,0), # A60
        "previous_index_cols_range": (6, 7), # G60:H60
        "weekly_change_cols_range": (6, 7), # G61:H61
        "route_names": ["Index(종합지수)", "$/day(정기용선, Time charter)"] # Corrected to a list of two strings
    }
}

# Global dictionary to map original sheet headers to their final JSON keys
# This will be populated once from SECTION_COLUMN_MAPPINGS
ORIGINAL_HEADER_TO_FINAL_KEY_MAP = {}
for section_key, details in SECTION_COLUMN_MAPPINGS.items():
    for original_header, final_key in details["data_cols_map"].items():
        # Ensure '날짜' is mapped to its specific date key (e.g., KCCI_Date)
        if original_header == "날짜": # This is a placeholder, actual headers might vary
            # This part is now less critical because we are mapping exact headers from the sheet
            # However, if '날짜' is literally a header in some sheet, this mapping is fine.
            ORIGINAL_HEADER_TO_FINAL_KEY_MAP[original_header] = final_key # Store the section-specific date key
        else:
            # For other data columns, store the final_key directly
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

        # Create a mapping from raw_header_original index to final desired name
        col_idx_to_final_header_name = {}
        
        # Populate col_idx_to_final_header_name using the global ORIGINAL_HEADER_TO_FINAL_KEY_MAP
        # This ensures that the column names in the DataFrame are the final desired JSON keys.
        for i, original_header_name_in_sheet in enumerate(raw_headers_full_charts):
            final_key = ORIGINAL_HEADER_TO_FINAL_KEY_MAP.get(original_header_name_in_sheet)
            if final_key:
                col_idx_to_final_header_name[i] = final_key
            else:
                # If an original header is not explicitly mapped, it's an unmapped column.
                col_idx_to_final_header_name[i] = f"UNMAPPED_COL_{i}"
                print(f"WARNING: Original header '{original_header_name_in_sheet}' (col {i}) not found in any data_cols_map. Using default name.")

        # Now, create a list of final column names in their original order
        final_column_names_ordered = [col_idx_to_final_header_name[i] for i in range(len(raw_headers_full_charts))]
        
        print(f"DEBUG: Final mapped column names for full DataFrame: {final_column_names_ordered}")

        # Create a DataFrame from all raw data, using the newly ordered final column names
        data_rows_for_df = all_data_charts[main_header_row_index + 1:]
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

        # Drop columns that were identified as unmapped placeholders
        cols_to_drop = [col for col in df_raw_full.columns if col.startswith('UNMAPPED_COL_')]
        if cols_to_drop:
            print(f"DEBUG: Dropping unmapped columns: {cols_to_drop}")
            df_raw_full.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        print(f"DEBUG: Raw full DataFrame shape after dropping unmapped columns: {df_raw_full.shape}")


        processed_chart_data_by_section = {}

        # Process each section for chart data
        for section_key, details in SECTION_COLUMN_MAPPINGS.items():
            date_col_name_in_df = details["data_cols_map"][list(details["data_cols_map"].keys())[0]] # Get the first key, which is assumed to be the date header
            
            # Get all final JSON keys for data columns in this section (excluding the date column)
            section_data_col_names_in_df = [
                final_json_key for original_header, final_json_key in details["data_cols_map"].items()
                if original_header != list(details["data_cols_map"].keys())[0] # Exclude the first key (date header)
            ]
            
            # Columns to select from the full DataFrame for this specific section
            cols_to_select = [date_col_name_in_df] + section_data_col_names_in_df
            
            # Ensure all selected columns actually exist in df_raw_full
            existing_cols_to_select = [col for col in cols_to_select if col in df_raw_full.columns]
            
            if not existing_cols_to_select:
                print(f"WARNING: No relevant columns found for section {section_key}. Skipping chart data processing for this section.")
                processed_chart_data_by_section[section_key] = []
                continue

            print(f"DEBUG: {section_key} - Columns selected for section DataFrame: {existing_cols_to_select}")
            df_section = df_raw_full[existing_cols_to_select].copy()
            print(f"DEBUG: {section_key} - Head of section DataFrame before date parsing:\n{df_section.head()}")


            # Clean and parse dates for THIS section
            df_section[date_col_name_in_df] = df_section[date_col_name_in_df].astype(str).str.strip()
            
            df_section['parsed_date'] = pd.to_datetime(df_section[date_col_name_in_df], errors='coerce')
            
            unparseable_dates_series = df_section[df_section['parsed_date'].isna()][date_col_name_in_df]
            num_unparseable_dates = unparseable_dates_series.count()
            if num_unparseable_dates > 0:
                print(f"WARNING: {num_unparseable_dates} dates could not be parsed for {section_key} and will be dropped. Sample unparseable date strings: {unparseable_dates_series.head().tolist()}")

            df_section.dropna(subset=['parsed_date'], inplace=True)
            print(f"DEBUG: DataFrame shape for {section_key} after date parsing and dropna: {df_section.shape}")

            # Convert numeric columns for this section
            for col in section_data_col_names_in_df:
                df_section[col] = pd.to_numeric(df_section[col].astype(str).str.replace(',', ''), errors='coerce')
            
            df_section = df_section.replace({pd.NA: None, float('nan'): None})

            # Sort and format date
            df_section = df_section.sort_values(by='parsed_date', ascending=True)
            df_section['date'] = df_section['parsed_date'].dt.strftime('%Y-%m-%d')
            
            # Select final columns for output (rename the specific date column back to 'date')
            output_cols = ['date'] + section_data_col_names_in_df
            processed_chart_data_by_section[section_key] = df_section[output_cols].to_dict(orient='records')
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
    # Previous Index data: B4:O4 (row 3, cols 1-14)
    
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
    # Previous Index data: B42:N42 (row 41, cols 1-13)

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
        "Index(종합지수)": {"current_col": 6, "previous_col": 6}, # G59, G60
        "$/day(정기용선, Time charter)": {"current_col": 7, "previous_col": 7} # H59, H60
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
