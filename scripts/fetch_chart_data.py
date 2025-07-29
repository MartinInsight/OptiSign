import gspread
import json
import os
import pandas as pd
import traceback
import re
from datetime import datetime
import numpy as np

from la_weather_fetcher import fetch_la_weather_data
from exchange_rate_fetcher import fetch_exchange_rate_data

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
        "date_col_idx": 0,
        "data_start_col_idx": 1,
        "data_end_col_idx": 14,
        "data_cols_map": {
            "종합지수(Point)와 그 외 항로별($/FEU)": "KCCI_Date",
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
        "date_col_idx": 15,
        "data_start_col_idx": 17,
        "data_end_col_idx": 30,
        "data_cols_map": {
            "": "SCFI_Date",
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
        "date_col_idx": 32,
        "data_start_col_idx": 33,
        "data_end_col_idx": 41,
        "data_cols_map": {
            "종합지수와 각 항로별($/FEU)": "WCI_Date",
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
        "date_col_idx": 43,
        "data_start_col_idx": 44,
        "data_end_col_idx": 44,
        "data_cols_map": {
            "date": "IACI_Date",
            "종합지수": "IACI_Composite_Index"
        }
    },
    "BLANK_SAILING": {
        "date_col_idx": 46,
        "data_start_col_idx": 47,
        "data_end_col_idx": 52,
        "data_cols_map": {
            "Index": "BLANK_SAILING_Date",
            "Gemini Cooperation": "BLANK_SAILING_Gemini_Cooperation",
            "MSC": "BLANK_SAILING_MSC",
            "OCEAN Alliance": "BLANK_SAILING_OCEAN_Alliance",
            "Premier Alliance": "BLANK_SAILING_Premier_Alliance",
            "Others/Independent": "BLANK_SAILING_Others_Independent",
            "Total": "BLANK_SAILING_Total"
        }
    },
    "FBX": {
        "date_col_idx": 54,
        "data_start_col_idx": 55,
        "data_end_col_idx": 67,
        "data_cols_map": {
            "종합지수와 각 항로별($/FEU)": "FBX_Date",
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
        "date_col_idx": 69,
        "data_start_col_idx": 70,
        "data_end_col_idx": 77,
        "data_cols_map": {
            "각 항로별($/FEU)": "XSI_Date",
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
        "date_col_idx": 79,
        "data_start_col_idx": 80,
        "data_end_col_idx": 80,
        "data_cols_map": {
            "Index(종합지수)": "MBCI_Date",
            "MBCI": "MBCI_Value",
        }
    }
}

ORIGINAL_HEADER_TO_FINAL_KEY_MAP = {}
for section_key, details in SECTION_COLUMN_MAPPINGS.items():
    for original_header, final_key in details["data_cols_map"].items():
        ORIGINAL_HEADER_TO_FINAL_KEY_MAP[original_header] = final_key

TABLE_DATA_CELL_MAPPINGS = {
    "KCCI": {
        "current_date_cell": (0, 1),
        "current_index_cols_range": (1, 14),
        "previous_date_cell": (0, 15),
        "previous_index_cols_range": (1, 15),
        "weekly_change_cols_range": (2, 15),
        "route_names": [
            "종합지수", "미주서안", "미주동안", "유럽", "지중해", "중동",
            "호주", "남미동안", "남미서안", "남아프리카", "서아프리카",
            "중국", "일본", "동남아시아"
        ]
    },
    "SCFI": {
        "current_date_cell": (0, 16),
        "current_index_cols_range": (1, 29),
        "previous_date_cell": (0, 30),
        "previous_index_cols_range": (1, 30),
        "weekly_change_cols_range": (2, 30),
        "route_names": [
            "종합지수", "미주서안", "미주동안", "북유럽", "지중해", "동남아시아",
            "중동", "호주/뉴질랜드", "남아메리카", "일본서안", "일본동안",
            "한국", "동부/서부 아프리카", "남아공"
        ]
    },
    "WCI": {
        "current_date_cell": (0, 31),
        "current_index_cols_range": (1, 39),
        "previous_date_cell": (0, 40),
        "previous_index_cols_range": (1, 40),
        "weekly_change_cols_range": (2, 40),
        "route_names": [
            "종합지수", "상하이 → 로테르담", "로테르담 → 상하이", "상하이 → 제노바",
            "상하이 → 로스엔젤레스", "로스엔젤레스 → 상하이", "상하이 → 뉴욕",
            "뉴욕 → 로테르담", "로테르담 → 뉴욕"
        ]
    },
    "IACI": {
        "current_date_cell": (0, 41),
        "current_index_cols_range": (1, 41),
        "previous_date_cell": (0, 42),
        "previous_index_cols_range": (1, 42),
        "weekly_change_cols_range": (2, 42),
        "route_names": ["종합지수"]
    },
    "BLANK_SAILING": {
        "current_date_cell": (0, 43),
        "current_index_cols_range": (1, 48),
        "previous_date_cells_and_ranges": [
            {"date_cell": (0, 49), "data_range": (1, 54)},
            {"date_cell": (0, 55), "data_range": (1, 60)},
            {"date_cell": (0, 61), "data_range": (1, 66)},
            {"date_cell": (0, 67), "data_range": (1, 72)},
        ],
        "route_names": [
            "Gemini Cooperation", "MSC", "OCEAN Alliance",
            "Premier Alliance", "Others/Independent", "Total"
        ]
    },
    "FBX": {
        "current_date_cell": (0, 73),
        "current_index_cols_range": (1, 85),
        "previous_date_cell": (0, 86),
        "previous_index_cols_range": (1, 86),
        "weekly_change_cols_range": (2, 86),
        "route_names": [
            "글로벌 컨테이너 운임 지수", "중국/동아시아 → 미주서안", "미주서안 → 중국/동아시아",
            "중국/동아시아 → 미주동안", "미주동안 → 중국/동아시아", "중국/동아시아 → 북유럽",
            "북유럽 → 중국/동아시아", "중국/동아시아 → 지중해", "지중해 → 중국/동아시아",
            "미주동안 → 북유럽", "북유럽 → 미주동안", "유럽 → 남미동안", "유럽 → 남미서안"
        ]
    },
    "XSI": {
        "current_date_cell": (0, 87),
        "current_index_cols_range": (1, 94),
        "previous_date_cell": (0, 95),
        "previous_index_cols_range": (1, 95),
        "weekly_change_cols_range": (2, 95),
        "route_names": [
            "동아시아 → 북유럽", "북유럽 → 동아시아", "동아시아 → 미주서안",
            "미주서안 → 동아시아", "동아시아 → 남미동안", "북유럽 → 미주동안",
            "미주동안 → 북유럽", "북유럽 → 남미동안"
        ]
    },
    "MBCI": {
        "current_date_cell": (0, 96),
        "current_index_cols_range": (1, 96),
        "previous_date_cell": (0, 97),
        "previous_index_cols_range": (1, 97),
        "weekly_change_cols_range": (2, 97),
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

        main_header_row_index = 1 
        if len(all_data_charts) <= main_header_row_index:
            print(f"Error: '{WORKSHEET_NAME_CHARTS}' sheet does not have enough rows for header at index {main_header_row_index}.")
            return

        raw_headers_full_charts = [str(h).strip().replace('"', '') for h in all_data_charts[main_header_row_index]]
        print(f"DEBUG: '{WORKSHEET_NAME_CHARTS}'에서 가져온 원본 헤더 (전체 행): {raw_headers_full_charts}")

        data_rows_for_df = all_data_charts[main_header_row_index + 1:]
        df_raw_full = pd.DataFrame(data_rows_for_df, columns=raw_headers_full_charts)
        print(f"DEBUG: Raw full DataFrame shape with original headers: {df_raw_full.shape}")

        processed_chart_data_by_section = {}

        for section_key, details in SECTION_COLUMN_MAPPINGS.items():
            date_col_idx_in_raw = details["date_col_idx"]
            data_start_col_idx_in_raw = details["data_start_col_idx"]
            data_end_col_idx_in_raw = details["data_end_col_idx"]
            
            raw_column_indices_for_section = [date_col_idx_in_raw] + list(range(data_start_col_idx_in_raw, data_end_col_idx_in_raw + 1))
            
            valid_raw_column_indices = [idx for idx in raw_column_indices_for_section if idx < len(raw_headers_full_charts)]

            if not valid_raw_column_indices:
                print(f"WARNING: No valid column indices found for section {section_key}. Skipping chart data processing for this section.")
                processed_chart_data_by_section[section_key] = []
                continue

            df_section_raw_cols = df_raw_full.iloc[:, valid_raw_column_indices].copy()
            
            actual_raw_headers_in_section_df = [raw_headers_full_charts[idx] for idx in valid_raw_column_indices]
            df_section_raw_cols.columns = actual_raw_headers_in_section_df

            print(f"DEBUG: {section_key} - Raw columns in section DataFrame before renaming: {df_section_raw_cols.columns.tolist()}")

            rename_map = {}
            for original_header_from_map, final_json_key in details["data_cols_map"].items():
                if original_header_from_map in actual_raw_headers_in_section_df:
                    rename_map[original_header_from_map] = final_json_key
                else:
                    print(f"WARNING: Header '{original_header_from_map}' from SECTION_COLUMN_MAPPINGS for {section_key} was not found in the extracted raw columns. It will not be renamed.")

            print(f"DEBUG: {section_key} - Constructed rename_map: {rename_map}")

            df_section = df_section_raw_cols.rename(columns=rename_map)
            print(f"DEBUG: {section_key} - Columns in section DataFrame after renaming: {df_section.columns.tolist()}")

            date_original_header = list(details["data_cols_map"].keys())[0]
            date_col_final_name = details["data_cols_map"][date_original_header]
            
            section_data_col_final_names = [
                final_json_key for original_header, final_json_key in details["data_cols_map"].items()
                if original_header != date_original_header
            ]
            
            if date_col_final_name not in df_section.columns:
                print(f"ERROR: Date column '{date_col_final_name}' not found in section {section_key} after renaming. Skipping.")
                processed_chart_data_by_section[section_key] = []
                continue

            df_section[date_col_final_name] = df_section[date_col_final_name].astype(str).str.strip()
            df_section['parsed_date'] = pd.to_datetime(df_section[date_col_final_name], errors='coerce')
            
            unparseable_dates_series = df_section[df_section['parsed_date'].isna()][date_col_final_name]
            num_unparseable_dates = unparseable_dates_series.count()
            if num_unparseable_dates > 0:
                print(f"WARNING: {num_unparseable_dates} dates could not be parsed for {section_key}. Sample unparseable date strings: {unparseable_dates_series.head().tolist()}")

            df_section.dropna(subset=['parsed_date'], inplace=True)
            print(f"DEBUG: DataFrame shape for {section_key} after date parsing and dropna: {df_section.shape}")

            for col_final_name in section_data_col_final_names:
                if col_final_name in df_section.columns:
                    df_section[col_final_name] = pd.to_numeric(df_section[col_final_name].astype(str).str.replace(',', ''), errors='coerce')
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
            table_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
            table_rows_data = []

            if section_key == "BLANK_SAILING" and "previous_date_cells_and_ranges" in table_details:
                current_row_idx = table_details["current_date_cell"][0]
                current_cols_start, current_cols_end = table_details["current_index_cols_range"]
                route_names = table_details["route_names"]
                
                current_data_row = all_data_tables[current_row_idx] if current_row_idx < len(all_data_tables) else []

                blank_sailing_historical_data = []
                
                current_bs_entry = {"date": (all_data_tables[current_row_idx][table_details["current_date_cell"][1]] if current_row_idx < len(all_data_tables) and table_details["current_date_cell"][1] < len(all_data_tables[current_row_idx]) else "")}
                for i in range(len(route_names)):
                    col_idx = current_cols_start + i
                    if col_idx <= current_cols_end and col_idx < len(current_data_row):
                        val = str(current_data_row[col_idx]).strip().replace(',', '')
                        current_bs_entry[route_names[i]] = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                blank_sailing_historical_data.append(current_bs_entry)

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
                
                blank_sailing_historical_data.sort(key=lambda x: pd.to_datetime(x['date'], errors='coerce', dayfirst=False) if x['date'] else pd.Timestamp.min)

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
                    current_row_idx = table_details["current_date_cell"][0]
                    previous_row_idx = table_details["previous_date_cell"][0]
                    weekly_change_row_idx_info = table_details["weekly_change_cols_range"]

                    current_cols_start, current_cols_end = table_details["current_index_cols_range"]
                    previous_cols_start, previous_cols_end = table_details["previous_index_cols_range"]
                    
                    weekly_change_cols_start, weekly_change_cols_end = (None, None)
                    if weekly_change_row_idx_info is not None:
                        weekly_change_cols_start, weekly_change_cols_end = weekly_change_row_idx_info

                    route_names = table_details["route_names"]

                    if current_row_idx >= len(all_data_tables) or \
                       previous_row_idx >= len(all_data_tables) or \
                       (weekly_change_row_idx_info is not None and weekly_change_row_idx_info[0] >= len(all_data_tables)):
                        print(f"경고: '{WORKSHEET_NAME_TABLES}'에 섹션 {section_key}의 테이블 데이터에 충분한 행이 없습니다. 건너뜁니다.")
                        processed_table_data[section_key] = {"headers": table_headers, "rows": []}
                        continue

                    current_data_row = all_data_tables[current_row_idx]
                    previous_data_row = all_data_tables[previous_row_idx]
                    weekly_change_data_row = all_data_tables[weekly_change_row_idx_info[0]] if weekly_change_row_idx_info is not None else None

                    num_data_points = len(route_names)

                    for i in range(num_data_points):
                        route_name = route_names[i]
                        
                        current_index_val = None
                        previous_index_val = None
                        weekly_change = None

                        col_idx_current = current_cols_start + i
                        if col_idx_current <= current_cols_end and col_idx_current < len(current_data_row):
                            val = str(current_data_row[col_idx_current]).strip().replace(',', '')
                            current_index_val = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None

                        col_idx_previous = previous_cols_start + i
                        if col_idx_previous <= previous_cols_end and col_idx_previous < len(previous_data_row):
                            val = str(previous_data_row[col_idx_previous]).strip().replace(',', '')
                            previous_index_val = float(val) if val and val.replace('.', '', 1).replace('-', '', 1).isdigit() else None
                        
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
                        else:
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
            
            processed_table_data[section_key] = {
                "headers": table_headers,
                "rows": table_rows_data
            }
            print(f"디버그: {section_key}의 처리된 테이블 데이터 (처음 3개 항목): {processed_table_data[section_key]['rows'][:3]}")


        weather_data = fetch_la_weather_data(spreadsheet)
        current_weather = weather_data.get("current_weather", {})
        forecast_weather = weather_data.get("forecast_weather", [])

        exchange_rate = fetch_exchange_rate_data(spreadsheet)
        
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
