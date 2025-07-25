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
OUTPUT_JSON_PATH = "data/crawling_data.json"

# --- Header Mapping Definitions for Chart Data (Historical Series from Crawling_Data) ---
# 이 매핑은 Google Sheet의 원본 헤더를 최종 JSON 키로 변환하는 방법을 정의합니다.
# 'data_cols_map'의 키는 Google Sheet의 2행에 있는 원본 헤더와 정확히 일치해야 합니다.
# 'data_cols_map'의 값은 원하는 최종 JSON 키이며, 이제 섹션 이름과 원본 한글 헤더를 조합하여 사용합니다.
SECTION_COLUMN_MAPPINGS = {
    "KCCI": {
        "date_col_idx": 0, # A열은 날짜
        "data_start_col_idx": 1, # B열은 종합지수
        "data_end_col_idx": 14, # O열은 동남아시아
        "data_cols_map": {
            "종합지수(Point)와 그 외 항로별($/FEU)": "KCCI_날짜", # 날짜 헤더
            "종합지수": "KCCI_종합지수",
            "미주서안": "KCCI_미주서안",
            "미주동안": "KCCI_미주동안",
            "유럽": "KCCI_유럽",
            "지중해": "KCCI_지중해",
            "중동": "KCCI_중동",
            "호주": "KCCI_호주",
            "남미동안": "KCCI_남미동안",
            "남미서안": "KCCI_남미서안",
            "남아프리카": "KCCI_남아프리카",
            "서아프리카": "KCCI_서아프리카",
            "중국": "KCCI_중국",
            "일본": "KCCI_일본",
            "동남아시아": "KCCI_동남아시아"
        }
    },
    "SCFI": {
        "date_col_idx": 15, # Q열은 날짜
        "data_start_col_idx": 17, # R열은 종합지수
        "data_end_col_idx": 30, # AE열은 남아공
        "data_cols_map": {
            "": "SCFI_날짜", # 원본 헤더가 비어 있으므로 '날짜'로 매핑
            "종합지수": "SCFI_종합지수",
            "미주서안": "SCFI_미주서안",
            "미주동안": "SCFI_미주동안",
            "북유럽": "SCFI_북유럽",
            "지중해": "SCFI_지중해",
            "동남아시아": "SCFI_동남아시아",
            "중동": "SCFI_중동",
            "호주/뉴질랜드": "SCFI_호주/뉴질랜드",
            "남아메리카": "SCFI_남아메리카",
            "일본서안": "SCFI_일본서안",
            "일본동안": "SCFI_일본동안",
            "한국": "SCFI_한국",
            "동부/서부 아프리카": "SCFI_동부/서부 아프리카",
            "남아공": "SCFI_남아공"
        }
    },
    "WCI": {
        "date_col_idx": 32, # AG열은 날짜
        "data_start_col_idx": 33, # AH열은 종합지수
        "data_end_col_idx": 41, # AP열은 로테르담 → 뉴욕
        "data_cols_map": {
            "종합지수와 각 항로별($/FEU)": "WCI_날짜", # 날짜 헤더
            "종합지수": "WCI_종합지수",
            "상하이 → 로테르담": "WCI_상하이 → 로테르담",
            "로테르담 → 상하이": "WCI_로테르담 → 상하이",
            "상하이 → 제노바": "WCI_상하이 → 제노바",
            "상하이 → 로스엔젤레스": "WCI_상하이 → 로스엔젤레스",
            "로스엔젤레스 → 상하이": "WCI_로스엔젤레스 → 상하이",
            "상하이 → 뉴욕": "WCI_상하이 → 뉴욕",
            "뉴욕 → 로테르담": "WCI_뉴욕 → 로테르담",
            "로테르담 → 뉴욕": "WCI_로테르담 → 뉴욕",
        }
    },
    "IACI": {
        "date_col_idx": 43, # AR열은 날짜
        "data_start_col_idx": 44, # AS열은 종합지수
        "data_end_col_idx": 44, # AS열
        "data_cols_map": {
            "date": "IACI_날짜", # 원본 헤더가 'date'이므로 '날짜'로 매핑
            "종합지수": "IACI_종합지수"
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
            "Total": "BLANK_SAILING_종합지수"
        }
    },
    "FBX": {
        "date_col_idx": 54, # BC열은 날짜
        "data_start_col_idx": 55, # BD열은 종합지수
        "data_end_col_idx": 67, # BP열은 유럽 → 남미서안
        "data_cols_map": {
            "종합지수와 각 항로별($/FEU)": "FBX_날짜", # Date header
            # These keys MUST match the actual headers in the Google Sheet's row 2.
            # Values are the desired JSON keys, prefixed with "FBX_".
            "종합지수": "FBX_종합지수",
            "중국/동아시아 → 미주서안": "FBX_중국/동아시아 → 미주서안",
            "미주서안 → 중국/동아시아": "FBX_미주서안 → 중국/동아시아",
            "중국/동아시아 → 미주동안": "FBX_중국/동아시아 → 미주동안",
            "미주동안 → 중국/동아시아": "FBX_미주동안 → 중국/동아시아",
            "중국/동아시아 → 북유럽": "FBX_중국/동아시아 → 북유럽",
            "북유럽 → 중국/동아시아": "FBX_북유럽 → 중국/동아시아",
            "중국/동아시아 → 지중해": "FBX_중국/동아시아 → 지중해",
            "지중해 → 중국/동아시아": "FBX_지중해 → 중국/동아시아",
            "미주동안 → 북유럽": "FBX_미주동안 → 북유럽",
            "북유럽 → 미주동안": "FBX_북유럽 → 미주동안",
            "유럽 → 남미동안": "FBX_유럽 → 남미동안",
            "유럽 → 남미서안": "FBX_유럽 → 남미서안",
        }
    },
    "XSI": {
        "date_col_idx": 69, # BR열은 날짜
        "data_start_col_idx": 70, # BS열은 동아시아 → 북유럽
        "data_end_col_idx": 77, # BZ열은 북유럽 → 남미동안
        "data_cols_map": {
            "각 항로별($/FEU)": "XSI_날짜", # Date header
            # These keys MUST match the actual headers in the Google Sheet's row 2.
            # Values are the desired JSON keys, prefixed with "XSI_".
            "동아시아 → 북유럽": "동아시아 → 북유럽",
            "북유럽 → 동아시아": "XSI_북유럽 → 동아시아",
            "동아시아 → 미주서안": "XSI_동아시아 → 미주서안",
            "미주서안 → 동아시아": "XSI_미주서안 → 동아시아",
            "동아시아 → 남미동안": "XSI_동아시아 → 남미동안",
            "북유럽 → 미주동안": "XSI_북유럽 → 미주동안",
            "미주동안 → 북유럽": "XSI_미주동안 → 북유럽",
            "북유럽 → 남미동안": "XSI_북유럽 → 남미동안"
        }
    },
    "MBCI": {
        "date_col_idx": 79, # CB열은 날짜
        "data_start_col_idx": 80, # CC열은 MBCI
        "data_end_col_idx": 80, # Assuming H column is also data
        "data_cols_map": {
            "Index(종합지수)": "MBCI_날짜", # Date header
            "MBCI": "MBCI_종합지수", # Changed to MBCI_Value (no double prefix)
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


def fetch_and_process_crawling_data():
    """
    Google Sheet의 'Crawling_Data' 시트에서 데이터를 가져와 처리합니다.
    주로 시계열 차트 데이터를 추출하고 변환합니다.
    """
    if not SPREADSHEET_ID or not GOOGLE_CREDENTIAL_JSON:
        print("오류: SPREADSHEET_ID 또는 GOOGLE_CREDENTIAL_JSON 환경 변수가 설정되지 않았습니다.")
        if not SPREADSHEET_ID:
            print("이유: SPREADSHEET_ID가 None입니다.")
        if not GOOGLE_CREDENTIAL_JSON:
            print("이유: GOOGLE_CREDENTIAL_JSON이 None입니다.")
        return {} # Empty dictionary if configuration is missing

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
            return {}

        # The header row is explicitly stated as row 2 (index 1)
        main_header_row_index = 1 
        if len(all_data_charts) <= main_header_row_index:
            print(f"Error: '{WORKSHEET_NAME_CHARTS}' sheet does not have enough rows for header at index {main_header_row_index}.")
            return {}

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
        
        return processed_chart_data_by_section

    except Exception as e:
        print(f"데이터 처리 중 오류 발생: {e}")
        traceback.print_exc()
        return {} # Return empty dictionary on error

# Example usage (if run as a script)
if __name__ == "__main__":
    # Ensure environment variables are set for testing purposes
    # os.environ["SPREADSHEET_ID"] = "YOUR_SPREADSHEET_ID" 
    # os.environ["GOOGLE_CREDENTIAL_JSON"] = json.dumps({"type": "service_account", ...}) # Your service account JSON

    chart_data = fetch_and_process_crawling_data()
    if chart_data:
        # You can save this data to a JSON file or use it as needed
        output_data = {"chart_data": chart_data}
        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4, cls=NpEncoder)
        print(f"데이터가 '{OUTPUT_JSON_PATH}'에 성공적으로 저장되었습니다.")
    else:
        print("처리할 차트 데이터가 없습니다.")
