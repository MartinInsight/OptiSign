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

# Name of the worksheet containing the exchange rate data
EXCHANGE_RATE_WORKSHEET_NAME = "환율"
OUTPUT_JSON_PATH = "data/exchange_rate_data.json" # Separate output path for exchange rate data

# --- Helper Functions ---
def get_cell_value(data_list, row_idx, col_idx):
    """
    Safely retrieves a cell value from a 2D list.
    Handles out-of-bounds indices by returning an empty string.
    """
    if 0 <= row_idx < len(data_list):
        row = data_list[row_idx]
        if 0 <= col_idx < len(row):
            return str(row[col_idx]).strip()
    return ""

def fetch_and_process_exchange_rate():
    """
    Google Sheet의 '환율' 시트에서 환율 데이터를 가져와 처리합니다.
    날짜별 환율 데이터를 추출하고 JSON 형식으로 반환합니다.
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
        exchange_rate_worksheet = spreadsheet.worksheet(EXCHANGE_RATE_WORKSHEET_NAME)
        all_data_exchange_rate = exchange_rate_worksheet.get_all_values()
        
        print(f"DEBUG: '{EXCHANGE_RATE_WORKSHEET_NAME}'에서 가져온 총 행 수 (원본): {len(all_data_exchange_rate)}")

        if not all_data_exchange_rate:
            print(f"오류: '{EXCHANGE_RATE_WORKSHEET_NAME}' 시트에서 데이터를 가져오지 못했습니다. 환율 데이터가 비어 있습니다.")
            return {}

        # 첫 번째 행을 헤더로 가정합니다. (예: 날짜, USD, EUR, JPY)
        headers = [h.strip() for h in all_data_exchange_rate[0]]
        data_rows = all_data_exchange_rate[1:]

        df_exchange_rate = pd.DataFrame(data_rows, columns=headers)

        # '날짜' 컬럼을 파싱하고 유효하지 않은 날짜는 제거
        date_col_name = "날짜" # 시트의 날짜 컬럼 이름
        if date_col_name in df_exchange_rate.columns:
            df_exchange_rate[date_col_name] = df_exchange_rate[date_col_name].astype(str).str.strip()
            df_exchange_rate['parsed_date'] = pd.to_datetime(df_exchange_rate[date_col_name], errors='coerce')
            df_exchange_rate.dropna(subset=['parsed_date'], inplace=True)
            df_exchange_rate = df_exchange_rate.sort_values(by='parsed_date', ascending=True)
            df_exchange_rate['date'] = df_exchange_rate['parsed_date'].dt.strftime('%Y-%m-%d')
        else:
            print(f"경고: '{date_col_name}' 컬럼이 '{EXCHANGE_RATE_WORKSHEET_NAME}' 시트에 없습니다. 날짜 파싱을 건너뜁니다.")
            return {}

        processed_exchange_rate_data = []
        # 날짜 컬럼을 제외한 모든 컬럼을 환율 데이터로 처리
        for col in df_exchange_rate.columns:
            if col not in [date_col_name, 'parsed_date', 'date']:
                # 숫자형으로 변환, 콤마 제거 및 유효하지 않은 값은 None으로 처리
                df_exchange_rate[col] = pd.to_numeric(df_exchange_rate[col].astype(str).str.replace(',', ''), errors='coerce')
        
        df_exchange_rate = df_exchange_rate.replace({pd.NA: None, float('nan'): None})

        # 최종 JSON 형식으로 변환
        output_cols = ['date'] + [col for col in headers if col != date_col_name]
        existing_output_cols = [col for col in output_cols if col in df_exchange_rate.columns]

        processed_exchange_rate_data = df_exchange_rate[existing_output_cols].to_dict(orient='records')
        
        print(f"DEBUG: Processed Exchange Rate Data (first 3): {processed_exchange_rate_data[:3]}")
        print(f"DEBUG: Processed Exchange Rate Data (last 3): {processed_exchange_rate_data[-3:]}")

        return {"exchange_rate_history": processed_exchange_rate_data}

    except Exception as e:
        print(f"환율 데이터 처리 중 오류 발생: {e}")
        traceback.print_exc()
        return {} # Return empty dictionary on error

if __name__ == "__main__":
    # Ensure environment variables are set for testing purposes
    # os.environ["SPREADSHEET_ID"] = "YOUR_SPREADSHEET_ID" 
    # os.environ["GOOGLE_CREDENTIAL_JSON"] = json.dumps({"type": "service_account", "project_id": "your-project-id", ...}) # Your service account JSON

    exchange_rate_data = fetch_and_process_exchange_rate()
    if exchange_rate_data:
        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(exchange_rate_data, f, ensure_ascii=False, indent=4, cls=NpEncoder)
        print(f"환율 데이터가 '{OUTPUT_JSON_PATH}'에 성공적으로 저장되었습니다.")
    else:
        print("처리할 환율 데이터가 없습니다.")
