import gspread
import json
import os
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

# Name of the worksheet containing the weather data
WEATHER_WORKSHEET_NAME = "LA날씨"
OUTPUT_JSON_PATH = "data/la_weather_data.json" # Separate output path for weather data

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

def fetch_and_process_la_weather():
    """
    Google Sheet의 'LA날씨' 시트에서 날씨 데이터를 가져와 처리합니다.
    현재 날씨와 예보 데이터를 추출하고 JSON 형식으로 반환합니다.
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
        weather_worksheet = spreadsheet.worksheet(WEATHER_WORKSHEET_NAME)
        weather_data_raw = weather_worksheet.get_all_values()
        
        processed_weather_data = {
            "current_weather": {},
            "forecast_weather": []
        }

        # 현재 날씨 데이터 처리
        if len(weather_data_raw) >= 9: # 현재 날씨에 충분한 행이 있는지 확인 (A1:B9)
            processed_weather_data['current_weather']['LA_WeatherStatus'] = get_cell_value(weather_data_raw, 0, 1) # B1
            processed_weather_data['current_weather']['LA_WeatherIcon'] = get_cell_value(weather_data_raw, 1, 1) # B2
            
            # 온도, 습도 등을 숫자로 변환 (B3:B7)
            temp_str = get_cell_value(weather_data_raw, 2, 1) # B3
            processed_weather_data['current_weather']['LA_Temperature'] = float(temp_str) if temp_str and temp_str.replace('.', '', 1).replace('-', '', 1).isdigit() else None
            
            humidity_str = get_cell_value(weather_data_raw, 3, 1) # B4
            processed_weather_data['current_weather']['LA_Humidity'] = float(humidity_str) if humidity_str and humidity_str.replace('.', '', 1).replace('-', '', 1).isdigit() else None
            
            wind_speed_str = get_cell_value(weather_data_raw, 4, 1) # B5
            processed_weather_data['current_weather']['LA_WindSpeed'] = float(wind_speed_str) if wind_speed_str and wind_speed_str.replace('.', '', 1).replace('-', '', 1).isdigit() else None
            
            pressure_str = get_cell_value(weather_data_raw, 5, 1) # B6
            processed_weather_data['current_weather']['LA_Pressure'] = float(pressure_str) if pressure_str and pressure_str.replace('.', '', 1).replace('-', '', 1).isdigit() else None
            
            visibility_str = get_cell_value(weather_data_raw, 6, 1) # B7
            processed_weather_data['current_weather']['LA_Visibility'] = float(visibility_str) if visibility_str and visibility_str.replace('.', '', 1).replace('-', '', 1).isdigit() else None
            
            processed_weather_data['current_weather']['LA_Sunrise'] = get_cell_value(weather_data_raw, 7, 1) # B8
            processed_weather_data['current_weather']['LA_Sunset'] = get_cell_value(weather_data_raw, 8, 1) # B9
            processed_weather_data['current_weather']['LA_FineDust'] = None # 시트에 없는 미세먼지 플레이스홀더

        # 예보 날씨 데이터 처리
        if len(weather_data_raw) > 12: # 예보 데이터가 있는지 확인 (A12부터 시작)
            for row_idx in range(11, len(weather_data_raw)): # 12행 (인덱스 11)부터 순회
                row = weather_data_raw[row_idx]
                # 각 행의 길이가 충분하고 첫 번째 셀(날짜)이 비어있지 않은지 확인
                if len(row) >= 5 and get_cell_value(weather_data_raw, row_idx, 0): 
                    date_val = get_cell_value(weather_data_raw, row_idx, 0) # A열
                    min_temp_str = get_cell_value(weather_data_raw, row_idx, 1) # B열
                    max_temp_str = get_cell_value(weather_data_raw, row_idx, 2) # C열
                    status_val = get_cell_value(weather_data_raw, row_idx, 3) # D열
                    icon_val = get_cell_value(weather_data_raw, row_idx, 4) # E열

                    forecast_day = {
                        'date': date_val,
                        'min_temp': float(min_temp_str) if min_temp_str and min_temp_str.replace('.', '', 1).replace('-', '', 1).isdigit() else None,
                        'max_temp': float(max_temp_str) if max_temp_str and max_temp_str.replace('.', '', 1).replace('-', '', 1).isdigit() else None,
                        'status': status_val,
                        'icon': icon_val # 아이콘 이름 또는 경로 가정
                    }
                    processed_weather_data['forecast_weather'].append(forecast_day)
        
        print(f"DEBUG: Processed Current Weather Data: {processed_weather_data['current_weather']}")
        print(f"DEBUG: Processed Forecast Weather Data (first 3): {processed_weather_data['forecast_weather'][:3]}")
        
        return processed_weather_data

    except Exception as e:
        print(f"LA 날씨 데이터 처리 중 오류 발생: {e}")
        traceback.print_exc()
        return {} # Return empty dictionary on error

if __name__ == "__main__":
    # Ensure environment variables are set for testing purposes
    # os.environ["SPREADSHEET_ID"] = "YOUR_SPREADSHEET_ID" 
    # os.environ["GOOGLE_CREDENTIAL_JSON"] = json.dumps({"type": "service_account", "project_id": "your-project-id", ...}) # Your service account JSON

    weather_data = fetch_and_process_la_weather()
    if weather_data:
        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(weather_data, f, ensure_ascii=False, indent=4, cls=NpEncoder)
        print(f"LA 날씨 데이터가 '{OUTPUT_JSON_PATH}'에 성공적으로 저장되었습니다.")
    else:
        print("처리할 LA 날씨 데이터가 없습니다.")
