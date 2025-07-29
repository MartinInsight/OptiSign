import gspread
import json
import os
import traceback

# 날씨 데이터 워크시트 이름 정의
WEATHER_WORKSHEET_NAME = "LA날씨"

def fetch_la_weather_data(spreadsheet: gspread.Spreadsheet):
    """
    Google Sheet에서 LA 날씨 데이터를 가져와 처리합니다.

    Args:
        spreadsheet: gspread.Spreadsheet 객체.

    Returns:
        dict: 현재 날씨와 예보 데이터를 포함하는 딕셔너리.
              오류 발생 시 빈 딕셔너리를 반환합니다.
    """
    try:
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

        return {
            "current_weather": current_weather,
            "forecast_weather": forecast_weather
        }

    except Exception as e:
        print(f"LA 날씨 데이터를 가져오는 중 오류 발생: {e}")
        traceback.print_exc()
        return {}

# 이 파일이 독립적으로 실행될 때를 위한 테스트 코드 (실제 환경에서는 사용되지 않음)
if __name__ == "__main__":
    # 실제 환경에서는 환경 변수를 통해 자격 증명을 설정해야 합니다.
    # 테스트를 위해 더미 값 또는 실제 자격 증명을 여기에 설정할 수 있습니다.
    # 예: os.environ["SPREADSHEET_ID"] = "YOUR_SPREADSHEET_ID"
    # 예: os.environ["GOOGLE_CREDENTIAL_JSON"] = json.dumps({"type": "service_account", ...})

    # 이 테스트 코드는 실제 gspread.Spreadsheet 객체가 필요합니다.
    # 실제 Google Sheet에 연결하려면 아래 주석을 해제하고 유효한 자격 증명을 제공하세요.
    # try:
    #     SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
    #     GOOGLE_CREDENTIAL_JSON = os.environ.get("GOOGLE_CREDENTIAL_JSON")
    #     if SPREADSHEET_ID and GOOGLE_CREDENTIAL_JSON:
    #         credentials_dict = json.loads(GOOGLE_CREDENTIAL_JSON)
    #         gc = gspread.service_account_from_dict(credentials_dict)
    #         spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    #         weather_data = fetch_la_weather_data(spreadsheet)
    #         print("\n--- Fetched LA Weather Data (Test) ---")
    #         print(json.dumps(weather_data, indent=2, ensure_ascii=False))
    #     else:
    #         print("환경 변수가 설정되지 않아 LA 날씨 데이터를 테스트할 수 없습니다.")
    # except Exception as e:
    #     print(f"LA 날씨 데이터 테스트 중 오류 발생: {e}")
    #     traceback.print_exc()
    pass
