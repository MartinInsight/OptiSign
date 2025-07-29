import gspread
import json
import os
from datetime import datetime
import traceback

# WEATHER_WORKSHEET_NAME을 전역으로 정의
WEATHER_WORKSHEET_NAME = "LA날씨"

def fetch_la_weather_data(spreadsheet: gspread.Spreadsheet):
    # DEBUG: print 문을 함수 내부로 이동하여 NameError 방지
    print(f"DEBUG: fetch_la_weather_data.py - WEATHER_WORKSHEET_NAME: {WEATHER_WORKSHEET_NAME} (inside function)")
    try:
        weather_worksheet = spreadsheet.worksheet(WEATHER_WORKSHEET_NAME)
        weather_data_raw = weather_worksheet.get_all_values()

        current_weather = {}
        forecast_weather = []

        # 이미지에 따르면 현재 날씨 데이터는 3행(0-인덱스 기준 2)에 시작합니다.
        # 예: 온도(°F)는 B3, 습도(%)는 B4 등.
        # 현재 날씨 헤더는 1행(0-인덱스 기준 0)에 있습니다.
        # current_weather_headers = [h.strip() for h in weather_data_raw[0]] # 이 줄은 사용하지 않지만 참고용으로 유지

        # 현재 날씨 값은 시트의 3행(0-인덱스 기준 2)에 있습니다.
        if len(weather_data_raw) > 2: # 최소 3행이 있어야 현재 날씨 데이터를 읽을 수 있습니다.
            current_weather_values_row_idx = 2 # 시트의 3행 (0-인덱스 기준 2)
            current_weather_values = weather_data_raw[current_weather_values_row_idx]

            # 이미지에 따른 컬럼 인덱스 조정
            current_weather = {
                "LA_Temperature": current_weather_values[1].strip() if len(current_weather_values) > 1 else None, # B3
                "LA_WeatherStatus": weather_data_raw[0][1].strip() if len(weather_data_raw) > 0 and len(weather_data_raw[0]) > 1 else None, # B1 (날씨 상태)
                "LA_Humidity": weather_data_raw[3][1].strip() if len(weather_data_raw) > 3 and len(weather_data_raw[3]) > 1 else None, # B4
                "LA_WindSpeed": weather_data_raw[4][1].strip() if len(weather_data_raw) > 4 and len(weather_data_raw[4]) > 1 else None, # B5
                "LA_Pressure": weather_data_raw[5][1].strip() if len(weather_data_raw) > 5 and len(weather_data_raw[5]) > 1 else None, # B6
                "LA_Visibility": weather_data_raw[6][1].strip() if len(weather_data_raw) > 6 and len(weather_data_raw[6]) > 1 else None, # B7
                "LA_Sunrise": weather_data_raw[7][1].strip() if len(weather_data_raw) > 7 and len(weather_data_raw[7]) > 1 else None, # B8
                "LA_Sunset": weather_data_raw[8][1].strip() if len(weather_data_raw) > 8 and len(weather_data_raw[8]) > 1 else None, # B9
            }
            # '날씨 아이콘'은 차트에 직접 표시되지 않으므로 제외했습니다.
            # 'LA_WeatherStatus'는 B1에서 가져오도록 변경했습니다.

        # 이미지에 따르면 예보 헤더는 11행(0-인덱스 기준 10)에 있습니다.
        # 예보 데이터는 12행(0-인덱스 기준 11)부터 시작합니다.
        if len(weather_data_raw) > 11: # 최소 12행이 있어야 예보 데이터를 읽을 수 있습니다.
            # forecast_headers = [h.strip() for h in weather_data_raw[10]] # 이 줄은 사용하지 않지만 참고용으로 유지
            
            # 예보 데이터가 시작하는 행 (시트의 12행부터, 0-인덱스 기준 11)
            for row_idx in range(11, len(weather_data_raw)): 
                row_values = weather_data_raw[row_idx]
                # 예보 데이터는 최소 4개의 열(날짜, 최저, 최고, 상태)을 가져야 합니다.
                if len(row_values) >= 4:
                    forecast_day = {
                        "date": row_values[0].strip(), # A열
                        "min_temp": row_values[1].strip(), # B열
                        "max_temp": row_values[2].strip(), # C열
                        "status": row_values[3].strip() # D열
                    }
                    forecast_weather.append(forecast_day)
        
        print(f"DEBUG: Current Weather Data: {current_weather}")
        print(f"DEBUG: Forecast Weather Data (first 3): {forecast_weather[:3]}")
        return {"current_weather": current_weather, "forecast_weather": forecast_weather}

    except Exception as e:
        print(f"날씨 데이터를 가져오는 중 오류 발생: {e}")
        traceback.print_exc()
        return {"current_weather": {}, "forecast_weather": []}

if __name__ == "__main__":
    pass
