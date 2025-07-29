import gspread
import json
import os
from datetime import datetime
import traceback

WEATHER_WORKSHEET_NAME = "LA날씨"

def fetch_la_weather_data(spreadsheet: gspread.Spreadsheet):
    try:
        weather_worksheet = spreadsheet.worksheet(WEATHER_WORKSHE_NAME)
        weather_data_raw = weather_worksheet.get_all_values()

        current_weather = {}
        forecast_weather = []

        if len(weather_data_raw) > 1:
            # 현재 날씨 데이터 (첫 번째 행)
            current_weather_headers = [h.strip() for h in weather_data_raw[0]]
            current_weather_values = weather_data_raw[1]

            current_weather = {
                "LA_Temperature": current_weather_values[0].strip() if len(current_weather_values) > 0 else None,
                "LA_WeatherStatus": current_weather_values[1].strip() if len(current_weather_values) > 1 else None,
                "LA_Humidity": current_weather_values[2].strip() if len(current_weather_values) > 2 else None,
                "LA_WindSpeed": current_weather_values[3].strip() if len(current_weather_values) > 3 else None,
                "LA_Pressure": current_weather_values[4].strip() if len(current_weather_values) > 4 else None,
                "LA_Visibility": current_weather_values[5].strip() if len(current_weather_values) > 5 else None,
                "LA_Sunrise": current_weather_values[6].strip() if len(current_weather_values) > 6 else None,
                "LA_Sunset": current_weather_values[7].strip() if len(current_weather_values) > 7 else None,
            }

            # 일기 예보 데이터 (세 번째 행부터)
            if len(weather_data_raw) > 2:
                forecast_headers = [h.strip() for h in weather_data_raw[2]]
                for row_idx in range(3, len(weather_data_raw)):
                    row_values = weather_data_raw[row_idx]
                    if len(row_values) >= 4:
                        forecast_day = {
                            "date": row_values[0].strip(),
                            "min_temp": row_values[1].strip(),
                            "max_temp": row_values[2].strip(),
                            "status": row_values[3].strip()
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
