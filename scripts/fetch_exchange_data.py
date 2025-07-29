import gspread
import json
import os
import traceback
from datetime import datetime # timedelta는 더 이상 필요 없으므로 제거

# EXCHANGE_RATE_WORKSHEET_NAME을 전역으로 정의
EXCHANGE_RATE_WORKSHEET_NAME = "환율"
print(f"DEBUG: fetch_exchange_data.py - WEATHER_WORKSHEET_NAME: {EXCHANGE_RATE_WORKSHEET_NAME}")

def fetch_exchange_data(spreadsheet: gspread.Spreadsheet):
    try:
        exchange_rate_worksheet = spreadsheet.worksheet(EXCHANGE_RATE_WORKSHEET_NAME)
        # 모든 값을 가져와서 DataFrame으로 처리하여 날짜별 데이터를 추출
        all_values = exchange_rate_worksheet.get_all_values()

        if not all_values:
            print("WARNING: No data found in the '환율' worksheet.")
            return []

        # 첫 번째 행을 헤더로 사용
        headers = [h.strip() for h in all_values[0]]
        print(f"DEBUG: fetch_exchange_data.py - Headers: {headers}")
        
        # '날짜' 또는 'Date' 열과 'USD to KRW' 또는 'Rate' 또는 '환율' 열을 찾음
        date_col_idx = -1
        rate_col_idx = -1
        
        for i, header in enumerate(headers):
            if header in ["날짜", "Date"]:
                date_col_idx = i
            elif header in ["USD to KRW", "Rate", "환율"]: # "환율" 헤더 추가
                rate_col_idx = i
        
        if date_col_idx == -1 or rate_col_idx == -1:
            print("ERROR: '날짜'/'Date' or 'USD to KRW'/'Rate'/'환율' column not found in '환율' worksheet headers.")
            return []

        historical_rates = []
        
        # 두 번째 행부터 데이터로 처리
        for row_num, row in enumerate(all_values[1:], start=2): # 행 번호는 1부터 시작하므로, 실제 시트 행 번호를 위해 start=2
            print(f"DEBUG: Processing row {row_num}: {row}") # 각 행의 원본 데이터 로그
            if len(row) > max(date_col_idx, rate_col_idx):
                date_str = row[date_col_idx].strip()
                rate_str = row[rate_col_idx].strip().replace(',', '') # 쉼표 제거

                parsed_date = None
                try:
                    # 새로운 "MM-DD-YYYY" 형식으로 날짜 파싱
                    parsed_date = datetime.strptime(date_str, "%m-%d-%Y")
                except ValueError:
                    print(f"WARNING: Row {row_num} - Could not parse date '{date_str}' with format MM-DD-YYYY. Skipping row.")
                    continue # 날짜 파싱에 실패한 경우 건너뛰기

                if rate_str and rate_str.replace('.', '', 1).replace('-', '', 1).isdigit(): # 음수 처리 추가
                    try:
                        rate_value = float(rate_str)
                        historical_rates.append({
                            "date": parsed_date.strftime("%Y-%m-%d"),
                            "rate": rate_value
                        })
                        print(f"DEBUG: Row {row_num} - Successfully parsed date '{date_str}' and rate '{rate_str}'.")
                    except ValueError:
                        print(f"WARNING: Row {row_num} - Could not convert rate '{rate_str}' to float. Skipping row.")
                else:
                    print(f"WARNING: Row {row_num} - Could not parse rate '{rate_str}' (not a valid number). Skipping row.")
            else:
                print(f"WARNING: Row {row_num} - Not enough columns for date/rate data. Skipping row.")
        
        # 날짜 순으로 정렬
        historical_rates.sort(key=lambda x: x['date'])

        print(f"DEBUG: Historical Exchange Rate Data (first 3): {historical_rates[:3]}")
        print(f"DEBUG: Historical Exchange Rate Data (last 3): {historical_rates[-3:]}")
        return historical_rates

    except Exception as e:
        print(f"환율 데이터를 가져오는 중 오류 발생: {e}")
        traceback.print_exc()
        return []

if __name__ == "__main__":
    pass
