import gspread
import json
import os
import traceback
from datetime import datetime

# EXCHANGE_RATE_WORKSHEET_NAME을 전역으로 정의
EXCHANGE_RATE_WORKSHEET_NAME = "환율"
print(f"DEBUG: fetch_exchange_data.py - EXCHANGE_RATE_WORKSHEET_NAME: {EXCHANGE_RATE_WORKSHEET_NAME}")

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
        
        # '날짜' 또는 'Date' 열과 'USD to KRW' 또는 'Rate' 열을 찾음
        date_col_idx = -1
        rate_col_idx = -1
        
        for i, header in enumerate(headers):
            if header in ["날짜", "Date"]:
                date_col_idx = i
            elif header in ["USD to KRW", "Rate"]:
                rate_col_idx = i
        
        if date_col_idx == -1 or rate_col_idx == -1:
            print("ERROR: '날짜'/'Date' or 'USD to KRW'/'Rate' column not found in '환율' worksheet headers.")
            return []

        historical_rates = []
        # 두 번째 행부터 데이터로 처리
        for row in all_values[1:]:
            if len(row) > max(date_col_idx, rate_col_idx):
                date_str = row[date_col_idx].strip()
                rate_str = row[rate_col_idx].strip().replace(',', '')

                try:
                    # 날짜 파싱 시도 (다양한 형식 고려)
                    # YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD 등
                    parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    try:
                        parsed_date = datetime.strptime(date_str, "%Y/%m/%d")
                    except ValueError:
                        try:
                            parsed_date = datetime.strptime(date_str, "%Y.%m.%d")
                        except ValueError:
                            # 날짜 형식이 아니거나 빈 문자열인 경우 건너뛰기
                            continue

                if rate_str and rate_str.replace('.', '', 1).isdigit():
                    rate_value = float(rate_str)
                    historical_rates.append({
                        "date": parsed_date.strftime("%Y-%m-%d"),
                        "rate": rate_value
                    })
        
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
