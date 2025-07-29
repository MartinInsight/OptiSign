import gspread
import json
import os
import traceback

EXCHANGE_RATE_WORKSHEET_NAME = "환율"

def fetch_exchange_data(spreadsheet: gspread.Spreadsheet):
    try:
        exchange_rate_worksheet = spreadsheet.worksheet(EXCHANGE_RATE_WORKSHEET_NAME)
        exchange_rate_data_raw = exchange_rate_worksheet.get_all_values()

        exchange_rate = {}
        if len(exchange_rate_data_raw) >= 2:
            exchange_headers = [h.strip() for h in exchange_rate_data_raw[0]]
            exchange_values = exchange_rate_data_raw[1]

            for i, header in enumerate(exchange_headers):
                if i < len(exchange_values):
                    val = exchange_values[i].strip().replace(',', '')
                    exchange_rate[header] = float(val) if val and val.replace('.', '', 1).isdigit() else None
        
        print(f"DEBUG: Exchange Rate Data: {exchange_rate}")
        return exchange_rate

    except Exception as e:
        print(f"환율 데이터를 가져오는 중 오류 발생: {e}")
        traceback.print_exc()
        return {}

if __name__ == "__main__":
    pass
