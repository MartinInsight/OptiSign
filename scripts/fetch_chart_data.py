import gspread
import json
import os
import pandas as pd # 데이터를 더 쉽게 다루기 위해 pandas 사용

# --- 설정 ---
# GitHub Secrets에서 환경 변수로 가져옵니다.
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDENTIAL_JSON = os.environ.get("GOOGLE_CREDENTIAL_JSON")

# 데이터가 있는 시트 이름
WORKSHEET_NAME = "Crawling_Data"
# JSON 파일을 저장할 경로 (GitHub Pages에서 접근 가능하도록 data/ 폴더에 저장)
OUTPUT_JSON_PATH = "data/crawling_data.json"

def fetch_and_process_data():
    """
    Google Sheet에서 데이터를 가져와 처리하고 JSON 파일로 저장합니다.
    """
    if not SPREADSHEET_ID or not GOOGLE_CREDENTIAL_JSON:
        print("오류: SPREADSHEET_ID 또는 GOOGLE_CREDENTIAL_JSON 환경 변수가 설정되지 않았습니다.")
        return

    try:
        # 1. Google Sheets 인증
        # GOOGLE_CREDENTIAL_JSON은 JSON 문자열이므로 파싱해야 합니다.
        credentials_dict = json.loads(GOOGLE_CREDENTIAL_JSON)
        gc = gspread.service_account_from_dict(credentials_dict)

        # 2. 스프레드시트 및 워크시트 열기
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

        # 3. 모든 데이터 가져오기
        all_data = worksheet.get_all_values()
        if not all_data:
            print("오류: 시트에서 데이터를 가져오지 못했습니다.")
            return

        # 4. 데이터 파싱 및 처리
        # 사용자가 제공한 데이터는 복잡한 헤더를 가지고 있습니다.
        # 실제 데이터가 시작되는 'date' 열을 기준으로 헤더를 찾습니다.
        # 'date' 열이 포함된 행을 실제 헤더로 간주합니다.
        header_row_index = -1
        for i, row in enumerate(all_data):
            if "date" in [cell.strip() for cell in row]: # 'date' 문자열이 포함된 행 찾기
                header_row_index = i
                break

        if header_row_index == -1:
            print("오류: 'date' 열을 포함하는 헤더 행을 찾을 수 없습니다.")
            return

        # 실제 헤더와 데이터 분리
        headers = [h.strip() for h in all_data[header_row_index]]
        data_rows = all_data[header_row_index + 1:]

        # DataFrame으로 변환하여 데이터 처리 용이하게 함
        df = pd.DataFrame(data_rows, columns=headers)

        # 빈 행 제거 (모든 값이 비어있는 행)
        df.replace('', pd.NA, inplace=True) # 빈 문자열을 NaN으로
        df.dropna(how='all', inplace=True) # 모든 값이 NaN인 행 제거

        # 'date' 열을 datetime 형식으로 변환 (오류 무시)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # 유효한 날짜가 있는 행만 남김
        df.dropna(subset=['date'], inplace=True)

        # 필요한 컬럼만 선택하고 숫자형으로 변환 (예시: '종합지수')
        # 사용자 시트의 실제 헤더 이름을 확인하여 여기에 추가하세요.
        # 예시: '종합지수', '미주서안', '미주동안' 등
        relevant_columns = ['date', '종합지수', '미주서안', '미주동안', '유럽', '지중해', '중동', '호주', '남미동안', '남미서안', '남아프리카', '서아프리카', '중국', '일본', '동남아시아'] # 예시 컬럼
        
        # 실제 df에 존재하는 컬럼만 선택
        df_filtered = df[[col for col in relevant_columns if col in df.columns]].copy()

        # 숫자형 컬럼을 숫자로 변환 (오류 무시)
        for col in df_filtered.columns:
            if col != 'date':
                df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

        # NaN 값은 0 또는 다른 값으로 대체 가능 (여기서는 NaN 유지)
        # df_filtered.fillna(0, inplace=True)

        # DataFrame을 JSON 형식으로 변환 (리스트 오브 딕셔너리)
        # orient='records'는 각 행을 하나의 객체로 만듭니다.
        processed_data = df_filtered.to_dict(orient='records')

        # 5. JSON 파일로 저장
        # data/ 폴더가 없으면 생성
        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)

        print(f"데이터를 성공적으로 '{OUTPUT_JSON_PATH}'에 저장했습니다.")
        print(f"저장된 데이터 샘플 (첫 3개): {processed_data[:3]}")

    except Exception as e:
        print(f"데이터 처리 중 오류 발생: {e}")

if __name__ == "__main__":
    fetch_and_process_data()
