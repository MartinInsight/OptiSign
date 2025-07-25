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

# Name of the worksheet containing the table data
WORKSHEET_NAME_TABLES = "Crawling_Data2"
OUTPUT_JSON_PATH = "data/crawling_data_tables.json" # Separate output path for table data

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

def calculate_change_and_percentage(current_val_str, previous_val_str):
    """
    Calculates the weekly change and percentage, returning formatted strings
    and a color class based on the change.
    """
    change_value = None
    percentage_string = None
    color_class = "text-gray-700" # Default color

    try:
        current_val = float(current_val_str.replace(',', '')) if current_val_str and current_val_str.replace('.', '', 1).replace('-', '', 1).isdigit() else None
        previous_val = float(previous_val_str.replace(',', '')) if previous_val_str and previous_val_str.replace('.', '', 1).replace('-', '', 1).isdigit() else None

        if current_val is not None and previous_val is not None:
            change_value = current_val - previous_val
            if previous_val != 0:
                percentage = (change_value / previous_val) * 100
                percentage_string = f"{percentage:.2f}%"
            else:
                percentage_string = "N/A" # Handle division by zero

            if change_value > 0:
                color_class = "text-red-500"
            elif change_value < 0:
                color_class = "text-blue-500"
            
            change_value = f"{change_value:.2f}"
            
    except ValueError:
        pass # Keep values as None if conversion fails

    return change_value, percentage_string, color_class

# --- Main Processing Function for Crawling_Data2 ---
def process_table_data_from_crawling_data2(raw_data):
    """
    Processes the raw data fetched from the 'Crawling_Data2' sheet to extract
    and format various table data sections (KCCI, SCFI, WCI, etc.).
    """
    table_data = {}
    
    # --- KCCI Table ---
    # KCCI (날짜 표기 형식: Current Index (2025-07-21), Previous Index (2025-07-14))
    # Current date: A3 (row 2, col 0)
    # Current Index data: B3:O3 (row 2, cols 1-14)
    # Previous date: A4 (row 3, col 0)
    # Previous Index data: B4:O4 (cols 1 to 14)
    
    kcci_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    kcci_rows = []

    kcci_current_date_raw_label = get_cell_value(raw_data, 2, 0) # A3
    kcci_previous_date_raw_label = get_cell_value(raw_data, 3, 0) # A4

    # Extract date from "Current Index (YYYY-MM-DD)" and "Previous Index (YYYY-MM-DD)"
    current_date_match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", kcci_current_date_raw_label)
    previous_date_match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", kcci_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(1), "%Y-%m-%d")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(1), "%Y-%m-%d")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    kcci_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    kcci_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # KCCI routes and their corresponding 0-indexed column in the sheet
    kcci_routes_data_cols = {
        "종합지수": 1, # B column
        "미주서안": 2, # C column
        "미주동안": 3, # D column
        "유럽": 4, # E column
        "지중해": 5, # F column
        "중동": 6, # G column
        "호주": 7, # H column
        "남미동안": 8, # I column
        "남미서안": 9, # J column
        "남아프리카": 10, # K column
        "서아프리카": 11, # L column
        "중국": 12, # M column
        "일본": 13, # N column
        "동남아시아": 14 # O column
    }

    for route_name, col_idx in kcci_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 2, col_idx) # B3 to O3
        previous_val = get_cell_value(raw_data, 3, col_idx) # B4 to O4
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        kcci_rows.append({
            "route": f"KCCI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["KCCI"] = {"headers": kcci_display_headers, "rows": kcci_rows}


    # --- SCFI Table ---
    # SCFI (날짜 표기 형식: Current Index (2025-07-18), Previous Index (2025-07-11))
    # Current date: A9 (row 8, col 0)
    # Current Index data: B9:O9 (row 8, cols 1-14)
    # Previous date: A10 (row 9, col 0)
    # Previous Index data: B10:O10 (row 9, cols 1-14)

    scfi_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    scfi_rows = []

    scfi_current_date_raw_label = get_cell_value(raw_data, 8, 0) # A9
    scfi_previous_date_raw_label = get_cell_value(raw_data, 9, 0) # A10

    current_date_match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", scfi_current_date_raw_label)
    previous_date_match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", scfi_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(1), "%Y-%m-%d")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(1), "%Y-%m-%d")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    scfi_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    scfi_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # SCFI routes and their corresponding 0-indexed column in the sheet - FIXED KEY-VALUE PAIRS
    scfi_routes_data_cols = {
        "종합지수": 1, # B column
        "유럽 (기본항)": 2, # C column
        "지중해 (기본항)": 3, # D column
        "미주서안 (기본항)": 4, # E column
        "미주동안 (기본항)": 5, # F column
        "페르시아만/홍해 (두바이)": 6, # G column
        "호주/뉴질랜드 (멜버른)": 7, # H column
        "동/서 아프리카 (라고스)": 8, # I column
        "남아프리카 (더반)": 9, # J column
        "서일본 (기본항)": 10, # K column
        "동일본 (기본항)": 11, # L column
        "동남아시아 (싱가포르)": 12, # M column
        "한국 (부산)": 13, # N column
        "중남미서안 (만사니요)": 14 # O column
    }

    for route_name, col_idx in scfi_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 8, col_idx) # B9 to O9
        previous_val = get_cell_value(raw_data, 9, col_idx) # B10 to O10
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        scfi_rows.append({
            "route": f"SCFI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["SCFI"] = {"headers": scfi_display_headers, "rows": scfi_rows}


    # --- WCI Table ---
    # WCI (날짜표기형식: 7/17/2025, 7/10/2025)
    # Current date: A21 (row 20, col 0)
    # Current Index data: B21:J21 (row 20, cols 1-9)
    # Previous date: A22 (row 21, col 0)
    # Previous Index data: B22:J22 (row 21, cols 1-9)

    wci_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    wci_rows = []

    wci_current_date_raw_label = get_cell_value(raw_data, 20, 0) # A21
    wci_previous_date_raw_label = get_cell_value(raw_data, 21, 0) # A22

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", wci_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", wci_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    wci_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    wci_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # WCI routes and their corresponding 0-indexed column in the sheet - FIXED KEY-VALUE PAIRS
    wci_routes_data_cols = {
        "종합지수": 1, # B column
        "상하이 → 로테르담": 2, # C column
        "로테르담 → 상하이": 3, # D column
        "상하이 → 제노바": 4, # E column
        "상하이 → 로스엔젤레스": 5, # F column
        "로스엔젤레스 → 상하이": 6, # G column
        "상하이 → 뉴욕": 7, # H column
        "뉴욕 → 로테르담": 8, # I column
        "로테르담 → 뉴욕": 9 # J column
    }

    for route_name, col_idx in wci_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 20, col_idx) # B21 to J21
        previous_val = get_cell_value(raw_data, 21, col_idx) # B22 to J22
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        wci_rows.append({
            "route": f"WCI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["WCI"] = {"headers": wci_display_headers, "rows": wci_rows}


    # --- IACI Table ---
    # IACI (날짜 표기 형식: 7/15/2025, 6/30/2025)
    # Current date: A27 (row 26, col 0)
    # Current Index data: B27 (row 26, col 1)
    # Previous date: A28 (row 27, col 0)
    # Previous Index data: B28 (row 27, col 1)

    iaci_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    iaci_rows = []

    iaci_current_date_raw_label = get_cell_value(raw_data, 26, 0) # A27
    iaci_previous_date_raw_label = get_cell_value(raw_data, 27, 0) # A28

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", iaci_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", iaci_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    iaci_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    iaci_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # IACI routes and their corresponding 0-indexed column in the sheet - FIXED KEY-VALUE PAIRS
    iaci_routes_data_cols = {
        "종합지수": 1 # B column
    }

    for route_name, col_idx in iaci_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 26, col_idx) # B27
        previous_val = get_cell_value(raw_data, 27, col_idx) # B28
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        iaci_rows.append({
            "route": f"IACI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["IACI"] = {"headers": iaci_display_headers, "rows": iaci_rows}


    # --- BLANK SAILING Table ---
    # BLANK SAILING (날짜 표기 형식: 7/18/2025, 7/11/2025, 7/4/2025, 6/27/2025, 6/20/2025)
    # Current date: A33 (row 32, col 0)
    # Current Index data: B33:G33 (row 32, cols 1-6)
    # Previous date_1: A34 (row 33, col 0)
    # Previous Index data_1: B34:G34 (row 33, cols 1-6)

    blank_sailing_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    blank_sailing_rows = []

    blank_sailing_current_date_raw_label = get_cell_value(raw_data, 32, 0) # A33
    blank_sailing_previous_date_raw_label = get_cell_value(raw_data, 33, 0) # A34

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", blank_sailing_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", blank_sailing_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    blank_sailing_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    blank_sailing_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # BLANK SAILING routes and their corresponding 0-indexed column in the sheet - FIXED KEY-VALUE PAIRS
    blank_sailing_routes_data_cols = {
        "Gemini Cooperation": 1, # B column
        "MSC": 2, # C column
        "OCEAN Alliance": 3, # D column
        "Premier Alliance": 4, # E column
        "Others/Independent": 5, # F column
        "Total": 6 # G column
    }

    for route_name, col_idx in blank_sailing_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 32, col_idx) # B33 to G33
        previous_val = get_cell_value(raw_data, 33, col_idx) # B34 to G34
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        blank_sailing_rows.append({
            "route": f"BLANK_SAILING_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["BLANK_SAILING"] = {"headers": blank_sailing_display_headers, "rows": blank_sailing_rows}


    # --- FBX Table ---
    # FBX (날짜 표기 형식: 7/18/2025, 7/11/2025)
    # Current date: A41 (row 40, col 0)
    # Current Index data: B41:N41 (row 40, cols 1-13)
    # Previous date: A42 (row 41, col 0)
    # Previous Index data: B42:N41 (row 41, cols 1-13)

    fbx_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    fbx_rows = []

    fbx_current_date_raw_label = get_cell_value(raw_data, 40, 0) # A41
    fbx_previous_date_raw_label = get_cell_value(raw_data, 41, 0) # A42

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", fbx_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", fbx_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    fbx_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    fbx_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # FBX routes and their corresponding 0-indexed column in the sheet - FIXED KEY-VALUE PAIRS
    fbx_routes_data_cols = {
        "종합지수": 1, # B column
        "중국/동아시아 → 미주서안": 2, # C column
        "미주서안 → 중국/동아시아": 3, # D column
        "중국/동아시아 → 미주동안": 4, # E column
        "미주동안 → 중국/동아시아": 5, # F column
        "중국/동아시아 → 북유럽": 6, # G column
        "북유럽 → 중국/동아시아": 7, # H column
        "중국/동아시아 → 지중해": 8, # I column
        "지중해 → 중국/동아시아": 9, # J column
        "미주동안 → 북유럽": 10, # K column
        "북유럽 → 미주동안": 11, # L column
        "유럽 → 남미동안": 12, # M column
        "유럽 → 남미서안": 13 # N column
    }

    for route_name, col_idx in fbx_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 40, col_idx) # B41 to N41
        previous_val = get_cell_value(raw_data, 41, col_idx) # B42 to N42
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        fbx_rows.append({
            "route": f"FBX_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["FBX"] = {"headers": fbx_display_headers, "rows": fbx_rows}


    # --- XSI Table ---
    # XSI (날짜 표기 형식: 7/23/2025, 7/16/2025)
    # Current date: A47 (row 46, col 0)
    # Current Index data: B47:I47 (row 46, cols 1-8)
    # Previous date: A48 (row 47, col 0)
    # Previous Index data: B48:N48 (row 47, cols 1-13)

    xsi_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    xsi_rows = []

    xsi_current_date_raw_label = get_cell_value(raw_data, 46, 0) # A47
    xsi_previous_date_raw_label = get_cell_value(raw_data, 47, 0) # A48

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", xsi_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", xsi_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    xsi_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    xsi_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    xsi_routes_data_cols = {
        "동아시아 → 북유럽": {"current_col": 1, "previous_col": 1}, # B47, B48
        "북유럽 → 동아시아": {"current_col": 2, "previous_col": 2}, # C47, C48
        "동아시아 → 미주서안": {"current_col": 3, "previous_col": 3}, # D47, D48
        "미주서안 → 동아시아": {"current_col": 4, "previous_col": 4}, # E47, E48
        "동아시아 → 남미동안": {"current_col": 5, "previous_col": 5}, # F47, F48
        "북유럽 → 미주동안": {"current_col": 6, "previous_col": 6}, # G47, G48
        "미주동안 → 북유럽": {"current_col": 7, "previous_col": 7}, # H47, H48
        "북유럽 → 남미동안": {"current_col": 8, "previous_col": 8}  # I47, I48
    }
    # Note: User specified Previous Index data: B48:N48. However, Current Index data is B47:I47.
    # To maintain consistency in "route" mapping and calculation, I will use the corresponding column for previous data.
    # If a route has current data in col X, its previous data will be taken from col X in the previous row.
    # The range B48:N48 might contain additional data not directly corresponding to the routes in B47:I47.
    # If the user intends to show more previous data, the table structure would need a more complex change.
    # For now, I will align previous_col with current_col for each route.

    for route_name, cols in xsi_routes_data_cols.items():
        current_val = get_cell_value(raw_data, 46, cols["current_col"]) # Current data from row 46 (B-I)
        previous_val = get_cell_value(raw_data, 47, cols["previous_col"]) # Previous data from row 47 (B-I)
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        xsi_rows.append({
            "route": f"XSI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["XSI"] = {"headers": xsi_display_headers, "rows": xsi_rows}


    # --- MBCI Table ---
    # MBCI (날짜 표기 형식: 7/18/2025, 7/11/2025)
    # Current date: A59 (row 58, col 0)
    # Current Index data: G59 (row 58, col 6)
    # Previous date: A60 (row 59, col 0)
    # Previous Index data: G60 (row 59, col 6)

    mbci_display_headers = ["항로", "Current Index", "Previous Index", "Weekly Change"]
    mbci_rows = []

    mbci_current_date_raw_label = get_cell_value(raw_data, 58, 0) # A59
    mbci_previous_date_raw_label = get_cell_value(raw_data, 59, 0) # A60

    # Extract date from "M/D/YYYY"
    current_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", mbci_current_date_raw_label)
    previous_date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", mbci_previous_date_raw_label)

    current_date_formatted = ""
    if current_date_match:
        try:
            date_obj = datetime.strptime(current_date_match.group(0), "%m/%d/%Y")
            current_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    previous_date_formatted = ""
    if previous_date_match:
        try:
            date_obj = datetime.strptime(previous_date_match.group(0), "%m/%d/%Y")
            previous_date_formatted = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            pass

    mbci_display_headers[1] = f"Current Index ({current_date_formatted})" if current_date_formatted else "Current Index"
    mbci_display_headers[2] = f"Previous Index ({previous_date_formatted})" if previous_date_formatted else "Previous Index"

    # MBCI routes and their corresponding 0-indexed column in the sheet
    # IACI와 동일한 구조로 통일
    mbci_routes_data_cols = {
        "MBCI": 6, # G column
    }

    for route_name, col_idx in mbci_routes_data_cols.items(): # col_idx를 직접 사용
        current_val = get_cell_value(raw_data, 58, col_idx) # Current data from row 58 (G)
        previous_val = get_cell_value(raw_data, 59, col_idx) # Previous data from row 59 (G)
        change_value, percentage_string, color_class = calculate_change_and_percentage(current_val, previous_val)
        mbci_rows.append({
            "route": f"MBCI_{route_name}", # Prefix with section key
            "current_index": current_val,
            "previous_index": previous_val,
            "weekly_change": {
                "value": change_value,
                "percentage": percentage_string,
                "color_class": color_class
            }
        })
    table_data["MBCI"] = {"headers": mbci_display_headers, "rows": mbci_rows}

    return table_data

def fetch_and_process_crawling_data2_sheet():
    """
    Google Sheet에서 'Crawling_Data2' 시트의 데이터를 가져와 처리합니다.
    주로 테이블 데이터를 추출하고 변환합니다.
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

        # --- Fetch Table Data (from Crawling_Data2) ---
        worksheet_tables = spreadsheet.worksheet(WORKSHEET_NAME_TABLES)
        all_data_tables = worksheet_tables.get_all_values()

        print(f"DEBUG: '{WORKSHEET_NAME_TABLES}'에서 가져온 총 행 수 (원본): {len(all_data_tables)}")

        if not all_data_tables:
            print(f"오류: '{WORKSHEET_NAME_TABLES}' 시트에서 데이터를 가져오지 못했습니다. 테이블 데이터가 비어 있습니다.")
            return {}

        processed_table_data = process_table_data_from_crawling_data2(all_data_tables)
        print(f"DEBUG: Processed Table Data (first section): {list(processed_table_data.keys())[0] if processed_table_data else 'N/A'}")
        
        return processed_table_data

    except Exception as e:
        print(f"데이터 처리 중 오류 발생: {e}")
        traceback.print_exc()
        return {} # Return empty dictionary on error

# Example usage (if run as a script)
if __name__ == "__main__":
    # Ensure environment variables are set for testing purposes
    # os.environ["SPREADSHEET_ID"] = "YOUR_SPREADSHEET_ID" 
    # os.environ["GOOGLE_CREDENTIAL_JSON"] = json.dumps({"type": "service_account", "project_id": "your-project-id", ...}) # Your service account JSON

    table_data = fetch_and_process_crawling_data2_sheet()
    if table_data:
        # You can save this data to a JSON file or use it as needed
        output_data = {"table_data": table_data}
        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4, cls=NpEncoder)
        print(f"테이블 데이터가 '{OUTPUT_JSON_PATH}'에 성공적으로 저장되었습니다.")
    else:
        print("처리할 테이블 데이터가 없습니다.")
