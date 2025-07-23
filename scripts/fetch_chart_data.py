import gspread
import pandas as pd
import json
from datetime import datetime, timedelta
import os # Import os module to handle file paths

# Configuration for Google Sheets API
# Ensure your service account key file is named 'service_account.json'
# and is located in the same directory or accessible path.
# For local development, you might need to set GOOGLE_APPLICATION_CREDENTIALS
# environment variable or place the key file in a known location.

# Determine the base directory of the script
# This helps in locating service_account.json relative to the script
script_dir = os.path.dirname(os.path.abspath(__file__))
# Assume service_account.json is in the project root (one level up from 'scripts')
service_account_path = os.path.join(script_dir, '..', 'service_account.json')

try:
    # Explicitly specify the path to the service account key file
    gc = gspread.service_account(filename=service_account_path)
except FileNotFoundError:
    print(f"Error: service_account.json not found at {service_account_path}")
    print("Please ensure 'service_account.json' is correctly configured and accessible in the project root.")
    exit()
except Exception as e:
    print(f"Error authenticating with Google Sheets API: {e}")
    print("Please ensure 'service_account.json' is correctly configured and accessible.")
    exit()

def fetch_data_from_sheet(sheet_name):
    """Fetches data from a specified Google Sheet and processes it."""
    try:
        sh = gc.open("OptiSign Dashboard Data") # Open the Google Sheet by its name
        worksheet = sh.worksheet(sheet_name) # Select the specific worksheet
        data = worksheet.get_all_records() # Get all data as a list of dictionaries
        return data
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Spreadsheet 'OptiSign Dashboard Data' not found.")
        return []
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Worksheet '{sheet_name}' not found in 'OptiSign Dashboard Data'.")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while fetching data from '{sheet_name}': {e}")
        return []

def process_chart_data(raw_data):
    """
    Processes raw chart data, ensuring dates are handled correctly
    and preparing it for Chart.js.
    """
    processed_data = {
        "KCCI": [], "SCFI": [], "WCI": [], "IACI": [],
        "BLANK_SAILING": [], "FBX": [], "XSI": [], "MBCI": []
    }

    # Assuming raw_data is a list of dictionaries where each dict is a row
    # and contains 'date' and various index values.
    # We need to structure this to be easily consumable by each chart.

    # Example: KCCI data extraction (adjust keys based on your actual sheet columns)
    # This part needs to be carefully mapped to your Google Sheet's column headers
    # from the 'Crawling_Data' sheet if it's different from the 'Crawling_Data2' structure.
    # For now, let's assume 'raw_data' contains all time-series data.
    # If 'Crawling_Data' has a different structure, this needs to be split.

    # Let's assume the main 'Crawling_Data' sheet has columns like:
    # 'Date', 'KCCI_Composite_Index', 'KCCI_US_West_Coast', etc.
    # If your 'Crawling_Data' sheet is structured differently,
    # you'll need to adjust the column names here.

    for row in raw_data:
        date_str = row.get('Date')
        if not date_str:
            continue

        try:
            # Attempt to parse various date formats
            # Prioritize 'YYYY-MM-DD' or 'MM/DD/YYYY'
            if '-' in date_str and len(date_str.split('-')[0]) == 4: # YYYY-MM-DD
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            elif '/' in date_str and len(date_str.split('/')[2]) == 4: # MM/DD/YYYY
                date_obj = datetime.strptime(date_str, '%m/%d/%Y')
            elif '/' in date_str and len(date_str.split('/')[2]) == 2: # MM/DD/YY
                date_obj = datetime.strptime(date_str, '%m/%d/%y')
            else:
                # Fallback for other formats, or skip if unparseable
                print(f"Warning: Could not parse date format for '{date_str}'. Skipping row.")
                continue

            formatted_date = date_obj.strftime('%Y-%m-%d') # Standardize to YYYY-MM-DD
        except ValueError:
            print(f"Warning: Could not parse date '{date_str}'. Skipping row.")
            continue

        # Populate KCCI data
        kcci_entry = {'date': formatted_date}
        for key_suffix in ['Composite_Index', 'US_West_Coast', 'US_East_Coast', 'Europe',
                           'Mediterranean', 'Middle_East', 'Australia', 'South_America_East_Coast',
                           'South_America_West_Coast', 'South_Africa', 'West_Africa', 'China',
                           'Japan', 'Southeast_Asia']:
            col_name = f'KCCI_{key_suffix}'
            kcci_entry[key_suffix] = row.get(col_name)
        processed_data["KCCI"].append(kcci_entry)

        # Populate SCFI data
        scfi_entry = {'date': formatted_date}
        for key_suffix in ['Composite_Index_1', 'US_West_Coast_1', 'US_East_Coast_1', 'North_Europe',
                           'Mediterranean_1', 'Southeast_Asia_1', 'Middle_East_1',
                           'Australia_New_Zealand_SCFI', 'South_America_SCFI', 'Japan_West_Coast_SCFI',
                           'Japan_East_Coast_SCFI', 'Korea_SCFI', 'East_West_Africa_SCFI', 'South_Africa_SCFI']:
            scfi_entry[key_suffix] = row.get(key_suffix) # Assuming SCFI keys are direct
        processed_data["SCFI"].append(scfi_entry)

        # Populate WCI data
        wci_entry = {'date': formatted_date}
        for key_suffix in ['Composite_Index_2', 'Shanghai_Rotterdam_WCI', 'Rotterdam_Shanghai_WCI',
                           'Shanghai_Genoa_WCI', 'Shanghai_Los_Angeles_WCI', 'Los_Angeles_Shanghai_WCI',
                           'Shanghai_New_York_WCI', 'New_York_Rotterdam_WCI', 'Rotterdam_New_York_WCI']:
            wci_entry[key_suffix] = row.get(key_suffix)
        processed_data["WCI"].append(wci_entry)

        # Populate IACI data
        iaci_entry = {'date': formatted_date}
        iaci_entry['Composite_Index_3'] = row.get('IACI_Composite_Index')
        processed_data["IACI"].append(iaci_entry)

        # Populate FBX data
        fbx_entry = {'date': formatted_date}
        for key_suffix in ['China_EA_US_West_Coast_FBX', 'US_West_Coast_China_EA_FBX',
                           'China_EA_US_East_Coast_FBX', 'US_East_Coast_China_EA_FBX',
                           'China_EA_North_Europe_FBX', 'North_Europe_China_EA_FBX',
                           'China_EA_Mediterranean_FBX', 'Mediterranean_China_EA_FBX']:
            fbx_entry[key_suffix] = row.get(key_suffix)
        processed_data["FBX"].append(fbx_entry)

        # Populate XSI data
        xsi_entry = {'date': formatted_date}
        for key_suffix in ['East_Asia_North_Europe', 'North_Europe_East_Asia',
                           'East_Asia_US_West_Coast', 'US_West_Coast_East_Asia',
                           'East_Asia_South_America_East_Coast', 'North_Europe_US_East_Coast',
                           'US_East_Coast_North_Europe', 'North_Europe_South_America_East_Coast']:
            xsi_entry[key_suffix] = row.get(key_suffix)
        processed_data["XSI"].append(xsi_entry)

        # Populate MBCI data
        mbci_entry = {'date': formatted_date}
        mbci_entry['MBCI_Value'] = row.get('MBCI_Index')
        processed_data["MBCI"].append(mbci_entry)

        # Populate Blank Sailing data (assuming it's also time-series in 'Crawling_Data' or a dedicated sheet)
        # If Blank Sailing is in 'Crawling_Data2' as aggregated data, this needs adjustment.
        # For now, let's assume it's part of the main time-series.
        blank_sailing_entry = {'date': formatted_date}
        for key_suffix in ['Gemini_Cooperation_Blank_Sailing', 'MSC_Alliance_Blank_Sailing',
                           'OCEAN_Alliance_Blank_Sailing', 'Premier_Alliance_Blank_Sailing',
                           'Others_Independent_Blank_Sailing']:
            blank_sailing_entry[key_suffix] = row.get(key_suffix)
        processed_data["BLANK_SAILING"].append(blank_sailing_entry)

    return processed_data

def process_weather_data(raw_data):
    """Processes raw weather data."""
    weather_data = {"current": {}, "forecast": []}
    if not raw_data:
        return weather_data

    # Assuming the first row contains current weather and subsequent rows are forecast
    # This needs to be carefully mapped to your Google Sheet's column headers
    # for weather data.
    current_weather_row = raw_data[0] if raw_data else {}
    weather_data["current"] = {
        "LA_Temperature": current_weather_row.get('LA_Temperature'),
        "LA_WeatherStatus": current_weather_row.get('LA_WeatherStatus'),
        "LA_Humidity": current_weather_row.get('LA_Humidity'),
        "LA_WindSpeed": current_weather_row.get('LA_WindSpeed'),
        "LA_Pressure": current_weather_row.get('LA_Pressure'),
        "LA_Visibility": current_weather_row.get('LA_Visibility'),
        "LA_Sunrise": current_weather_row.get('LA_Sunrise'),
        "LA_Sunset": current_weather_row.get('LA_Sunset'),
    }

    # Assuming forecast data starts from the second row
    for row in raw_data[1:]:
        forecast_entry = {
            "date": row.get('Date'), # Or 'Forecast_Date'
            "min_temp": row.get('Min_Temp'),
            "max_temp": row.get('Max_Temp'),
            "status": row.get('Status') # Or 'Forecast_Status'
        }
        weather_data["forecast"].append(forecast_entry)

    return weather_data

def process_exchange_rates(raw_data):
    """Processes raw exchange rate data."""
    exchange_rates_data = []
    for row in raw_data:
        date_str = row.get('Date') # Assuming 'Date' column for exchange rates
        rate = row.get('Rate') # Assuming 'Rate' column for exchange rates
        if date_str and rate is not None:
            try:
                # Standardize date format for consistency
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                exchange_rates_data.append({
                    "date": date_obj.strftime('%Y-%m-%d'),
                    "rate": float(rate)
                })
            except ValueError:
                print(f"Warning: Could not parse exchange rate date '{date_str}' or rate '{rate}'. Skipping row.")
                continue
    return exchange_rates_data

def process_table_data(raw_data):
    """
    Processes data from Crawling_Data2 for the summary table.
    This function needs to be robust to the exact structure of Crawling_Data2.
    """
    table_data = {}

    # KCCI Table Data
    # Find the row containing "KCCIGroupIndexMainlaneMainlane" to start parsing KCCI
    kcci_start_row_idx = -1
    for i, row in enumerate(raw_data):
        # Check for a unique header that indicates the start of the KCCI table data
        # Based on the image, 'KCCIGroupIndexMainlaneMainlane' is in the first column of that section
        if row.get('KCCIGroupIndexMainlaneMainlane') == 'KCCIGroupIndexMainlaneMainlane':
            kcci_start_row_idx = i
            break

    if kcci_start_row_idx != -1:
        # These indices are relative to kcci_start_row_idx
        # The actual column headers from the sheet are crucial here.
        # I'm using placeholder column names based on the image provided.
        # You MUST verify these against the actual column names returned by gspread.
        kcci_headers_row = raw_data[kcci_start_row_idx + 1] # Row with "Code", "종합지수(Point)" etc.
        kcci_weight_row = raw_data[kcci_start_row_idx + 2] # Row with "Weight"
        kcci_current_row = raw_data[kcci_start_row_idx + 3] # Row with "Current Index (2025-07-21)"
        kcci_previous_row = raw_data[kcci_start_row_idx + 4] # Row with "Previous Index (2025-07-14)"
        kcci_weekly_change_row = raw_data[kcci_start_row_idx + 5] # Row with "Weekly Change"

        # Extract actual headers from the sheet's header row for KCCI
        # This assumes the headers are in a specific row and order.
        # Adjust these keys based on what gspread returns for `kcci_headers_row`.
        # For example, if '종합지수(Point)' is the actual key for 'Comprehensive Index' column.
        actual_kcci_cols = [
            'Code', '종합지수(Point)', 'USWC', 'USEC', 'Europe', 'Mediterranean',
            'Middle East', 'Australia', 'Latin America East Coast', 'Latin America West Coast',
            'South Africa', 'West Africa', 'China', 'Japan', 'South East Asia'
        ]

        kcci_table = {
            "headers": ["Category"] + [kcci_headers_row.get(col, col) for col in actual_kcci_cols[1:]],
            "rows": []
        }

        # Populate rows using the actual column names from the sheet
        kcci_table["rows"].append(["Weight"] + [kcci_weight_row.get(col) for col in actual_kcci_cols[1:]])
        kcci_table["rows"].append([kcci_current_row.get('Code')] + [kcci_current_row.get(col) for col in actual_kcci_cols[1:]])
        kcci_table["rows"].append([kcci_previous_row.get('Code')] + [kcci_previous_row.get(col) for col in actual_kcci_cols[1:]])
        kcci_table["rows"].append([kcci_weekly_change_row.get('Code')] + [kcci_weekly_change_row.get(col) for col in actual_kcci_cols[1:]])

        table_data["KCCI"] = kcci_table

    # SCFI Table Data
    scfi_start_row_idx = -1
    for i, row in enumerate(raw_data):
        if row.get('Description') == 'Description' and row.get('Weighting') == 'Weighting':
            scfi_start_row_idx = i
            break

    if scfi_start_row_idx != -1:
        scfi_headers_row = raw_data[scfi_start_row_idx] # Row with "Description", "Comprehensive Index"
        scfi_weighting_row = raw_data[scfi_start_row_idx + 1] # Row with "Weighting"
        scfi_current_row = raw_data[scfi_start_row_idx + 2] # Row with "Current Index (2025-07-18)"
        scfi_previous_row = raw_data[scfi_start_row_idx + 3] # Row with "Previous Index (2025-07-11)"
        scfi_compare_row = raw_data[scfi_start_row_idx + 4] # Row with "Compare With Last Week"

        actual_scfi_cols = [
            'Description', 'Comprehensive Index', 'Europe (Base port)', 'Mediterranean (Base port)',
            'USWC (Base port)', 'USEC (Base port)', 'Persian Gulf and Red Sea (Dubai)',
            'Australia/New Zealand (Melbourne)', 'East/West Africa (Lagos)', 'South Africa (Durban)',
            'West Japan (Base port)', 'East Japan (Base port)', 'Southeast Asia (Singapore)',
            'Korea (Pusan)', 'Central/South America West Coast(Manzanillo)'
        ]

        scfi_table = {
            "headers": ["Category"] + [scfi_headers_row.get(col, col) for col in actual_scfi_cols[1:]],
            "rows": []
        }

        scfi_table["rows"].append(["Weighting"] + [scfi_weighting_row.get(col) for col in actual_scfi_cols[1:]])
        scfi_table["rows"].append([scfi_current_row.get('Description')] + [scfi_current_row.get(col) for col in actual_scfi_cols[1:]])
        scfi_table["rows"].append([scfi_previous_row.get('Description')] + [scfi_previous_row.get(col) for col in actual_scfi_cols[1:]])
        scfi_table["rows"].append([scfi_compare_row.get('Description')] + [scfi_compare_row.get(col) for col in actual_scfi_cols[1:]])

        table_data["SCFI"] = scfi_table

    # SCFI2 Table Data
    scfi2_start_row_idx = -1
    for i, row in enumerate(raw_data):
        if row.get('SCFI2종합지수($/TEU)') == 'SCFI2종합지수($/TEU)':
            scfi2_start_row_idx = i
            break

    if scfi2_start_row_idx != -1:
        scfi2_headers_row = raw_data[scfi2_start_row_idx]
        scfi2_current_row = raw_data[scfi2_start_row_idx + 1]
        scfi2_previous_row = raw_data[scfi2_start_row_idx + 2]
        scfi2_compare_row = raw_data[scfi2_start_row_idx + 3]

        actual_scfi2_cols = [
            'SCFI2종합지수($/TEU)', 'USWC (Base port)', 'USEC (Base port)', 'Europe (Base port)',
            'Mediterranean (Base port)', 'Southeast Asia (Singapore)', 'Persian Gulf and Red Sea (Dubai)',
            'Australia/New Zealand (Melbourne)', 'South America (Santos)', 'West Japan (Base port)',
            'East Japan (Base port)', 'Korea (Pusan)', 'East/West Africa (Lagos)', 'South Africa (Durban)'
        ]

        scfi2_table = {
            "headers": ["Category"] + [scfi2_headers_row.get(col, col) for col in actual_scfi2_cols[1:]],
            "rows": []
        }

        scfi2_table["rows"].append([scfi2_current_row.get('Current Index (2025-07-18)')] + [scfi2_current_row.get(col) for col in actual_scfi2_cols[1:]])
        scfi2_table["rows"].append([scfi2_previous_row.get('Previous Index (2025-07-11)')] + [scfi2_previous_row.get(col) for col in actual_scfi2_cols[1:]])
        scfi2_table["rows"].append([scfi2_compare_row.get('Compare With Last Week')] + [scfi2_compare_row.get(col) for col in actual_scfi2_cols[1:]])

        table_data["SCFI2"] = scfi2_table


    # CCFI Table Data
    ccfi_start_row_idx = -1
    for i, row in enumerate(raw_data):
        if row.get('COMPOSITE INDEX') == 'COMPOSITE INDEX' and row.get('JAPAN') == 'JAPAN':
            ccfi_start_row_idx = i
            break
    if ccfi_start_row_idx != -1:
        ccfi_headers_row = raw_data[ccfi_start_row_idx]
        ccfi_current_row = raw_data[ccfi_start_row_idx + 1]
        ccfi_previous_row = raw_data[ccfi_start_row_idx + 2]
        ccfi_weekly_growth_row = raw_data[ccfi_start_row_idx + 3]

        actual_ccfi_cols = [
            'COMPOSITE INDEX', 'JAPAN', 'EUROPE', 'W/C AMERICA', 'E/C AMERIC',
            'KOREA', 'SOUTHEAST', 'MEDITERRANEAN', 'AUSTRALIA/NEW ZEALAND',
            'SOUTH AFRICA', 'SOUTH AMERICA', 'WEST EAST AFRICA', 'PERSIAN GULF/RED SEA'
        ]

        ccfi_table = {
            "headers": ["Category"] + [ccfi_headers_row.get(col, col) for col in actual_ccfi_cols],
            "rows": []
        }
        # The first column for rows is the actual value of the first header, not a label like "Current Index"
        ccfi_table["rows"].append([ccfi_current_row.get(col) for col in actual_ccfi_cols])
        ccfi_table["rows"].append([ccfi_previous_row.get(col) for col in actual_ccfi_cols])
        ccfi_table["rows"].append([ccfi_weekly_growth_row.get(col) for col in actual_ccfi_cols])

        table_data["CCFI"] = ccfi_table

    # WCI Table Data
    wci_table_start_row_idx = -1
    for i, row in enumerate(raw_data):
        if row.get('Composite Index') == 'Composite Index' and row.get('Shanghai-Rotterdam') == 'Shanghai-Rotterdam':
            wci_table_start_row_idx = i
            break
    if wci_table_start_row_idx != -1:
        wci_headers_row = raw_data[wci_table_start_row_idx]
        wci_current_row = raw_data[wci_table_start_row_idx + 1] # 17-Jul-25
        wci_weekly_row = raw_data[wci_table_start_row_idx + 2] # Weekly(%)
        wci_annual_row = raw_data[wci_table_start_row_idx + 3] # Annual(%)
        wci_previous_row = raw_data[wci_table_start_row_idx + 4] # 10-Jul-25

        actual_wci_cols = [
            'Composite Index', 'Shanghai-Rotterdam', 'Rotterdam-Shanghai',
            'Shanghai-Genoa', 'Shanghai-LosAngeles', 'LosAngeles-Shanghai',
            'Shanghai-NewYork', 'NewYork-Rotterdam', 'Rotterdam-NewYork'
        ]

        wci_table = {
            "headers": ["Category"] + [wci_headers_row.get(col, col) for col in actual_wci_cols],
            "rows": []
        }
        wci_table["rows"].append([wci_current_row.get(col) for col in actual_wci_cols])
        wci_table["rows"].append([wci_weekly_row.get('Weekly(%)')] + [wci_weekly_row.get(col) for col in actual_wci_cols[1:]])
        wci_table["rows"].append([wci_annual_row.get('Annual(%)')] + [wci_annual_row.get(col) for col in actual_wci_cols[1:]])
        wci_table["rows"].append([wci_previous_row.get('10-Jul-25')] + [wci_previous_row.get(col) for col in actual_wci_cols[1:]])
        table_data["WCI"] = wci_table

    # IACI Table Data
    iaci_table_start_row_idx = -1
    for i, row in enumerate(raw_data):
        if row.get('IACIdate') == 'IACIdate':
            iaci_table_start_row_idx = i
            break
    if iaci_table_start_row_idx != -1:
        iaci_headers_row = raw_data[iaci_table_start_row_idx]
        iaci_current_row = raw_data[iaci_table_start_row_idx + 1]
        iaci_previous_row = raw_data[iaci_table_start_row_idx + 2]

        actual_iaci_cols = ["IACIdate", "US$/40ft"]

        iaci_table = {
            "headers": ["Category"] + [iaci_headers_row.get(col, col) for col in actual_iaci_cols],
            "rows": []
        }
        iaci_table["rows"].append([iaci_current_row.get('IACIdate')] + [iaci_current_row.get('US$/40ft')])
        iaci_table["rows"].append([iaci_previous_row.get('IACIdate')] + [iaci_previous_row.get('US$/40ft')])
        table_data["IACI_Table"] = iaci_table

    # Blank Sailing Table
    blank_sailing_table_start_row_idx = -1
    for i, row in enumerate(raw_data):
        if row.get('Index') == 'Index' and row.get('Gemini Cooperation') == 'Gemini Cooperation':
            blank_sailing_table_start_row_idx = i
            break
    if blank_sailing_table_start_row_idx != -1:
        blank_sailing_headers_row = raw_data[blank_sailing_table_start_row_idx]
        blank_sailing_current_row = raw_data[blank_sailing_table_start_row_idx + 1]
        blank_sailing_previous_row = raw_data[blank_sailing_table_start_row_idx + 2]

        actual_blank_sailing_cols = [
            'Index', 'Gemini Cooperation', 'MSC', 'OCEAN Alliance', 'Premier Alliance', 'Others/Independent', 'Total'
        ]

        blank_sailing_table = {
            "headers": ["Category"] + [blank_sailing_headers_row.get(col, col) for col in actual_blank_sailing_cols],
            "rows": []
        }
        blank_sailing_table["rows"].append([blank_sailing_current_row.get('Index')] + [blank_sailing_current_row.get(col) for col in actual_blank_sailing_cols[1:]])
        blank_sailing_table["rows"].append([blank_sailing_previous_row.get('Index')] + [blank_sailing_previous_row.get(col) for col in actual_blank_sailing_cols[1:]])
        table_data["BLANK_SAILING_Table"] = blank_sailing_table

    # FBX Table Data
    fbx_table_start_row_idx = -1
    for i, row in enumerate(raw_data):
        if row.get('Global Container Freight Index') == 'Global Container Freight Index' and row.get('China/East Asia - North America West Coast') == 'China/East Asia - North America West Coast':
            fbx_table_start_row_idx = i
            break
    if fbx_table_start_row_idx != -1:
        fbx_headers_row = raw_data[fbx_table_start_row_idx]
        fbx_current_row = raw_data[fbx_table_start_row_idx + 1]
        fbx_previous_row = raw_data[fbx_table_start_row_idx + 2]

        actual_fbx_cols = [
            'Global Container Freight Index', 'China/East Asia - North America West Coast',
            'North America West Coast - China/East Asia', 'China/East Asia - North America East Coast',
            'North America East Coast - China/East Asia', 'China/East Asia - North Europe',
            'North Europe - China/East Asia', 'China/East Asia - Mediterranean',
            'Mediterranean - China/East Asia', 'North America East Coast - North Europe',
            'North Europe - North America East Coast', 'Europe - South America East Coast',
            'Europe - South America West Coast'
        ]

        fbx_table = {
            "headers": ["Date"] + [fbx_headers_row.get(col, col) for col in actual_fbx_cols],
            "rows": []
        }
        fbx_table["rows"].append([fbx_current_row.get('2025-07-18')] + [fbx_current_row.get(col) for col in actual_fbx_cols])
        fbx_table["rows"].append([fbx_previous_row.get('2025-07-11')] + [fbx_previous_row.get(col) for col in actual_fbx_cols])
        table_data["FBX"] = fbx_table

    # XSI Table Data
    xsi_table_start_row_idx = -1
    for i, row in enumerate(raw_data):
        if row.get('Far East - N. Europe') == 'Far East - N. Europe' and row.get('N. Europe - Far East') == 'N. Europe - Far East':
            xsi_table_start_row_idx = i
            break
    if xsi_table_start_row_idx != -1:
        xsi_headers_row = raw_data[xsi_table_start_row_idx]
        xsi_current_row = raw_data[xsi_table_start_row_idx + 1] # 07-22-2025
        xsi_weekly_row = raw_data[xsi_table_start_row_idx + 2] # WoW(%)
        xsi_monthly_row = raw_data[xsi_table_start_row_idx + 3] # MoM(%)

        actual_xsi_cols = [
            'Far East - N. Europe', 'N. Europe - Far East', 'Far East - USWC', 'USWC - Far East',
            'Far East - SAEC', 'N. Europe - USEC', 'USEC - N. Europe', 'N. Europe - SAEC'
        ]

        xsi_table = {
            "headers": ["Category"] + [xsi_headers_row.get(col, col) for col in actual_xsi_cols],
            "rows": []
        }
        xsi_table["rows"].append([xsi_current_row.get('07-22-2025')] + [xsi_current_row.get(col) for col in actual_xsi_cols])
        xsi_table["rows"].append([xsi_weekly_row.get('WoW(%)')] + [xsi_weekly_row.get(col) for col in actual_xsi_cols])
        xsi_table["rows"].append([xsi_monthly_row.get('MoM(%)')] + [xsi_monthly_row.get(col) for col in actual_xsi_cols])
        table_data["XSI"] = xsi_table

    # MBCI Table Data
    mbci_table_start_row_idx = -1
    for i, row in enumerate(raw_data):
        if row.get('Index(종합지수)') == 'Index(종합지수)':
            mbci_table_start_row_idx = i
            break
    if mbci_table_start_row_idx != -1:
        mbci_headers_row = raw_data[mbci_table_start_row_idx]
        mbci_current_row = raw_data[mbci_table_start_row_idx + 1] # Latest 2025-07-18
        mbci_previous_row = raw_data[mbci_table_start_row_idx + 2] # 2025-07-11

        actual_mbci_cols = ["Index(종합지수)", "$/day(정기용선, Time charter)"]

        mbci_table = {
            "headers": ["Category"] + [mbci_headers_row.get(col, col) for col in actual_mbci_cols],
            "rows": []
        }
        mbci_table["rows"].append([mbci_current_row.get('Latest')] + [mbci_current_row.get(col) for col in actual_mbci_cols])
        mbci_table["rows"].append([mbci_previous_row.get('2025-07-11')] + [mbci_previous_row.get(col) for col in actual_mbci_cols])
        table_data["MBCI_Table"] = mbci_table


    return table_data


def main():
    """Main function to fetch, process, and save all dashboard data."""
    all_data = {
        "chart_data": {},
        "weather_data": {},
        "exchange_rates": [],
        "table_data": {} # New key for table data
    }

    # Fetch data from 'Crawling_Data' for charts, weather, and exchange rates
    raw_crawling_data = fetch_data_from_sheet("Crawling_Data")
    if raw_crawling_data:
        all_data["chart_data"] = process_chart_data(raw_crawling_data)
        # Assuming weather and exchange rates are also in 'Crawling_Data' or separate dedicated sheets
        # If they are in 'Crawling_Data', you need to filter/extract them from raw_crawling_data.
        # For simplicity, let's assume they are structured in a way process_weather_data and process_exchange_rates can handle.
        # You might need to adjust these calls based on your actual sheet structure.
        all_data["weather_data"] = process_weather_data(raw_crawling_data) # Adjust if weather is in a different sheet
        all_data["exchange_rates"] = process_exchange_rates(raw_crawling_data) # Adjust if exchange rates are in a different sheet

    # Fetch data from 'Crawling_Data2' for the new tables
    raw_crawling_data2 = fetch_data_from_sheet("Crawling_Data2")
    if raw_crawling_data2:
        all_data["table_data"] = process_table_data(raw_crawling_data2)

    # Save all data to a single JSON file
    output_dir = "data"
    output_file = f"{output_dir}/crawling_data.json"
    try:
        # Create the 'data' directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
        print(f"Successfully saved all dashboard data to {output_file}")
    except Exception as e:
        print(f"Error saving data to JSON file: {e}")

if __name__ == "__main__":
    main()
