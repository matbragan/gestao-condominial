import gspread
import pandas as pd

SHEET_ID = '1CYZR2Oa_7uhvMWP65WlkvpoUUwQkLT0UV3cUhSvszGg'

def sheets_reader(
        sheet_name: str,
        sheet_id: str = SHEET_ID
    ) -> pd.DataFrame:
    
    gc = gspread.service_account('gcp_key.json') # type: ignore
    spreadsheet = gc.open_by_key(sheet_id)
    worksheet = spreadsheet.worksheet(sheet_name)
    return pd.DataFrame(worksheet.get())
