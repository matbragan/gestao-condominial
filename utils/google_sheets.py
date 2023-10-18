import gspread
import pandas as pd

from utils import GOOGLE_CREDENTIALS, SHEET_ID

def sheets_reader(
        sheet_name: str,
        sheet_id: str = SHEET_ID
    ) -> pd.DataFrame:
    
    gc = gspread.service_account(GOOGLE_CREDENTIALS)
    spreadsheet = gc.open_by_key(sheet_id)
    worksheet = spreadsheet.worksheet(sheet_name)
    return pd.DataFrame(worksheet.get())
