import pandas as pd

def sheet_reader(
        sheet_id: str,
        page_name: str
    ) -> pd.DataFrame:
    
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={page_name}'
    return pd.read_csv(url)

def expenses_sheet() -> pd.DataFrame:
    sheet_id = '1V3aPdPWMJHLa_Gnra_GJc6pxAWCcaLbQjKyfhVtN2rU'
    page_name = 'expenses_template'
    return sheet_reader(sheet_id=sheet_id, page_name=page_name)
