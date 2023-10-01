from utils.sheet_operator import expenses_sheet
from utils.gcp_operator import gcs_writer

dataframe = expenses_sheet()
gcs_writer(dataframe, 'bronze/expenses.csv')