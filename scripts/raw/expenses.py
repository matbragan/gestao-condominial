from utils.s3_operator import s3_writer
from utils.sheet_operator import expenses_sheet

dataframe = expenses_sheet()
s3_writer(dataframe, 'raw', 'expenses.csv')