from scripts.silver.expenses import expenses_silver
from utils.gcp_operator import gcs_writer

dataframe = expenses_silver(category='FUNCIONÁRIOS')
gcs_writer(dataframe, 'silver/expenses/employees.csv')
