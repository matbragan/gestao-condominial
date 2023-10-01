from scripts.silver.expenses import expenses_silver
from utils.gcp_operator import gcs_writer

dataframe = expenses_silver(category='MENSAIS FIXO')
gcs_writer(dataframe, 'silver/expenses/fixed.csv')
