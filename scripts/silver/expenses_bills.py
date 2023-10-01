from scripts.silver.expenses import expenses_silver
from utils.gcp_operator import gcs_writer

dataframe = expenses_silver(category='BOLETO DO CONDOMÍNIO')
gcs_writer(dataframe, 'silver/expenses/bills.csv')
