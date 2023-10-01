from scripts.silver.expenses import expenses_silver
from utils.gcp_operator import gcs_writer

dataframe = expenses_silver(category='MANUTENÇÃO E CONSERVAÇÃO')
gcs_writer(dataframe, 'silver/expenses/maintenance.csv')
