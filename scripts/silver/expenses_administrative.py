from scripts.silver.expenses import expenses_silver
from utils.gcp_operator import gcs_writer

dataframe = expenses_silver(category='ADMINISTRATIVAS')
gcs_writer(dataframe, 'silver/expenses/administrative.csv')
