from utils.google_storage import storage_reader
from scripts.silver.generic_financial_writer import generic_writer

dataframe = storage_reader('bronze/receita.csv')

generic_writer(dataframe, 'receita')