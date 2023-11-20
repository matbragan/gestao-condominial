from utils.google_storage import storage_reader
from etl.operational.generic_financial import generic_writer

dataframe = storage_reader('extraction/despesas.csv')

generic_writer(dataframe, 'despesas')