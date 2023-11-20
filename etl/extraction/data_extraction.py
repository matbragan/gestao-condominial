import pandas as pd
import logging

from etl import TABLES

from utils.google_sheets import sheets_reader
from utils.google_storage import storage_writer

log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO)


def raw_extraction(table_name: str) -> None:

    try:
        dataframe = sheets_reader(table_name)
    except Exception as e:
        logging.error(f"Sheets reader Exception: {e}")
    # se a coluna do dataframe for um index automatico do pandas 
    # deve haver a reposição da coluna pela 1a linha do dataframe
    if type(dataframe.columns) == pd.RangeIndex:
        dataframe.columns = dataframe.iloc[0]
        dataframe = dataframe[1:].reset_index().drop('index', axis=1)
        logging.info(f"Table {table_name} - Automatic pandas index correction done!")

    try:
        storage_writer(dataframe, f'extraction/{table_name}.csv')
        logging.info(f"Table {table_name} - Google Storage writer Completed!")
    except Exception as e:
        logging.warning(f"Google Storage writer Fatal Error: {e}")


if __name__ == "__main__":
    for table in TABLES:
        raw_extraction(table)