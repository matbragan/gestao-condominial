import pandas as pd
from utils.google_sheets import sheets_reader
from utils.google_storage import storage_writer

def generic_writer(table_name: str) -> None:

    dataframe = sheets_reader(table_name)
    # se a coluna do dataframe for um index automatico do pandas,
    # deve haver a reposição da coluna pela 1a linha do dataframe
    if type(dataframe.columns) == pd.RangeIndex:
        dataframe.columns = dataframe.iloc[0]
        dataframe = dataframe[1:].reset_index().drop('index', axis=1)

    storage_writer(dataframe, f'bronze/{table_name}.csv')

if __name__ == '__main__':
    bronze_tables = [
        'despesas',
        'receita',
        'funcionarios',
        'moradores',
        'periodicos'
    ]

    for table in bronze_tables:
        generic_writer(table)