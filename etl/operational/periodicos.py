import pandas as pd

from etl.operational.source import snake_case, operational_writer


def treatment(
        input_dataframe: pd.DataFrame
) -> pd.DataFrame:
    
    dataframe = input_dataframe.copy()

    # ajustando a estrutura do DataFrame, removendo o cabeçalho e trazendo a 1a linha para as colunas
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index().drop('index', axis=1)

    # renomeando as colunas em snake_case
    for column in dataframe.columns:
        new_column = snake_case(column)
        dataframe = dataframe.rename(columns={column: new_column})

    return dataframe


operational_writer('periodicos', treatment)
