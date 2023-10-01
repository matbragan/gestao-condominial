import pandas as pd
from utils.gcp_operator import gcs_reader

expenses_bronze = gcs_reader('bronze/expenses.csv')

def _categories_dict(
        dataframe: pd.DataFrame = expenses_bronze
) -> dict:
    
    # ajustando a estrutura do DataFrame, removendo o cabeçalho e trazendo a 1a linha para as colunas
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index().drop('index', axis=1)

    # criando um dicionário para as categorias da planilha
    categories_indices = dataframe.index[dataframe['filter'] == '1'].tolist()
    categories_dict = dict()

    for index in enumerate(categories_indices):
        next_index = dataframe.index[-1] if index[0] == len(categories_indices)-1 else categories_indices[index[0]+1]
        category = dataframe.iloc[index[1]]['Mês']
        categories_dict[category] = dataframe.iloc[index[1]+1:next_index]['Mês'].tolist()

    return categories_dict

def _expenses_silver(
        dataframe: pd.DataFrame = expenses_bronze
) -> pd.DataFrame:
    
    # ajustando a estrutura do DataFrame, removendo o cabeçalho e trazendo a 1a linha para as colunas
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index().drop('index', axis=1)

    # removendo as categorias do DataFrame
    dataframe = dataframe[dataframe['filter'] != '1']
    dataframe = dataframe.drop('filter', axis=1)

    # ajustando a estrutura do DataFrame
    dataframe = dataframe.transpose().reset_index()
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index().drop('index', axis=1)

    # ajustando as colunas de valores de gastos
    for column in dataframe.columns:
        if column != 'Mês':
            dataframe[column] = dataframe[column].str.strip().str.replace(r'^R\$', '', regex=True)
            dataframe[column] = dataframe[column].str.replace(r'\s+', '', regex=True)
            dataframe[column] = dataframe[column].str.replace(r'\.', '', regex=True)
            dataframe[column] = dataframe[column].str.replace(r',', '.', regex=True)
            dataframe[column] = dataframe[column].astype(float)
    
    # ajustando a coluna mês
    dataframe['Mês'] = pd.to_datetime(dataframe['Mês'])

    return dataframe
