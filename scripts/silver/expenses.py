import pandas as pd
import unicodedata
from utils.gcp_operator import gcs_reader

expenses_bronze = gcs_reader('bronze/expenses.csv')

def categories_dict(
        dataframe: pd.DataFrame = expenses_bronze
) -> dict:
    
    # ajustando a estrutura do DataFrame, removendo o cabeçalho e trazendo a 1a linha para as colunas
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index().drop('index', axis=1)

    # criando um dicionário para as categorias da planilha
    categories_indices = dataframe.index[dataframe['filter'] == '1'].tolist()
    categories_dict = dict()

    # populando dicionário com os gastos de cada categoria
    for index in enumerate(categories_indices):
        next_index = dataframe.index[-1] if index[0] == len(categories_indices)-1 else categories_indices[index[0]+1]
        category = dataframe.iloc[index[1]]['Mês']
        categories_dict[category] = dataframe.iloc[index[1]+1:next_index]['Mês'].tolist()

    return categories_dict

def rename_columns(
        dataframe: pd.DataFrame
) -> pd.DataFrame:
    
    def remove_accents(input_str):
        nfkd_form = unicodedata.normalize('NFKD', input_str)
        return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

    # remover acentos
    dataframe.columns = [remove_accents(col) for col in dataframe.columns]

    # caixa baixa
    dataframe.columns = [col.lower() for col in dataframe.columns]

    # snake case
    str_to_snake = [ ' (', ' - ', ' ', '(', '/', '-', "'", '.' ]
    for str in str_to_snake:
        dataframe.columns = [col.replace(str, '_') for col in dataframe.columns]
    dataframe.columns = [col.replace(')', '') for col in dataframe.columns]
    
    return dataframe

def expenses_silver(
        dataframe: pd.DataFrame = expenses_bronze,
        category: str = None
) -> pd.DataFrame:
    
    # ajustando a estrutura do DataFrame, removendo o cabeçalho e trazendo a 1a linha para as colunas
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index().drop('index', axis=1)

    # removendo as categorias do DataFrame
    dataframe = dataframe[dataframe['filter'] != '1']
    dataframe = dataframe.drop('filter', axis=1)

    # ajustando a estrutura do DataFrame, transpondo-o, transformando os valores da 1a colunas nas colunas
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

    # filtrando dataframe pela categoria
    if category:
        columns = categories_dict()[category]
        columns.insert(0, 'Mês')
        dataframe = dataframe[columns]

    # renomeando colunas
    dataframe = rename_columns(dataframe)

    return dataframe
