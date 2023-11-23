import logging
import unicodedata
from typing import Callable
import pandas as pd

from utils.google_storage import storage_reader, storage_writer

log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO)


def operational_writer(
        table_name: str,
        treatment_func: Callable
) -> None:
    
    try:
        input_dataframe = storage_reader(f'extraction/{table_name}.csv')
        dataframe = treatment_func(input_dataframe)
        storage_writer(dataframe, f'operational/{table_name}.csv')
        logging.info(f"Operational writer {table_name} Completed!")
    except Exception as e:
        logging.warning(f"Operational writer {table_name} Fatal Error: {e}")


def snake_case(
        input_str: str
) -> str:
    
    # remover acentos
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    input_str = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

    # caixa baixa
    input_str = input_str.lower()

    # snake case
    str_to_snake = [ ' (', ' - ', ' + ', ' ', '(', '/', '-', "'", '.' ]
    for str in str_to_snake:
        input_str = input_str.replace(str, '_')
    input_str = input_str.replace(')', '')

    return input_str


def generic_treatment(
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

    # mapeando as colunas do dataframe e tratando-as conforme necessidade
    for column in dataframe.columns:
        if "unidade" in column:
            # preenchendo unidades vazias
            dataframe[column] = dataframe[column].ffill()

            # separando a coluna unidade em edificio e apartamento
            dataframe[['apartamento', 'edificio']] = dataframe[column].str.extract(r'(\d+) (.+)')

            # reordenando as colunas
            dataframe = pd.concat([dataframe[['edificio', 'apartamento']], dataframe.drop(['edificio', 'apartamento', column], axis=1)], axis=1)

        if "valor" in column:
            # ajustando os valores das colunas valor
            dataframe[column] = dataframe[column].str.strip().str.replace(r'^R\$', '', regex=True)
            dataframe[column] = dataframe[column].str.replace(r'\s+', '', regex=True)
            dataframe[column] = dataframe[column].str.replace(r'\.', '', regex=True)
            dataframe[column] = dataframe[column].str.replace(r',', '.', regex=True)
            dataframe[column] = dataframe[column].astype(float)

    return dataframe


def financial_categories_dict(
        input_dataframe: pd.DataFrame
) -> dict:
    
    dataframe = input_dataframe.copy()
    
    # ajustando a estrutura do DataFrame, removendo o cabeçalho e trazendo a 1a linha para as colunas
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index().drop('index', axis=1)

    # criando um dicionário para as categorias da planilha
    categories_indices = dataframe.index[dataframe['filter'] == '1'].tolist()
    categories_dict = dict()

    # populando dicionário com os gastos de cada categoria
    for index in enumerate(categories_indices):
        next_index = dataframe.index[-1]+1 if index[0] == len(categories_indices)-1 else categories_indices[index[0]+1]
        category = dataframe.iloc[index[1]]['Mês']
        categories_dict[category] = dataframe.iloc[index[1]+1:next_index]['Mês'].tolist()

    return categories_dict


def financial_generic_treatment(
        input_dataframe: pd.DataFrame
) -> pd.DataFrame:
    
    dataframe = input_dataframe.copy()
    
    # ajustando a estrutura do DataFrame, removendo o cabeçalho e trazendo a 1a linha para as colunas
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index().drop('index', axis=1)

    # removendo as categorias do DataFrame
    dataframe = dataframe[dataframe['filter'] != '1']
    dataframe = dataframe.drop('filter', axis=1)

    # ajustando a estrutura do DataFrame, criando 3 colunas - subcategoria, mes, valor
    dataframe = dataframe.melt(id_vars='Mês', var_name='mes', value_name='valor')
    dataframe.rename(columns={'Mês': 'subcategoria'}, inplace=True)

    # ajustando os valores das colunas valor e mes
    dataframe['valor'] = dataframe['valor'].str.strip().str.replace(r'^R\$', '', regex=True)
    dataframe['valor'] = dataframe['valor'].str.replace(r'\s+', '', regex=True)
    dataframe['valor'] = dataframe['valor'].str.replace(r'\.', '', regex=True)
    dataframe['valor'] = dataframe['valor'].str.replace(r',', '.', regex=True)
    dataframe['valor'] = dataframe['valor'].astype(float)
    dataframe['mes'] = pd.to_datetime(dataframe['mes'], format='%m-%Y')

    # acrescentando a coluna categoria no dataframe
    categories = financial_categories_dict(input_dataframe)
    def map_category(value):
        for category, items in categories.items():
            if value in items:
                return category
        return None
            
    dataframe['categoria'] = dataframe['subcategoria'].apply(map_category)

    # resetando indices do dataframe
    dataframe = dataframe.reset_index().drop('index', axis=1)

    # renomeando as categorias e subcategorias
    dataframe['categoria'] = dataframe['categoria'].apply(snake_case)
    dataframe['subcategoria'] = dataframe['subcategoria'].apply(snake_case)

    # reordenando as colunas
    dataframe = dataframe[['categoria', 'subcategoria', 'mes', 'valor']]

    return dataframe
