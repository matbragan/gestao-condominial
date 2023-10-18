import json
import unicodedata
import pandas as pd

from utils.google_storage import storage_writer


def categories_dict(
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
        input_dataframe: pd.DataFrame,
        category: str = None
) -> pd.DataFrame:
    
    dataframe = input_dataframe.copy()
    
    # ajustando a estrutura do DataFrame, removendo o cabeçalho e trazendo a 1a linha para as colunas
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index().drop('index', axis=1)

    # removendo as categorias do DataFrame
    dataframe = dataframe[dataframe['filter'] != '1']
    dataframe = dataframe.drop('filter', axis=1)

    # ajustando a estrutura do DataFrame, criando 3 colunas - descricao, mes, valor
    dataframe = dataframe.melt(id_vars='Mês', var_name='mes', value_name='valor')
    dataframe.rename(columns={'Mês': 'descricao'}, inplace=True)

    # ajustando os valores das colunas valor e mes
    dataframe['valor'] = dataframe['valor'].str.strip().str.replace(r'^R\$', '', regex=True)
    dataframe['valor'] = dataframe['valor'].str.replace(r'\s+', '', regex=True)
    dataframe['valor'] = dataframe['valor'].str.replace(r'\.', '', regex=True)
    dataframe['valor'] = dataframe['valor'].str.replace(r',', '.', regex=True)
    dataframe['valor'] = dataframe['valor'].astype(float)
    dataframe['mes'] = pd.to_datetime(dataframe['mes'], format='%m-%Y')

    # filtrando dataframe pela categoria
    if category:
        dataframe = dataframe[dataframe['descricao'].isin(categories_dict(input_dataframe)[category])]

    # resetando indices do dataframe
    dataframe = dataframe.reset_index().drop('index', axis=1)

    # renomeando as descrições
    dataframe['descricao'] = dataframe['descricao'].apply(snake_case)

    return dataframe


def generic_partial_writer(
        input_dataframe: pd.DataFrame,
        category: str,
        schema_name: str,
        table_name: str,
) -> None:
    
    dataframe = generic_treatment(input_dataframe, category)
    storage_writer(dataframe, f'silver/{schema_name}/{table_name}.csv')


def generic_writer(
        input_dataframe: pd.DataFrame,
        schema_name: str
) -> None:
    
    table_name_dict = dict()
    categories = categories_dict(input_dataframe)
    for category in categories:
        table_name_dict[category] = snake_case(category)

    print('Dicionário de categorias encontrado: \n', json.dumps(table_name_dict, indent=4, ensure_ascii=False))
    print('\n')

    for category in table_name_dict:
        table_name = table_name_dict[category]
        generic_partial_writer(input_dataframe, category, schema_name, table_name)
        print(f'Tabela {schema_name}.{table_name} escrita no Google Storage! \n')
