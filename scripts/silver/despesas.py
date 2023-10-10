import json
import unicodedata
import pandas as pd

from utils.google_storage import reader
from utils.google_storage import writer

despesas_bronze = reader('bronze/despesas.csv')


def categories_dict(
        dataframe: pd.DataFrame = despesas_bronze
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


def snake_case(input_str: str):
    
    # remover acentos
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    input_str = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

    # caixa baixa
    input_str = input_str.lower()

    # snake case
    str_to_snake = [ ' (', ' - ', ' ', '(', '/', '-', "'", '.' ]
    for str in str_to_snake:
        input_str = input_str.replace(str, '_')
    input_str = input_str.replace(')', '')

    return input_str


def expenses_silver(
        dataframe: pd.DataFrame = despesas_bronze,
        category: str = None # type: ignore
) -> pd.DataFrame:
    
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
        dataframe = dataframe[dataframe['descricao'].isin(categories_dict()[category])]

    # resetando indices do dataframe
    dataframe = dataframe.reset_index().drop('index', axis=1)

    # renomeando as descrições
    dataframe['descricao'] = dataframe['descricao'].apply(snake_case)

    return dataframe


def expenses_silver_writer(
        file_name: str,
        category: str,
        dataframe: pd.DataFrame = despesas_bronze
) -> None:
    
    writing_dataframe = expenses_silver(dataframe, category)
    writer(writing_dataframe, f'silver/despesas/{file_name}.csv')
    

if __name__ == '__main__':
    file_name_dict = dict()
    categories = categories_dict()
    for category in categories:
        file_name_dict[category] = snake_case(category)

    print('dicionário de categorias encontrado: \n', json.dumps(file_name_dict, indent=4, ensure_ascii=False))
    print('\n')

    for category in file_name_dict:
        file_name = file_name_dict[category]
        expenses_silver_writer(file_name=file_name, category=category)
        print(f'Arquivo {file_name} escrito! \n')
