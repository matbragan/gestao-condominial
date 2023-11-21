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
    categories = categories_dict(input_dataframe)
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


def generic_writer(
        input_dataframe: pd.DataFrame,
        table_name: str,
) -> None:
    
    dataframe = generic_treatment(input_dataframe)
    storage_writer(dataframe, f'operational/{table_name}.csv')
