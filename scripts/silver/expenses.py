import pandas as pd
import unicodedata
from utils.gcp_operator import gcs_reader
from utils.gcp_operator import gcs_writer

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


def rename_description(
        dataframe: pd.DataFrame
) -> pd.DataFrame:
    
    def remove_accents(input_str):
        nfkd_form = unicodedata.normalize('NFKD', input_str)
        return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

    # remover acentos
    dataframe['description'] = dataframe['description'].apply(remove_accents)

    # caixa baixa
    dataframe['description'] = dataframe['description'].str.lower()

    # snake case
    str_to_snake = [ ' (', ' - ', ' ', '(', '/', '-', "'", '.' ]
    for str in str_to_snake:
        dataframe['description'] = dataframe['description'].str.replace(str, '_')
    dataframe['description'] = dataframe['description'].str.replace(')', '')
    
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

    # ajustando a estrutura do DataFrame, criando 3 colunas - description, month, value
    dataframe = dataframe.melt(id_vars='Mês', var_name='month', value_name='value')
    dataframe.rename(columns={'Mês': 'description'}, inplace=True)

    # ajustando os valores das colunas value e month
    dataframe['value'] = dataframe['value'].str.strip().str.replace(r'^R\$', '', regex=True)
    dataframe['value'] = dataframe['value'].str.replace(r'\s+', '', regex=True)
    dataframe['value'] = dataframe['value'].str.replace(r'\.', '', regex=True)
    dataframe['value'] = dataframe['value'].str.replace(r',', '.', regex=True)
    dataframe['value'] = dataframe['value'].astype(float)
    dataframe['month'] = pd.to_datetime(dataframe['month'])

    # filtrando dataframe pela categoria
    if category:
        dataframe = dataframe[dataframe['description'].isin(categories_dict()[category])]

    # resetando indices do dataframe
    dataframe = dataframe.reset_index().drop('index', axis=1)

    # renomeando colunas
    dataframe = rename_description(dataframe)

    return dataframe


def expenses_silver_writer(
        file_name: str,
        category: str,
        dataframe: pd.DataFrame = expenses_bronze
) -> None:
    
    writing_dataframe = expenses_silver(dataframe, category)
    gcs_writer(writing_dataframe, f'silver/expenses/{file_name}.csv')
    

if __name__ == '__main__':
    file_name_dict = {
        'FUNCIONÁRIOS': 'employees',
        'BOLETO DO CONDOMÍNIO': 'bills',
        'MENSAIS FIXO': 'fixed',
        'MANUTENÇÃO E CONSERVAÇÃO': 'maintenance',
        'DIVERSAS': 'several',
        'ADMINISTRATIVAS': 'administrative'
    }

    for category in file_name_dict:
        file_name = file_name_dict[category]
        expenses_silver_writer(file_name=file_name, category=category)
        print(f'{category} write! \n')
