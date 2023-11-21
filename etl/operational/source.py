import unicodedata
from typing import Callable

from utils.google_storage import storage_reader, storage_writer


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


def operational_writer(
        table_name: str,
        treatment_func: Callable
) -> None:
    
    input_dataframe = storage_reader(f'extraction/{table_name}.csv')
    dataframe = treatment_func(input_dataframe)
    storage_writer(dataframe, f'operational/{table_name}.csv')
