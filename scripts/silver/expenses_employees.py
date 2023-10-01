import pandas as pd
from scripts.silver.expenses import _expenses_silver, _categories_dict
from utils.gcp_operator import gcs_writer

def _expenses_employees(
        dataframe: pd.DataFrame
) -> pd.DataFrame:
    employees_category = 'DESPESAS COM PESSOAL'
    
    categories_dict = _categories_dict()

    columns = categories_dict[employees_category]
    columns.insert(0, 'Mês')

    # criando dicionário para renomear colunas
    rename_columns = {
        'Mês': 'mes',
        'Total com Salários': 'total_salarios',
        'Total com Adiantamento salarial (sempre dia 20)': 'total_adiantamento_salarial',
        'Exame médico periódico': 'exame_medico_periodico',
        'Férias Vitamar': 'ferias_vitamar',
        'Férias Daniel': 'ferias_daniel',
        'Férias Fábio': 'ferias_fabio',
        'Férias Jailza': 'ferias_jailza',
        'Férias Joana': 'ferias_joana',
        'Férias Karla': 'ferias_karla',
        'Total com Décimo terceiro salário (1 parc - setembro)': 'decimo_terceiro_parcela_1',
        'Total com Décimo terceiro salário (2 parc - dezembro)': 'decimo_terceiro_parcela_2',
        'Ticket alimentação': 'ticket_alimentacao',
        'INSS': 'inss',
        'FGTS': 'fgts',
        'Pis sobre folha de pagamento': 'pis_folha_pagamento',
        'Vale Transporte': 'vale_transporte',
        'Seguro de vida dos funcionários': 'seguro_vida',
        'Uniformes': 'uniformes'
    }

    # renomeando colunas
    dataframe = dataframe.rename(columns=rename_columns)

    return dataframe

if __name__ == '__main__':
    expenses_silver = _expenses_silver()
    expenses_employees = _expenses_employees(expenses_silver)
    gcs_writer(expenses_employees, 'silver/expenses/employees.csv')

