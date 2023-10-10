import pandas as pd
from utils.google_sheets import reader
from utils.google_storage import writer

dataframe = reader('despesas')
# se a coluna do dataframe for um index automatico do pandas,
# deve haver a reposição da coluna para a 1a linha do dataframe
if type(dataframe.columns) == pd.RangeIndex:
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index().drop('index', axis=1)

writer(dataframe, 'bronze/despesas.csv')