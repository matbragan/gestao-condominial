import pandas as pd
import re

from utils.google_storage import storage_reader

df = storage_reader('bronze/moradores.csv')

buildings = [col for col in df.columns if col.startswith('EDIFÍCIO')]
buildings

build_index = 0
for col_name in buildings:
    treat_name = col_name.replace('EDIFÍCIO ', '').title()
    df[col_name] = treat_name
    
    new_col_name = 'edificio.' + str(build_index) if build_index > 0 else 'edificio'
    df.rename(columns={col_name: new_col_name}, inplace=True)

    build_index += 1

reshaped_df = pd.DataFrame()

index_cols = ['Apartamento']
unique_cols = [col.split('.')[0] for col in df.columns]
unique_cols = list(list(dict.fromkeys(unique_cols)))
unique_cols = list(filter(lambda x: x not in index_cols, unique_cols))

for i in range(build_index):
    treat_df = df[index_cols + unique_cols].rename(columns={col: re.sub(r'\.\d+$', '', col) for col in unique_cols})
    reshaped_df = pd.concat([reshaped_df, treat_df], ignore_index=True)
    unique_cols = list(map(lambda x: x + '.' + str(i+1), unique_cols))

reshaped_df