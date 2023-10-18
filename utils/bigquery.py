import os 
from google.cloud import bigquery

from utils.google_storage import list_files
from utils import GOOGLE_CREDENTIALS, PROJECT_ID, BUCKET_NAME

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_CREDENTIALS


def load_tables(
        layer: str,
        project_id: str = PROJECT_ID,
        bucket_name: str = BUCKET_NAME
) -> None:
    
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition='WRITE_TRUNCATE'
    )

    bucket_files = list_files(bucket_name)
    
    layer_files = []

    for file in bucket_files:
        split_file = file.split('/')
        if split_file[0] == layer and split_file[2].endswith('.csv'):
            layer_files.append(file)
        
    for file in layer_files:
        uri = f'gs://{bucket_name}/{file}'

        split_file = file.split('/')
        schema = split_file[1]
        table = split_file[2].replace('.csv', '')
        table_id = f'{project_id}.{schema}.{table}'

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        load_job.result()

        destination_table = client.get_table(table_id)
        print(f'Tabela {schema}.{table} carregada para BigQuery, com {destination_table.num_rows} linhas.')
