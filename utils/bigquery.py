import os 
import logging
from google.cloud import bigquery

from utils.google_storage import list_files
from utils import GOOGLE_CREDENTIALS, PROJECT_ID, BUCKET_NAME
from etl import TABLES

log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO)

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

    for file in bucket_files:
        split_file = file.split('/')
        dataset = split_file[0]
        table = split_file[-1].replace('.csv', '')
        
        if dataset == layer and table in TABLES:
            uri = f'gs://{bucket_name}/{file}'
            table_id = f'{project_id}.{dataset}.{table}'
            
            try:
                load_job = client.load_table_from_uri(
                    uri, table_id, job_config=job_config
                )

                load_job.result()

                destination_table = client.get_table(table_id)
                logging.info(f'Load to BigQuery {dataset}.{table}, with {destination_table.num_rows} rows.')
            except Exception as e:
                logging.warning(f'Exception to Load to BigQuery {dataset}.{table}: {e}')


def run_sql_file_query(
        file_path: str
) -> None:
    client = bigquery.Client()

    with open(file_path, "r") as file:
        query = file.read()

    try:
        query_job = client.query(query)
        query_job.result()
        logging.info(f'Successful Query {file_path} in BigQuery.')
    except Exception as e:
        logging.warning(f'Exception running Query {file_path} in BigQuery: {e}')
