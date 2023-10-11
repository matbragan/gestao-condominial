import os
import io
import pandas as pd
from google.cloud import storage

from utils import GOOGLE_CREDENTIALS, BUCKET_NAME

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_CREDENTIALS
bucket_name = BUCKET_NAME

def storage_writer(
        dataframe: pd.DataFrame,
        blob_path: str,
        bucket_name: str = bucket_name
) -> None:
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(dataframe.to_csv(index=False), 'text/csv')

def storage_reader(
        blob_path: str,
        bucket_name: str = bucket_name
) -> pd.DataFrame:
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path)
    csv_string = blob.download_as_string()
    return pd.read_csv(io.StringIO(csv_string.decode('utf-8')))

def list_files(
        bucket_name: str = bucket_name
) -> list:
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    
    files = []
    for blob in blobs:
        files.append(blob.name)
    
    return files
