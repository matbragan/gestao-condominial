import os
import io
import pandas as pd
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS']='gcp_key.json'

def gcs_writer(
        dataframe: pd.DataFrame,
        bucket_name: str,
        blob_path: str
) -> None:
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(dataframe.to_csv(index=False), 'text/csv')

def gcs_reader(
        bucket_name: str,
        blob_path: str
) -> pd.DataFrame:
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path)
    csv_string = blob.download_as_string()
    return pd.read_csv(io.StringIO(csv_string.decode('utf-8')))
