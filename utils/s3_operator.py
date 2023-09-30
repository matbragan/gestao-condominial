import io
import os

import boto3
import pandas as pd

AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def s3_writer(
        dataframe: pd.DataFrame,
        layer_name: str,
        file_name: str
    ) -> None:
    
    with io.StringIO() as csv_buffer:
        dataframe.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET, 
            Key=f'{layer_name}/{file_name}', 
            Body=csv_buffer.getvalue()
        )

        status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')

        if status == 200:
            print(f'Successful S3 put_object response. Status - {status}')
        else:
            print(f'Unsuccessful S3 put_object response. Status - {status}')

def s3_reader(
        layer_name: str,
        file_name: str
    ) -> pd.DataFrame:

    response = s3_client.get_object(
        Bucket=AWS_S3_BUCKET, 
        Key=f'{layer_name}/{file_name}'
    )

    status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')

    if status == 200:
        print(f'Successful S3 get_object response. Status - {status}')
        return pd.read_csv(response.get('Body'))
    else:
        print(f'Unsuccessful S3 get_object response. Status - {status}')
