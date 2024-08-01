import boto3
import pandas as pd
import pyarrow
import os

s3=boto3.client("s3")
bucket_name=os.environ['bucket_name']

def read_file(bucket_name,object_key):
    df=pd.read_parquet(path=f"s3://{bucket_name}/{object_key}")
    return df

def transform_data(df):
    # remove null values and rows with zero passengers
    print(f"Before Cleaning | Shape: {df.shape}")      
    df_cleaned=df.dropna()
    df_cleaned=df_cleaned.loc[df['passenger_count']!= 0]
    print(f"After Cleaning | Shape: {df_cleaned.shape}")      
    return df

def write_to_s3(df,bucket_name,object_key):
    cleaned_object_key = object_key.replace('raw', 'cleaned')
    df.to_parquet('/tmp/cleaned.parquet')
    s3.upload_file('/tmp/cleaned.parquet', bucket_name, cleaned_object_key)

    print(f"File Uploaded to {bucket_name}/{cleaned_object_key}")
    return cleaned_object_key

def lambda_handler(event, context):
    object_key = event['object_key']
    
    df=read_file(bucket_name,object_key)
    df_cleaned=transform_data(df)
    cleaned_object_key=write_to_s3(df_cleaned,bucket_name,object_key)
        
    return {
        'status': 200,
        'message': "Success. File Uploaded to S3",
        'bucket_name': bucket_name,
        'object_key': cleaned_object_key
    }
