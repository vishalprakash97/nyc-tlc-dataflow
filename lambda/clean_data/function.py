import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os

s3=boto3.client("s3")
bucket_name=os.environ['bucket_name']

def read_file(bucket_name,object_key):
    parquet_file=pq.ParquetFile(f"s3://{bucket_name}/{object_key}")
    return parquet_file

def transform_data(df):
    # remove null values and rows with zero passengers
    #print(f"Before Cleaning | Shape: {df.shape}")      
    df_cleaned=df.dropna()
    df_cleaned=df_cleaned.loc[df['passenger_count']!= 0]
    df_cleaned['tpep_pickup_date'] = df_cleaned['tpep_pickup_datetime'].dt.date
    #print(f"After Cleaning | Shape: {df_cleaned.shape}")      
    return df_cleaned

def write_to_s3(df, bucket_name, base_object_key):
    # Convert the dataframe to a pyarrow table
    table = pa.Table.from_pandas(df)    
    s3_path = f"s3://{bucket_name}/{base_object_key}"
    
    # Use write_to_dataset to write the partitioned data to S3
    pq.write_to_dataset(table, root_path=s3_path, partition_cols=['tpep_pickup_date'])
    return None

def lambda_handler(event, context):
    object_key=event['object_key']
    
    output_object_key= os.path.splitext(object_key)[0]
    output_object_key=output_object_key.replace('raw','cleaned')

    parquet_file=read_file(bucket_name,object_key)
    old,new=0,0
    for batch in parquet_file.iter_batches(batch_size=100000):
        batch_df = batch.to_pandas()
        old+=len(batch_df)
        transformed_df = transform_data(batch_df)
        new+=len(transformed_df)
        write_to_s3(transformed_df, bucket_name, output_object_key)
    
    print(f"Before Cleaning | Rows: {old}")  
    print(f"After Cleaning | Rows: {new}") 
        
    return {
        'status': 200,
        'message': "Success. Files Uploaded to S3",
        'bucket_name': bucket_name,
        'path': output_object_key
    }
