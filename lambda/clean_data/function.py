import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os

s3=boto3.client("s3")
bucket_name=os.environ['bucket_name']

taxi_dtypes = {
    'VendorID': 'int64',
    'passenger_count': 'int64',
    'trip_distance': float,
    'RatecodeID': 'int64',
    'store_and_fwd_flag':str,
    'PULocationID': 'int64',
    'DOLocationID': 'int64',
    'payment_type': 'int64',
    'fare_amount': float,
    'extra':float,
    'mta_tax':float,
    'tip_amount':float,
    'tolls_amount':float,
    'improvement_surcharge':float,
    'total_amount':float,
    'congestion_surcharge':float

}

def read_file(bucket_name,object_key):
    parquet_file=pq.ParquetFile(f"s3://{bucket_name}/{object_key}")
    return parquet_file

def delete_files_with_prefix(bucket_name, path):
    # List objects with the specified prefix
    objects_to_delete = s3.list_objects_v2(Bucket=bucket_name, Prefix=path)
    
    if 'Contents' not in objects_to_delete:
        print(f"No objects found at {path}")
        return
    
    # Create a list of objects and delete
    delete_keys = [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]
    response = s3.delete_objects(Bucket=bucket_name,
        Delete={
                'Objects': delete_keys
            }
        )
        
    print(f"Deleted all objects at {path}")
    return None

def transform_data(df):
    
    # remove null values and rows with zero passengers  
    df_cleaned=df.dropna()
    df_cleaned=df_cleaned.loc[df_cleaned['passenger_count']!= 0]
    df_cleaned=df_cleaned.astype(taxi_dtypes)

    #column names to lowercase
    df_cleaned.columns=df_cleaned.columns.str.lower()
    
    #fix datatypes
    df_cleaned['tpep_pickup_datetime'] = pd.to_datetime(df_cleaned['tpep_pickup_datetime'])
    df_cleaned['tpep_dropoff_datetime'] = pd.to_datetime(df_cleaned['tpep_dropoff_datetime'])
    df_cleaned['tpep_pickup_date'] = df_cleaned['tpep_pickup_datetime'].dt.date  
    mapping = {'N': 0, 'Y': 1}
    df_cleaned['store_and_fwd_flag'] = df_cleaned['store_and_fwd_flag'].map(mapping).astype('int64')

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

    #for idempotence, delete files if any
    delete_files_with_prefix(bucket_name,output_object_key)
    
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
        'object_path': output_object_key
    }
