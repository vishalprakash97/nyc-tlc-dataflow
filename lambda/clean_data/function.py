import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os

s3=boto3.client("s3")
bucket_name=os.environ['bucket_name']

color_dtypes={
    'yellow':{
            'airport_fee':'float64'
    },
    'green':{
        'ehail_fee':'float64', 
        'trip_type':'int32'
    }
}

common_dtypes={
        'vendorid':'int32',
        'passenger_count':'int32',
        'trip_distance':'float64',
        'ratecodeid':'int32',
        'store_and_fwd_flag':str,
        'pulocationid':'int32',
        'dolocationid':'int32',
        'payment_type':'int32',
        'fare_amount':'float64',
        'extra':'float64',
        'mta_tax':'float64',
        'tip_amount':'float64',
        'tolls_amount':'float64',
        'improvement_surcharge':'float64',
        'total_amount':'float64',
        'congestion_surcharge':'float64'
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

def transform_data(df,color):
        
    #column names to lowercase
    df.columns=df.columns.str.lower()
    if color=='green':
        df['pickup_date'] = df['lpep_pickup_datetime'].dt.date  
        #most ehail fees are null
        df['ehail_fee']=df['ehail_fee'].fillna(0)
    else:
        df['pickup_date'] = df['tpep_pickup_datetime'].dt.date  
        
    # remove null values and rows with zero passengers  
    df_cleaned=df.dropna()
    df_cleaned=df_cleaned.loc[df['passenger_count']!= 0]
    
    #fix datatypes
    taxi_dtypes=common_dtypes|color_dtypes[color]
    df_cleaned=df_cleaned.astype(taxi_dtypes)
    mapping = {'N': 0, 'Y': 1}
    df_cleaned['store_and_fwd_flag'] = df_cleaned['store_and_fwd_flag'].map(mapping).astype('int32')
    
    return df_cleaned


def write_to_s3(df, bucket_name, base_object_key):
    # Convert the dataframe to a pyarrow table
    table = pa.Table.from_pandas(df)    
    s3_path = f"s3://{bucket_name}/{base_object_key}"
    
    # Use write_to_dataset to write the partitioned data to S3
    pq.write_to_dataset(table, root_path=s3_path, partition_cols=['pickup_date'])
    
    return s3_path

def lambda_handler(event, context):
    year=event['year']
    month=event['month']
    color=event['color']
    
    object_key=f"raw/{color}/{year}/{color}_tripdata_{year}-{month:02d}.parquet"
    
    output_object_key= os.path.splitext(object_key)[0]
    output_object_key=output_object_key.replace('raw','cleaned')

    #for idempotence, delete files if any
    delete_files_with_prefix(bucket_name,output_object_key)
    
    parquet_file=read_file(bucket_name,object_key)
    old,new=0,0
    for batch in parquet_file.iter_batches(batch_size=100000):
        batch_df = batch.to_pandas()
        old+=len(batch_df)
        transformed_df = transform_data(batch_df,color)
        new+=len(transformed_df)
        s3_path=write_to_s3(transformed_df, bucket_name, output_object_key)
    
    print(f"Data written to {s3_path}")
    print(f"Before Cleaning | Rows: {old}")  
    print(f"After Cleaning | Rows: {new}") 
        
    return {
        'status': 200,
        'message': "Success. Files Uploaded to S3",
        'bucket_name': bucket_name,
        'color':color,
        'month':month,
        'year':year
    }
