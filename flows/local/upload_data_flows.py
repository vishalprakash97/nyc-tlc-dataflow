
#importing librarires
import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
from decouple import config
import os
from datetime import timedelta

from prefect import flow,task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_file(csv_url):
    #download csv from url
    if csv_url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    os.system(f"wget {csv_url} -O {csv_name}")#-o for log file, -O for naming output file
    return csv_name

@task(log_prints=True)
def transform_data(df):
    #change to datetime
    df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)
    #drop rows with zero passenger count
    df = df[df['passenger_count'] != 0]
    return df

@flow(name="Add_table_Subflow",log_prints=True)
def add_table(csv_name,table_name,engine):
    #add table
    df_head=pd.read_csv(csv_name,nrows=100)
    df_head=transform_data(df_head)
    df_head.head(0).to_sql(name=table_name,con=engine,if_exists='replace')

@flow(name="Upload_Subflow",log_prints=True)
def upload_data(csv_name,table_name,engine):
    #add data in chunks
    df_iter=pd.read_csv(csv_name,iterator=True,chunksize=100000)
    for i,chunk in enumerate(df_iter):
        
        t_start=time()  
        chunk=transform_data(chunk)
        chunk.to_sql(name=table_name,con=engine,if_exists='append')
        t_end=time()
        print(f"Inserted Chunk {i+1}: took {(t_end-t_start):.3f} seconds")
    

@flow(name="ETL_flow",log_prints=True)
def upload_pipeline(params):
    
    db_name=params.db_name
    table_name=params.table_name
    csv_url=params.csv_url
    connection_block=SqlAlchemyConnector.load("postgres-creds")
    with connection_block.get_connection(begin=False) as engine:
        csv_name=download_file(csv_url)
        add_table(csv_name,table_name,engine) 
        upload_data(csv_name,table_name,engine)


if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    #dbname tablename csv-url

    parser.add_argument('--db_name', help='name of postgres database')
    parser.add_argument('--table_name', help='name of the table in db')
    parser.add_argument('--csv_url', help='csv-url')
    args=parser.parse_args()

    upload_pipeline(args)
    
    


