
#importing librarires
import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
from decouple import config
import os

def upload_data(params):
    user=config('username')
    password=config('password')
    host=params.host
    port=params.port
    db_name=params.db_name
    table_name=params.table_name
    csv_url=params.csv_url
    
    #download csv from url
    if csv_url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    os.system(f"wget {csv_url} -O {csv_name}")#-o for log file, -O for naming output file

    #connect engine
    engine=create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    engine.connect()

    #add table
    df_head=pd.read_csv(csv_name,nrows=100)
    df_head.tpep_pickup_datetime=pd.to_datetime(df_head.tpep_pickup_datetime)
    df_head.tpep_dropoff_datetime=pd.to_datetime(df_head.tpep_dropoff_datetime)
    df_head.head(0).to_sql(name=table_name,con=engine,if_exists='replace')

    #add data in chunks
    df_iter=pd.read_csv(csv_name,iterator=True,chunksize=100000)
    chunk_id=1
    while True:
        try:
            t_start=time()
            df=next(df_iter)
            
            df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)
            
            df.to_sql(name=table_name,con=engine,if_exists='append')
            t_end=time()
            print(f"Inserted Chunk {chunk_id}: took {(t_end-t_start):.3f} seconds")
            chunk_id+=1
        except:
            print('Done Inserting Data')
            break

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    #user password host port dbname tablename csv-url
    parser.add_argument('--host', help='hostname for postgres')
    parser.add_argument('--port', type=int,help='port number for postgres')
    parser.add_argument('--db_name', help='name of postgres database')
    parser.add_argument('--table_name', help='name of the table in db')
    parser.add_argument('--csv_url', help='csv-url')
    args=parser.parse_args()

    upload_data(args)
    
    


