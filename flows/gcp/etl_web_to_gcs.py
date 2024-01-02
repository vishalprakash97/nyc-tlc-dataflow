#importing Libraries
import pandas as pd
import os
from datetime import timedelta
from pathlib import Path

from prefect import flow,task
from prefect_gcp import GcpCredentials,GcsBucket
from prefect.tasks import task_input_hash

@task(log_prints=True, retries=3,  cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def fetch_data(url: str):
    df=pd.read_csv(url)
    return df

@task(log_prints=True)
def transform_data(df):
    #change to datetime
    df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)
    #drop rows with zero passenger count
    print(f"pre:Zero Passenger Count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post:Zero Passenger Count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task()
def write_local(df,color,file):
    folder=Path(f"data/{color}")
    folder.mkdir(exist_ok=True,parents=True)

    path=Path(f"{folder}/{file}.parquet")
    df.to_parquet(path,compression="gzip")
    return path

@task()
def write_gcs(path):
    gcs_block=GcsBucket.load("gcs-creds")
    gcs_block.upload_from_path(from_path=path,to_path=path)
    return

@flow()
def etl_web_to_gcs():
    color="yellow"
    year=2021
    month=1
    dataset_file=f"{color}_tripdata_{year}-{month:02}"#fill 0 in width=2
    dataset_url=f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    #print(dataset_url)
    df=fetch_data(dataset_url)
    df_clean=transform_data(df)
    path=write_local(df_clean,color,dataset_file)
    write_gcs(path)

if __name__=="__main__":
    etl_web_to_gcs()