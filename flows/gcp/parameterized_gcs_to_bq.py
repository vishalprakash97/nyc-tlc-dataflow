#importing Libraries
import pandas as pd

from pathlib import Path
import pandas_gbq,pyarrow
from prefect import flow,task
from prefect_gcp import GcpCredentials, GcsBucket
from prefect.tasks import task_input_hash

@task()
def extract_from_gcs(color,year,month):
    path=f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block=GcsBucket.load("gcs-creds")
    gcs_block.download_object_to_path(from_path=path,to_path=path)
    return Path(path)

def transform_data(path):
    df=pd.read_parquet(path)
    print(f"pre: Passenger Missing Count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0,inplace=True)
    print(f"post: Passenger Missing Count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df):
    gcp_credentials_block=GcpCredentials.load("gcp-creds")
    df.to_gbq(destination_table="yellow_taxi_trips.rides",
              project_id="project-one-402615",
              credentials=gcp_credentials_block.get_credentials_from_service_account(),
              chunksize=100000,
              if_exists="append")


@flow()
def etl_gcs_to_bq():
    color='yellow'
    year='2021'
    month=1
    
    path=extract_from_gcs(color,year,month)
    df=transform_data(path)
    write_bq(df)

if __name__=="__main__":
    etl_gcs_to_bq()
