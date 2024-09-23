import boto3
from bs4 import BeautifulSoup
import os
import requests
import re 

s3=boto3.client("s3")
ssm=boto3.client("ssm")


bucket_name=os.getenv('bucket_name')
source_url=os.getenv('url')

ssm_month_var=os.getenv('ssm_month_var')
ssm_year_var=os.getenv('ssm_year_var')

def get_url(color: str ,year: int, month: int):
    #fetch source url of website
    source=requests.get(source_url)
    soup=BeautifulSoup(source.content,"lxml")
    
    #find url of required parqsuet file
    url_object=soup.find("a",href=re.compile(f"{color}_tripdata_{year}-{month:02d}"))
    if url_object is None:
        raise LookupError("URL Does not exist, File not uploaded yet.")
    
    url=url_object['href'].strip()
    print(f"File URL: {url}")
    
    return url

def upload_to_s3(url, year, month,color):
    #fetch parquet file
    response=requests.get(url)
    
    #upload file to s3
    object_key=f"raw/{color}/{year}/{color}_tripdata_{year}-{month:02d}.parquet"
    s3.put_object(Body=response.content, Bucket=bucket_name, Key=object_key)
    print(f"File Uploaded to {bucket_name}/{object_key}")
    return object_key
    
def update_ssm_parameters(month,year):
    #get next month and year
    year=year+(month)//12
    month=(month%12) +1
    
    ssm.put_parameter(Name=ssm_month_var, Value=str(month), Type='String', Overwrite=True)
    ssm.put_parameter(Name=ssm_year_var, Value=str(year), Type='String', Overwrite=True)
    print("SSM Parameters Updated")
    return None

def lambda_handler(event, context):
    month=int(event['month'])
    year=int(event['year'])
    color=event['color']
    url=get_url(color,year, month)
    object_key=upload_to_s3(url, year, month, color)
    update_ssm_parameters(month,year)
    return {
        'status': 200,
        'message': "Success. File Uploaded to S3",
        'bucket_name': bucket_name,
        'month': month,
        'year': year,
        'color': color
    }   
        
