import boto3
import os
import requests
import string
import secrets
import re
import json

s3=boto3.client("s3")
ssm=boto3.client("ssm")
redshift=boto3.client("redshift-data")

bucket_name=os.getenv('bucket_name')
db_name = os.environ['redshift_db']
workgroup_name = os.environ['workgroup_name']
redshift_role_arn = os.environ['iam_role_arn']
secret_arn= os.environ['secret_arn']

object_key=f"lookup/taxi_zones.csv"
source_url="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
password_var=os.environ['parameter_name']

copy_from_s3_sql = f"""
    COPY bronze.taxi_zones
    FROM 's3://{bucket_name}/{object_key}'
    IAM_ROLE '{redshift_role_arn}'
    CSV
    IGNOREHEADER 1;
    """

#function to download file and write to s3
def load_taxi_zones(url, bucket_name,object_key, secret_arn, workgroup_name, db_name):
    #fetch csv file
    response=requests.get(url)
    
    #upload file to s3
    s3.put_object(Body=response.content, Bucket=bucket_name, Key=object_key)
    print(f"File Uploaded to {bucket_name}/{object_key}")
    
    response = execute_sql_statement(copy_from_s3_sql, secret_arn, workgroup_name, db_name)
    wait_for_statement_to_finish(response['Id'], "COPY")
    print(f"File Uploaded to Redshift")
    return object_key

def get_script(script_path,password):
    with open(script_path, 'r') as sql_file:
        sql_script = sql_file.read()
    sql_script = sql_script.replace('{PASSWORD_PLACEHOLDER}', password)
    return sql_script

def generate_secure_password(length=12):
    # generate a secure random password
    alphabet = string.ascii_letters + string.digits + "_-"
    while True:
        password = ''.join(secrets.choice(alphabet) for i in range(10))
        if (any(c.islower() for c in password)
            and any(c.isupper() for c in password)
            and any(c.isdigit() for c in password)):
            break
    return password

def upload_password_to_ssm(var_name,password):
    ssm.put_parameter(Name=var_name, Value=str(password), Type='SecureString', Overwrite=True)
    print("Uploaded Password to SSM")
    return None

def wait_for_statement_to_finish(statement_id,description):
    # Polling to check the status of the statement
    prev_status= None
    while True:
        response = redshift.describe_statement(Id=statement_id)
        status = response['Status']
        
        if status == 'FINISHED':
            print(f"{description} Executed successfully.")
            break
        elif status in ['FAILED','ABORTED']:
            raise Exception(f"Statement failed: {response['Error']}")
        elif status == prev_status:
            continue
        else:
            print(f"Statement status: {status}")
            prev_status= status
    return None

def execute_sql_statement(statement,secret_arn,workgroup_name,db_name):
    response = redshift.execute_statement(
            WorkgroupName=workgroup_name,
            SecretArn=secret_arn,
            Database=db_name,
            Sql=statement
            )
    return response

def execute_sql_script(sql_script, secret_arn, workgroup_name, db_name):
    
    #discarding comments
    sql_script = re.sub(r'(--.*?\n)|(/\*.*?\*/)', '', sql_script, flags=re.DOTALL)
    for statement in sql_script.split(';'):
        #discarding empty lines
        if not statement.strip():
            continue
        response = execute_sql_statement(statement, secret_arn, workgroup_name, db_name)
        wait_for_statement_to_finish(response['Id'], statement.split()[0])

def send_cfn_response(event, context, status):
    
    responseUrl = event['ResponseURL']
    responseBody={
        'StackId' : event['StackId'],
        'RequestId' : event['RequestId'],
        'LogicalResourceId' : event['LogicalResourceId'],
        'PhysicalResourceId': context.log_stream_name,
        'Status':status,
        'Reason': f"{event['LogicalResourceId']} Failed, Check Logs: {context.log_stream_name}"    
    }

    json_responseBody=json.dumps(responseBody)
    headers = {
        'content-type' : '',
        'content-length' : str(len(json_responseBody))
    }
    response = requests.put(responseUrl, data=json_responseBody, headers=headers)
    print(response)


def lambda_handler(event, context):

    print(event)
    print("Request Type:",event['RequestType'])
    try:
        if event['RequestType'] == 'Create':
            password=generate_secure_password()
            upload_password_to_ssm(password_var,password)
            script=get_script("./script.sql",password)
            execute_sql_script(script, secret_arn, workgroup_name, db_name)
            load_taxi_zones(source_url,bucket_name,object_key, secret_arn, workgroup_name, db_name)
        else:
            print("No Action Taken")
        send_cfn_response(event, context, "SUCCESS")
    
    except Exception as e:
        send_cfn_response(event, context, "FAILED")
    
    return None