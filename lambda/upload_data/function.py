import boto3
import json
import os
import time
# Initialize Redshift Data API client
client = boto3.client('redshift-data')

# Environment variables
db_name = os.environ['redshift_db']
workgroup_name = os.environ['workgroup_name']
redshift_role_arn = os.environ['iam_role_arn']
secret_arn= os.environ['secret_arn']
schema_name='bronze'

def wait_for_statement_to_finish(statement_id,description):
    # Polling to check the status of the statement
    prev_status= None
    while True:
        response = client.describe_statement(Id=statement_id)
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

def execute_sql(statement,secret_arn,workgroup_name,db_name):
    response = client.execute_statement(
            WorkgroupName=workgroup_name,
            SecretArn=secret_arn,
            Database=db_name,
            Sql=statement
            )
    return response

def lambda_handler(event, context):

    bucket_name = event['bucket_name']
    color=event['color']
    month=event['month']
    year=event['year']
    
    table_name=f"{color}_tripdata"
    object_path=f"cleaned/{color}/{year}/{color}_tripdata_{year}-{month:02d}"

    # SQL statements to run
    copy_from_s3_sql = f"""
    COPY {schema_name}.{table_name}
    FROM 's3://{bucket_name}/{object_path}'
    IAM_ROLE '{redshift_role_arn}'
    FORMAT AS PARQUET;
    """
    
    # Execute SQL statements
    try:
        response= execute_sql("BEGIN;",secret_arn,workgroup_name,db_name)
        wait_for_statement_to_finish(response['Id'],"BEGIN")
        
        response= execute_sql(copy_from_s3_sql,secret_arn,workgroup_name,db_name)
        wait_for_statement_to_finish(response['Id'],"COPY")

        response= execute_sql("COMMIT;",secret_arn,workgroup_name,db_name)
        wait_for_statement_to_finish(response['Id'],"COMMIT")

    except Exception as e:
        response= execute_sql("ROLLBACK;",secret_arn,workgroup_name,db_name)
        print(f"Error executing statements: {e}")
        raise e

    return {
        'status': 200,
        'message': "Success. Data loaded into Redshift Serverless."
    }
