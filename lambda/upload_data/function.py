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
table_name = os.environ['table_name']
secret_arn= os.environ['secret_arn']

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
    object_path = event['object_path']

    # SQL statements to run
    create_table_sql=f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            "vendorid" BIGINT,
            "tpep_pickup_datetime" TIMESTAMP,
            "tpep_dropoff_datetime" TIMESTAMP,
            "passenger_count" BIGINT,
            "trip_distance" DOUBLE PRECISION,
            "ratecodeid" BIGINT,
            "store_and_fwd_flag" BIGINT,
            "pulocationid" BIGINT,
            "dolocationid" BIGINT,
            "payment_type" BIGINT,
            "fare_amount" DOUBLE PRECISION,
            "extra" DOUBLE PRECISION,
            "mta_tax" DOUBLE PRECISION,
            "tip_amount" DOUBLE PRECISION,
            "tolls_amount" DOUBLE PRECISION,
            "improvement_surcharge" DOUBLE PRECISION,
            "total_amount" DOUBLE PRECISION,
            "congestion_surcharge" DOUBLE PRECISION,
            "airport_fee" DOUBLE PRECISION,
            "tpep_pickup_date" TIMESTAMP
        )
        DISTSTYLE KEY
        DISTKEY (tpep_pickup_date)
        SORTKEY (tpep_pickup_date, tpep_pickup_datetime);
    """

    copy_from_s3_sql = f"""
    COPY {table_name}
    FROM 's3://{bucket_name}/{object_path}'
    IAM_ROLE '{redshift_role_arn}'
    FORMAT AS PARQUET;
    """
    # Execute SQL statements
    try:
        response= execute_sql("BEGIN;",secret_arn,workgroup_name,db_name)
        wait_for_statement_to_finish(response['Id'],"BEGIN")

        response= execute_sql(create_table_sql,secret_arn,workgroup_name,db_name)
        wait_for_statement_to_finish(response['Id'],"CREATE TABLE")
        
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
