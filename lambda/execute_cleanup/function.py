#new
import boto3
import os

import json
import requests

s3 = boto3.client('s3')
ssm=boto3.client("ssm")
bucket_name=os.environ['bucket_name']
parameter_name=os.environ['parameter_name']

def delete_ssm_parameter(parameter_name):
    ssm.delete_parameter(Name=parameter_name)
    print(f"SSM Parameter {parameter_name} Deleted")
    return None

def delete_bucket_objects(bucket_name):
    
    objects = s3.get_paginator("list_objects_v2")
    objects_iterator = objects.paginate(Bucket=bucket_name)
    
    for page in objects_iterator:
        if "Contents" in page:
            objects = [{"Key": obj["Key"],'VersionId': "null"} for obj in page["Contents"]]
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
            print(f"Objects Deleted: {len(objects)}")
    print(f"Emptied Bucket {bucket_name}")
    return None

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
        if event['RequestType'] == 'Delete':
            delete_bucket_objects(bucket_name)
            delete_ssm_parameter(parameter_name)
        else:
            print("No Action Taken")
        send_cfn_response(event, context, "SUCCESS")
    
    except Exception as e:
        send_cfn_response(event, context, "FAILED")
    
    return None
        