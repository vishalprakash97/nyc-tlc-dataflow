## Cloud Formation 

### template.yaml

- S3 Bucket
- Lambda Functions and associated IAM Roles
  - _fetch_data_
  - _clean_data_
  - _upload_data_
- SSM parameters
- Redshift Nested Stack 
- StepFunctions Nested Stack

### redshift.yaml

- Serverless Namespace
- Serverless Workgroup
- Secrets Manger Secret for Redshift
- IAM Role for Namespace


## stepfunctions.yaml

- Core ETL pipeline
- Backfill pipeline
- Monthly pipeline
  - EventBridge Schedule
- IAM Roles and policies for state machines