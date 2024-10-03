## upload_data

### Description
* Create table if it does not exist
* Upload data to Redshift Serverless using COPY command
  
### Input
* **From Event**
  * *bucket_name*
  * *object_path*
* **From Environment variables**
  * *db_name*
  * *workgroup_name*
  * *table_name*
  * *redshift_role_arn*
  * *secret_arn*
  