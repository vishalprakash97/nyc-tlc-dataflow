## clean_data

### Description
* Read parquet file from S3 Bucket
* Delete existing files(if any) to preserve idempotence
* Data read in chunks for memory efficiency
* Transform data
  * Drop null values
  * Drop rows with ```passenger_count=0```
  * Convert all column names to lowercase
  * Fix datatype mismatch
* Partition data by Date
* Upload partitions to S3 bucket
  
### Input
* **From Event**
  * *object_key*
* **From Environment variables**
  * *bucket_name*
  
### Output
* *bucket_name*
* *path*