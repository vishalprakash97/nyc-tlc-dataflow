## clean_data.py

* Read parquet file from S3 Bucket
* Data read in chunks for memory efficiency
* Transform data
  * Drop null values
  * Drop rows with ```passenger_count=0```
* Partition data by Date
* Upload partitions to S3 bucket
  
