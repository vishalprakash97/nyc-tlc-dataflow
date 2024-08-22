## fetch_data

### Description
* Fetch [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) using BeatifulSoup
* Upload to S3
  
### Input
* **From Event**
  * *year*
  * *month*
* **From Environment variables**
  * *bucket_name*
  * *color*
  * *url* : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
  
### Output
* *bucket_name*
* *object_key*