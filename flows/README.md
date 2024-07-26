```
url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python3 upload_data.py \
  --db_name=ny_taxi \
  --table_name=yellow_taxi_trips \
  --csv_url=${url}
```

```
prefect server start
```

- pyarrow required for gzip compression:
- pandas-gbq for writing to Big Query 
- pip install package and then register blocks for the module

```bash
# create deployment file, add parameters in file
prefect deployment build flows/gcp/etl_to_gcs.py:<flow_name> -n "some_name
```

```bash
#Load Deployment to Prefect
prefect deployment apply <yaml-file>
``` 
* Applying a Deployment schedules a run
* Agent runs it in a work queue
