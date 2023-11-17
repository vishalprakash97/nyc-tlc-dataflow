# ETL to Postgres

This repo includes code for:
- Configuring Docker containers for PostgreSQL and pgAdmin
- ETL to PostgreSQL server

### Useful Commands
- Creating Network
```
docker network create <network-name>
```

- Running Docker containers from CLI
```
docker run -it \
  -e POSTGRES_USER=<user-id> \
  -e POSTGRES_PASSWORD=<password> \
  -e POSTGRES_DB=<db-name> \
  -v <your-folder>/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network <network name> \
  --name <container-name> \
  postgres:13

docker run -it \
    -v <your-folder>/pgadmin_data:/var/lib/pgadmin \
    -e PGADMIN_DEFAULT_EMAIL=<email-id> \
    -e PGADMIN_DEFAULT_PASSWORD=<password> \
    -p 8080:80 \
    --network <network-name> \
    --name <container-name> \
    dpage/pgadmin4
```
- Running PGCLI
```
pgcli -h <localhost/ip-addr> -p 5432 -u <user-id> -d <db-name>
```

- Convert jupyter notebook to .py script
```
jupyter nbconvert --to=script upload_data.ipynb
```

- Run upload_data.py
```
python3 upload_data.py \
  --host=<localhost/ip-addr> \
  --port=5432 \
  --db_name=<db-name> \
  --table_name=<table-name> \
  --csv_url=${url}
```
*Note*: Create .env file containing username and password

- Build Docker images from docker-compose.yaml
```
docker-compose up -d
```
- Build Dockerfile
```
docker build -t taxi_ingest:v01 . 
```



- Run Docker
```
docker run -it \
    --network=<network-name> \
    taxi_ingest:v01 \
        --host=pg-database \
        --port=5432 \
        --db_name=<db-name> \
        --table_name=<table-name> \
        --csv_url=${url} 
```

