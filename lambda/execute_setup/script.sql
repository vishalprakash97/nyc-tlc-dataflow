-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

-- Create Tables in Bronze Schema
CREATE TABLE IF NOT EXISTS bronze.green_tripdata (
    "vendorid" INTEGER,
    "lpep_pickup_datetime" TIMESTAMP,
    "lpep_dropoff_datetime" TIMESTAMP,
    "store_and_fwd_flag"  INTEGER,
    "ratecodeid" INTEGER,
    "pulocationid" INTEGER,
    "dolocationid" INTEGER,
    "passenger_count" INTEGER,
    "trip_distance" DOUBLE PRECISION,
    "fare_amount" DOUBLE PRECISION,
    "extra" DOUBLE PRECISION,
    "mta_tax" DOUBLE PRECISION,
    "tip_amount" DOUBLE PRECISION,
    "tolls_amount" DOUBLE PRECISION,    
    "ehail_fee" DOUBLE PRECISION,
    "improvement_surcharge" DOUBLE PRECISION,
    "total_amount" DOUBLE PRECISION,
    "payment_type" INTEGER,
    "trip_type" INTEGER,
    "congestion_surcharge" DOUBLE PRECISION,
    "pickup_date" TIMESTAMP
)
DISTSTYLE EVEN
SORTKEY (lpep_pickup_datetime);

CREATE TABLE IF NOT EXISTS bronze.yellow_tripdata (
    "vendorid" INTEGER,
    "tpep_pickup_datetime" TIMESTAMP,
    "tpep_dropoff_datetime" TIMESTAMP,
    "passenger_count" INTEGER,
    "trip_distance" DOUBLE PRECISION,
    "ratecodeid" INTEGER,
    "store_and_fwd_flag"  INTEGER,
    "pulocationid" INTEGER,
    "dolocationid" INTEGER,
    "payment_type" INTEGER,
    "fare_amount" DOUBLE PRECISION,
    "extra" DOUBLE PRECISION,
    "mta_tax" DOUBLE PRECISION,
    "tip_amount" DOUBLE PRECISION,
    "tolls_amount" DOUBLE PRECISION,
    "improvement_surcharge" DOUBLE PRECISION,
    "total_amount" DOUBLE PRECISION,
    "congestion_surcharge" DOUBLE PRECISION,
    "airport_fee" DOUBLE PRECISION,
    "pickup_date" TIMESTAMP
)
DISTSTYLE EVEN
SORTKEY (tpep_pickup_datetime);

CREATE TABLE IF NOT EXISTS bronze.taxi_zones (
    "locationid" INTEGER,
    "borough" VARCHAR(20),
    "zone" VARCHAR(50),
    "service_zone" VARCHAR(20)
)
DISTSTYLE EVEN;

-- Create Groups
CREATE GROUP transformer;
CREATE GROUP reporter;

-- Create Users
CREATE USER dbt_user 
    PASSWORD '{PASSWORD_PLACEHOLDER}'
    IN GROUP transformer;

-- Grant necessary privileges to transformer group
GRANT CREATE ON DATABASE nyc_tlc_db TO GROUP transformer;

GRANT USAGE ON SCHEMA bronze TO GROUP transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO GROUP transformer;

GRANT USAGE, CREATE ON SCHEMA silver TO GROUP transformer;
GRANT SELECT, INSERT, UPDATE, DELETE, DROP ON ALL TABLES IN SCHEMA gold TO GROUP transformer;

GRANT USAGE, CREATE ON SCHEMA gold TO GROUP transformer;
GRANT SELECT, INSERT, UPDATE, DELETE, DROP ON ALL TABLES IN SCHEMA gold TO GROUP transformer;

-- Grant necessary privileges to transformer group
-- Grant usage and select privileges to reporter group on gold schema
GRANT USAGE ON SCHEMA gold TO GROUP reporter;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO GROUP reporter;

-- Automate privileges for reporter on newly created tables in gold schema
ALTER DEFAULT PRIVILEGES FOR USER dbt_user IN SCHEMA gold
GRANT SELECT ON TABLES TO GROUP reporter;

-- Grant access to system tables (Granted to users/groups by default in Redshift)
-- GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO GROUP transformer;
-- GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO GROUP transformer;
