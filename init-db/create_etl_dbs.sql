-- init-db/create_etl_dbs.sql

-- Create database for raw ETL data

-- Create database for processed ETL data
CREATE DATABASE etl;

-- Optional: Grant all privileges on the new databases to the 'airflow' user
-- This assumes 'airflow' user already exists and has the necessary superuser privileges
-- from POSTGRES_USER in the environment.
GRANT ALL PRIVILEGES ON DATABASE etl TO airflow;
