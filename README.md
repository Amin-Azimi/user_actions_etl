# User Actions ETL Pipeline

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline to process mobile app logs from `dags/raw_data/raw_logs.json`, transform them into a star schema, and load them into a PostgreSQL database (`user_actions`). The pipeline uses Apache Airflow for orchestration, Docker Compose for containerization, Alembic for database migrations, and SQLAlchemy for schema definition. ETL logic resides in `dags/` (`extract.py`, `transform.py`, `load.py`), with utilities in `dags/utils/` (`db.py`, `quality_checks.py`). The pipeline handles non-unique `user_id` values, performs pre-transform quality checks, and retrieves existing `action_key` values for `action_type` in subsequent runs to ensure data consistency.

## Star Schema
The PostgreSQL database uses a star schema with five tables:
- **dim_users**: Unique users.
  - Columns: `user_key` (Integer, Primary Key), `user_id` (Varchar, Not Null, Unique).
- **dim_actions**: Unique action types.
  - Columns: `action_key` (Integer, Primary Key, Autoincrement), `action_type` (Varchar(100), Not Null, Unique).
- **dim_devices**: Unique device types.
  - Columns: `device_key` (Integer, Primary Key, Autoincrement), `device_type` (Varchar(50), Not Null, Unique).
- **dim_locations**: Unique locations.
  - Columns: `location_key` (Integer, Primary Key, Autoincrement), `location_name` (Varchar(100), Not Null, Unique).
- **fact_user_actions**: Action events.
  - Columns: `action_event_id` (Integer, Primary Key, Autoincrement), `user_key` (Integer, Foreign Key), `action_key` (Integer, Foreign Key), `device_key` (Integer, Foreign Key, Nullable), `location_key` (Integer, Foreign Key, Nullable), `timestamp` (Timestamp with Timezone, Not Null).

## Approach
1. **Schema Creation**:
   - Defined in `dags/database/model.py`, applied via Alembic migrations in `dags/database/alembic/`.
   - Ensures uniqueness for `user_id`, `action_type`, `device_type`, and `location_name`.
2. **Extraction** (`dags/extract.py`):
   - Reads JSON data from `dags/raw_data/raw_logs.json` using `read_json_log`.
3. **Pre-Transform Quality Check** (`dags/utils/quality_checks.py`):
   - Validates raw data for nulls in `user_id`, `action_type`, `timestamp` and duplicates based on `user_id`, `timestamp`, `action_type`.
   - Returns cleaned data, excluding null or duplicate records.
4. **Transformation** (`dags/transform.py`):
   - Cleans data (formats timestamps, handles invalid entries).
   - Deduplicates `user_id`, `action_type`, `device`, and `location`.
   - Uses `dags/utils/db.py` to query `dim_actions`, `dim_devices`, and `dim_locations` for existing keys.
   - Maps log entries to `user_key`, `action_key`, `device_key`, and `location_key`.
5. **Loading** (`dags/load.py`):
   - Inserts data into dimension and fact tables using `dags/utils/db.py`.
   - Validates data quality using `dags/utils/quality_checks.py`.
6. **Configuration** (`dags/config.py`):
   - Defines database connection strings and file paths.
7. **Orchestration**:
   - Airflow DAG (`dags/etl_pipeline_dag.py`) runs tasks: `extract_task >> pre_transform_quality_check_task >> transform_task >> load_task`.
8. **Containerization**:
   - Docker Compose sets up Airflow (webserver, scheduler, PostgreSQL for metadata) and a PostgreSQL database for output.

## Project Structure
```
user_actions_etl/
├── dags/
│   ├── database/
│   │   ├── alembic/
│   │   │   ├── env.py
│   │   │   ├── script.py.mako
│   │   │   └── versions/
│   │   │       └── 001_initial_schema.py
│   │   └── model.py
│   ├── raw_data/
│   │   └── raw_logs.json
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── db.py
│   │   └── quality_checks.py
│   ├── config.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── etl_pipeline_dag.py
├── init-db/
│   └── (initialization scripts, if any)
├── docker-compose.yaml
├── requirements.txt
├── README.md
```

## Setup Instructions
### Prerequisites
- Docker and Docker Compose installed.
- `dags/raw_data/raw_logs.json` in the project directory.

### Steps
1. **Generate Fernet Key**:
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```
   Replace `AIRFLOW__CORE__FERNET_KEY` in `docker-compose.yaml` with the generated key.

2. **Run Docker Compose**:
   ```bash
   docker-compose up -d
   ```
   This starts:
   - PostgreSQL for Airflow metadata (`airflow_metadata`).
   - PostgreSQL for output database (`user_actions`).
   - Airflow initializer (sets up database, admin user).
   - Airflow webserver (port 8080).
   - Airflow scheduler.

3. **Access Airflow**:
   - Open `http://localhost:8080` in a browser.
   - Log in with username `admin`, password `admin`.
   - Enable and trigger the `etl_pipeline_dag` DAG manually.

4. **Verify Output**:
   - Connect to the PostgreSQL output database:
     ```bash
     docker exec -it user_actions_etl_output_db_1 psql -U user -d user_actions
     \dt
     SELECT * FROM dim_users;
     SELECT * FROM dim_actions;
     SELECT * FROM dim_devices;
     SELECT * FROM dim_locations;
     SELECT * FROM fact_user_actions;
     ```
   - Check Airflow task logs for validation results (e.g., nulls, duplicates, uniqueness).

5. **Stop Services**:
   ```bash
   docker-compose down
   ```

## Data Quality Checks
- **Pre-Transform** (`pre_transform_quality_check_task`):
  - Checks for nulls in `user_id`, `action_type`, `timestamp` using `check_nulls`.
  - Checks for duplicates based on `user_id`, `timestamp`, `action_type` using `check_duplicates`.
  - Excludes invalid records, logs results.
- **Post-Load** (`load_task`):
  - Ensures uniqueness in `dim_users.user_id`, `dim_actions.action_type`, `dim_devices.device_type`, and `dim_locations.location_name`.
  - Validates foreign key relationships in `fact_user_actions`.
  - Logs quality metrics in Airflow.

## Notes
- **PostgreSQL**: The output database is PostgreSQL (`user_actions`). Airflow metadata uses a separate PostgreSQL instance (`airflow_metadata`).
- **Task Sequence**: `extract_task >> pre_transform_quality_check_task >> transform_task >> load_task`.
- **Subsequent Runs**: Queries `dim_actions`, `dim_devices`, and `dim_locations` via `dags/utils/db.py` to reuse existing keys, preventing foreign key errors.
- **Cache Removal**: `__pycache__` directories are excluded via `.gitignore` and removed before running.
- **init-db/**: Assumed to contain optional initialization scripts; update if critical.
- **Performance**: Ensure 4GB+ RAM for Docker Compose.
- **Security**: Do not commit `docker-compose.yaml` with the Fernet key; use environment variables in production.

## Future Improvements
- Add incremental data loading for large datasets.
- Implement detailed logging for monitoring.
- Add unit tests for ETL and utility modules.
- Configure Airflow for scheduled runs.

## Repository
- GitHub: `github.com/amin-azimi/user_actions_etl`

## Example Data
For `dags/raw_data/raw_logs.json`:
```json
[
    {"user_id": "user_8", "action_type": "login", "timestamp": "2025-05-23T10:30:00Z", "metadata": {"device": "iPhone", "location": "US"}},
    {"user_id": "user_8", "action_type": "purchase", "timestamp": "2025-05-23T10:35:00Z", "metadata": {"device": "iPhone", "location": "US"}},
    {"user_id": "user_12", "action_type": "login", "timestamp": "2025-05-23T11:00:00Z", "metadata": {"device": "Android", "location": "EU"}}
]
```
The database stores:
- `dim_users`: Unique users (`user_8`, `user_12`).
- `dim_actions`: Unique actions (`login`, `purchase`).
- `dim_devices`: Unique devices (`iPhone`, `Android`).
- `dim_locations`: Unique locations (`US`, `EU`).
- `fact_user_actions`: All actions with `user_key`, `action_key`, `device_key`, `location_key`, etc.

  ![image](https://github.com/user-attachments/assets/38953231-e782-489c-9d13-a0e207b08200)
