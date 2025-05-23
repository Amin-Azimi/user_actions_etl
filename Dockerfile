# Dockerfile
FROM apache/airflow:2.7.2-python3.9

USER root

# System dependencies for PostgreSQL and general development
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy project files
COPY ./airflow /opt/airflow
COPY ./scripts /opt/airflow/scripts
COPY ./database /opt/airflow/database
COPY ./raw_data /opt/airflow/raw_data

USER airflow
