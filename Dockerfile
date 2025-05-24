# Dockerfile
FROM apache/airflow:3.0.1

USER root

# System dependencies for PostgreSQL and general development
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt


RUN mkdir -p /opt/airflow/dags && chmod 775 /opt/airflow/dags
USER airflow
