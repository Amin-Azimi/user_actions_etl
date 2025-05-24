#!/bin/bash
set -e

echo "📦 Running Alembic migrations..."
alembic -c database/alembic.ini  upgrade head

echo "🚀 Starting Airflow $@"
exec airflow "$@"
