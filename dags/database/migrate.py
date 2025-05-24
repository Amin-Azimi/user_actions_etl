# /database/migrate.py

from alembic.config import Config
from alembic import command
import os

# Load from environment or fallback to hardcoded (adjust if needed)
db_url = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

# Path to alembic.ini inside the container
alembic_cfg = Config("/opt/airflow/database/alembic.ini")
alembic_cfg.set_main_option("sqlalchemy.url", db_url)

# Apply all available migrations
command.upgrade(alembic_cfg, "head")
