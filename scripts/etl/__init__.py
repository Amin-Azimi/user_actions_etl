# scripts/etl/__init__.py

# Optional: expose key ETL functions for top-level access
from .extract import read_json_logs
from .transform import clean_and_transform
from .load import load_to_postgres
