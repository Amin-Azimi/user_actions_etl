import os
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from database.model import FactUserActions # Ensure this import path is correct and accessible
from utils.db import get_engine, get_session # Ensure these import paths are correct and accessible
from config import DATABASE_URL

# Get a logger for this module
log = logging.getLogger(__name__)

def load_data(ti=None):
    """
    Loads cleaned data into the FactUserActions table in the PostgreSQL database.

    Args:
        ti: The Airflow TaskInstance object, used to pull XComs (if applicable).
            This function expects 'cleaned_data' to be passed via XComs from the 'transform' task.
    """
    log.info("Starting load_data process.")

    # --- 1. Retrieve Data from Upstream Task ---
    if ti is None:
        log.error("Task Instance (ti) not provided to load_data. Cannot retrieve data.")
        raise ValueError("Airflow Task Instance (ti) is required for XCom pull.")

    # Assuming 'transform' task pushed data with key 'transformed_log_data'
    cleaned_data = ti.xcom_pull(task_ids='transform', key='transformed_log_data')

    if not cleaned_data:
        log.warning("No cleaned data found from the 'transform' task. Skipping data load.")
        return # Exit if no data to load

    log.info(f"Received {len(cleaned_data)} items for loading into FactUserAction table.")

    # --- 2. Database Connection ---
    # Ensure AIRFLOW__DATABASE__SQL_ALCHEMY_CONN points to the correct ETL database
    # (e.g., if you created 'etl_raw_db' or 'etl_processed_db' via init scripts).
    # If your model points to a different DB, you might need to adjust the conn string here.

    engine = None # Initialize engine to None for finally block
    try:
        engine = get_engine() # Pass the connection string to get_engine
        log.info("Successfully obtained database engine.")

        # --- 3. Data Loading Transaction ---
        with get_session(engine) as session: # Ensure get_session is a context manager
            log.info("Opened database session for loading.")
            successful_inserts = 0
            for i, entry in enumerate(cleaned_data):
                try:
                    # Log the entry being processed (can be debug if verbose)
                    log.debug(f"Processing entry {i+1}/{len(cleaned_data)}: {entry}")
                    
                    # Create model instance. Ensure 'entry' keys match FactUserAction constructor.
                    action = FactUserActions(**entry)
                    session.add(action)
                    successful_inserts += 1

                except TypeError as e: # Catch errors if entry keys don't match model fields
                    log.error(f"Schema mismatch or invalid data for entry {i+1}: {entry}. Error: {e}", exc_info=True)
                    # Decide if you want to skip, or fail here. For robustness, you might skip.
                    # For critical errors, you might re-raise.
                    raise # Re-raise to fail the task if data structure is critical

                except SQLAlchemyError as e:
                    # Catch database-specific errors during add (e.g., constraint violations)
                    log.error(f"Database error adding entry {i+1}: {entry}. Error: {e}", exc_info=True)
                    session.rollback() # Rollback current transaction to prevent partial commit
                    raise # Re-raise to fail the task

                except Exception as e:
                    log.error(f"An unexpected error occurred processing entry {i+1}: {entry}. Error: {e}", exc_info=True)
                    session.rollback() # Rollback on unexpected error
                    raise # Re-raise to fail the task

            session.commit() # Commit all changes in one transaction
            log.info(f"Successfully committed {successful_inserts} records to FactUserActions table.")

    except SQLAlchemyError as e:
        log.critical(f"A critical SQLAlchemy error occurred during load_data: {e}", exc_info=True)
        raise # Fail the task
    except Exception as e:
        log.critical(f"An unexpected critical error occurred in load_data: {e}", exc_info=True)
        raise # Fail the task
    finally:
        if engine:
            log.info("Disposing of database engine.")
            engine.dispose() # Ensure all connections in the pool are closed

    log.info("load_data process completed.")