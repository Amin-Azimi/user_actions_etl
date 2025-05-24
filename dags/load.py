import os
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from database.model import FactUserActions
from utils.db import get_engine, get_session 

log = logging.getLogger(__name__)

def load_data(ti=None):
    """
    Loads cleaned data into the FactUserActions table in the PostgreSQL database.

    Args:
        ti: The Airflow TaskInstance object, used to pull XComs (if applicable).
            This function expects 'cleaned_data' to be passed via XComs from the 'transform' task.
    """
    log.info("Starting load_data process.")

    if ti is None:
        log.error("Task Instance (ti) not provided to load_data. Cannot retrieve data.")
        raise ValueError("Airflow Task Instance (ti) is required for XCom pull.")

    cleaned_data = ti.xcom_pull(task_ids='transform', key='transformed_log_data')

    if not cleaned_data:
        log.warning("No cleaned data found from the 'transform' task. Skipping data load.")
        return

    log.info(f"Received {len(cleaned_data)} items for loading into FactUserAction table.")


    engine = None
    try:
        engine = get_engine() 
        log.info("Successfully obtained database engine.")

        with get_session(engine) as session:
            log.info("Opened database session for loading.")
            successful_inserts = 0
            for i, entry in enumerate(cleaned_data):
                try:
                    log.debug(f"Processing entry {i+1}/{len(cleaned_data)}: {entry}")
                    
                    action = FactUserActions(**entry)
                    session.add(action)
                    successful_inserts += 1

                except TypeError as e:
                    log.error(f"Schema mismatch or invalid data for entry {i+1}: {entry}. Error: {e}", exc_info=True)
                    raise

                except SQLAlchemyError as e:
                    log.error(f"Database error adding entry {i+1}: {entry}. Error: {e}", exc_info=True)
                    session.rollback()
                    raise

                except Exception as e:
                    log.error(f"An unexpected error occurred processing entry {i+1}: {entry}. Error: {e}", exc_info=True)
                    session.rollback()
                    raise

            session.commit()
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
            engine.dispose()

    log.info("load_data process completed.")