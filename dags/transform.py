# import logging
# from datetime import datetime

# # Get a logger for this module
# log = logging.getLogger(__name__)

# def clean_logs(ti=None):
#     """
#     Cleans and transforms raw log data, filtering out incomplete entries
#     and converting timestamps.

#     Args:
#         ti: The Airflow TaskInstance object, used to pull XComs.
#             This function expects 'raw_logs' to be pulled from the 'extract' task.
#     """
#     log.info("Starting clean_logs transformation process.")

#     # --- 1. Retrieve Data from Upstream Task ---
#     if ti is None:
#         log.error("Task Instance (ti) not provided to clean_logs. Cannot retrieve raw data.")
#         raise ValueError("Airflow Task Instance (ti) is required for XCom pull.")

#     # Pull data from the 'extract' task (assuming it was pushed to XComs)
#     raw_logs = ti.xcom_pull(task_ids='extract')

#     if not raw_logs:
#         log.warning("No raw log data found from the 'extract' task. Transformation skipped.")
#         return [] # Return an empty list if no data to process

#     log.info(f"Received {len(raw_logs)} raw log entries for transformation.")
#     clean_data = []
#     skipped_entries_count = 0

#     # --- 2. Iterate and Transform Data ---
#     for i, log_entry in enumerate(raw_logs):
#         log.debug(f"Processing raw log entry {i+1}/{len(raw_logs)}: {log_entry}")

#         # Check for essential fields
#         if not log_entry.get("user_id"):
#             log.warning(f"Skipping entry {i+1} due to missing 'user_id': {log_entry}")
#             skipped_entries_count += 1
#             continue
        
#         if not log_entry.get("action_type"):
#             log.warning(f"Skipping entry {i+1} due to missing 'action_type': {log_entry}")
#             skipped_entries_count += 1
#             continue

#         # Convert timestamp
#         timestamp_str = log_entry.get("timestamp")
#         converted_timestamp = convert_to_iso(timestamp_str)
#         if converted_timestamp is None:
#             log.warning(f"Skipping entry {i+1} due to invalid 'timestamp' format: '{timestamp_str}'. Entry: {log_entry}")
#             skipped_entries_count += 1
#             continue

#         try:
#             # Construct the cleaned log entry
#             clean_log = {
#                 "user_id": log_entry["user_id"],
#                 "action_type": log_entry["action_type"],
#                 "timestamp": converted_timestamp,
#                 "device": log_entry.get("device"),
#                 "location": log_entry.get("location"),
#                 # Add other fields as needed for your model
#             }
#             clean_data.append(clean_log)
#             log.debug(f"Successfully cleaned entry {i+1}.")
#         except KeyError as e:
#             # This handles cases where expected keys are missing despite initial checks,
#             # or if `log_entry` structure is unexpected.
#             log.error(f"Missing expected key during transformation for entry {i+1}: {e}. Entry: {log_entry}", exc_info=True)
#             skipped_entries_count += 1
#             continue
#         except Exception as e:
#             log.error(f"An unexpected error occurred during transformation of entry {i+1}: {log_entry}. Error: {e}", exc_info=True)
#             skipped_entries_count += 1
#             continue
    
#     log.info(f"Completed transformation. Successfully processed {len(clean_data)} entries. Skipped {skipped_entries_count} entries.")

#     # --- 3. Push Cleaned Data to XComs ---
#     ti.xcom_push(key='transformed_log_data', value=clean_data)
#     log.info(f"Pushed {len(clean_data)} cleaned entries to XComs with key 'transformed_log_data'.")
    
#     return clean_data

# def convert_to_iso(timestamp_str):
#     """
#     Converts a timestamp string to a datetime object. Returns None on failure.
#     """
#     if not timestamp_str:
#         log.debug("Received empty timestamp string for conversion.")
#         return None
#     try:
#         # Assuming timestamp_str is in ISO format, e.g., "2023-10-26T10:30:00"
#         return datetime.fromisoformat(timestamp_str)
#     except ValueError as e:
#         log.error(f"Failed to convert timestamp '{timestamp_str}' to ISO format: {e}", exc_info=False)
#         return None
#     except TypeError as e:
#         log.error(f"TypeError during timestamp conversion for '{timestamp_str}': {e}", exc_info=False)
#         return None



import logging
import os
from datetime import datetime

# Import your models from model.py
from database.model import Base, DimUsers, DimActions, DimDevices, DimLocations, FactUserActions

# Import database functions from db.py
from utils.db import get_session, get_engine # Only need get_session, as it internally calls get_engine

# Get a logger for this module
log = logging.getLogger(__name__)

# --- Helper functions for Dimension Tables (Get or Create) ---

def get_or_create_user(session, user_id):
    """Gets or creates a DimUsers entry and returns its user_key."""
    try:
        user = session.query(DimUsers).filter_by(user_id=user_id).one_or_none()
        if user:
            log.debug(f"Found existing user: {user_id} (key: {user.user_key})")
            return user.user_key
        else:
            new_user = DimUsers(user_id=user_id)
            session.add(new_user)
            session.flush() # Flush to get the generated primary key before commit
            log.info(f"Created new user: {user_id} (key: {new_user.user_key})")
            return new_user.user_key
    except IntegrityError:
        session.rollback()
        # This can happen in concurrent scenarios if another process creates the same user
        # between the query and the insert. Re-query to get the key.
        log.warning(f"IntegrityError for user_id '{user_id}'. Retrying to get existing user.")
        user = session.query(DimUsers).filter_by(user_id=user_id).one()
        return user.user_key
    except Exception as e:
        session.rollback()
        log.error(f"Error getting or creating user '{user_id}': {e}", exc_info=True)
        raise

def get_or_create_action(session, action_type):
    """Gets or creates a DimActions entry and returns its action_key."""
    try:
        action = session.query(DimActions).filter_by(action_type=action_type).one_or_none()
        if action:
            log.debug(f"Found existing action: {action_type} (key: {action.action_key})")
            return action.action_key
        else:
            new_action = DimActions(action_type=action_type)
            session.add(new_action)
            session.flush()
            log.info(f"Created new action: {action_type} (key: {new_action.action_key})")
            return new_action.action_key
    except IntegrityError:
        session.rollback()
        log.warning(f"IntegrityError for action_type '{action_type}'. Retrying to get existing action.")
        action = session.query(DimActions).filter_by(action_type=action_type).one()
        return action.action_key
    except Exception as e:
        session.rollback()
        log.error(f"Error getting or creating action '{action_type}': {e}", exc_info=True)
        raise

def get_or_create_device(session, device_type):
    """Gets or creates a DimDevices entry and returns its device_key."""
    if not device_type: # Handle cases where device might be null/empty
        return None
    try:
        device = session.query(DimDevices).filter_by(device_type=device_type).one_or_none()
        if device:
            log.debug(f"Found existing device: {device_type} (key: {device.device_key})")
            return device.device_key
        else:
            new_device = DimDevices(device_type=device_type)
            session.add(new_device)
            session.flush()
            log.info(f"Created new device: {device_type} (key: {new_device.device_key})")
            return new_device.device_key
    except IntegrityError:
        session.rollback()
        log.warning(f"IntegrityError for device_type '{device_type}'. Retrying to get existing device.")
        device = session.query(DimDevices).filter_by(device_type=device_type).one()
        return device.device_key
    except Exception as e:
        session.rollback()
        log.error(f"Error getting or creating device '{device_type}': {e}", exc_info=True)
        raise

def get_or_create_location(session, location_name):
    """Gets or creates a DimLocations entry and returns its location_key."""
    if not location_name: # Handle cases where location might be null/empty
        return None
    try:
        location = session.query(DimLocations).filter_by(location_name=location_name).one_or_none()
        if location:
            log.debug(f"Found existing location: {location_name} (key: {location.location_key})")
            return location.location_key
        else:
            new_location = DimLocations(location_name=location_name)
            session.add(new_location)
            session.flush()
            log.info(f"Created new location: {location_name} (key: {new_location.location_key})")
            return new_location.location_key
    except IntegrityError:
        session.rollback()
        log.warning(f"IntegrityError for location_name '{location_name}'. Retrying to get existing location.")
        location = session.query(DimLocations).filter_by(location_name=location_name).one()
        return location.location_key
    except Exception as e:
        session.rollback()
        log.error(f"Error getting or creating location '{location_name}': {e}", exc_info=True)
        raise

def clean_logs(ti=None):
    """
    Cleans and transforms raw log data, filtering out incomplete entries,
    converting timestamps, and resolving dimension keys by interacting with the database.

    Args:
        ti: The Airflow TaskInstance object, used to pull XComs.
            This function expects 'raw_logs' to be pulled from the 'extract' task.
    """
    log.info("Starting clean_logs transformation process.")

    # --- 1. Retrieve Data from Upstream Task ---
    if ti is None:
        log.error("Task Instance (ti) not provided to clean_logs. Cannot retrieve raw data.")
        raise ValueError("Airflow Task Instance (ti) is required for XCom pull.")

    # Pull data from the 'extract' task (assuming it was pushed to XComs)
    raw_logs = ti.xcom_pull(task_ids='pre_transform_quality_check')

    if not raw_logs:
        log.warning("No raw log data found from the 'extract' task. Transformation skipped.")
        ti.xcom_push(key='transformed_log_data', value=[]) # Push empty list to XCom
        return [] # Return an empty list if no data to process

    log.info(f"Received {len(raw_logs)} raw log entries for transformation.")
    fact_records_for_load = []
    skipped_entries_count = 0

    # --- 2. Establish Database Session using db.py ---
    db_session = None
    engine = None
    try:
        engine = get_engine() # Pass the connection string to get_engine
        log.info("Successfully obtained database engine.")
        db_session = get_session(engine) # Use the get_session from db.py

        # --- 3. Iterate and Transform Data ---
        for i, log_entry in enumerate(raw_logs):
            log.debug(f"Processing raw log entry {i+1}/{len(raw_logs)}: {log_entry}")

            # Basic Validation for essential fields
            user_id = log_entry.get("user_id")
            action_type = log_entry.get("action_type")
            timestamp_str = log_entry.get("timestamp")

            if not user_id:
                log.warning(f"Skipping entry {i+1} due to missing 'user_id': {log_entry}")
                skipped_entries_count += 1
                continue
            
            if not action_type:
                log.warning(f"Skipping entry {i+1} due to missing 'action_type': {log_entry}")
                skipped_entries_count += 1
                continue

            converted_timestamp = convert_to_iso(timestamp_str)
            if converted_timestamp is None:
                log.warning(f"Skipping entry {i+1} due to invalid 'timestamp' format: '{timestamp_str}'. Entry: {log_entry}")
                skipped_entries_count += 1
                continue

            # Extract metadata fields safely
            metadata = log_entry.get("metadata", {})
            device_type = metadata.get("device")
            location_name = metadata.get("location")

            try:
                # Get or create dimension keys
                user_key = get_or_create_user(db_session, user_id)
                action_key = get_or_create_action(db_session, action_type)
                device_key = get_or_create_device(db_session, device_type)
                location_key = get_or_create_location(db_session, location_name)

                # Commit dimension changes immediately to ensure keys are available for subsequent lookups
                # and to handle IntegrityErrors gracefully for each dimension.
                db_session.commit()

                # Construct the fact record dictionary
                fact_record = {
                    "user_key": user_key,
                    "action_key": action_key,
                    "device_key": device_key,
                    "location_key": location_key,
                    "timestamp": converted_timestamp,
                }
                fact_records_for_load.append(fact_record)
                log.debug(f"Successfully prepared fact record for entry {i+1}.")

            except Exception as e:
                db_session.rollback() # Rollback any pending changes for this entry
                log.error(f"An error occurred during dimension key resolution or fact record preparation for entry {i+1}: {log_entry}. Error: {e}", exc_info=True)
                skipped_entries_count += 1
                continue
        
        log.info(f"Completed transformation. Successfully prepared {len(fact_records_for_load)} fact records. Skipped {skipped_entries_count} entries.")

    except Exception as e:
        log.critical(f"Critical error during clean_logs execution: {e}", exc_info=True)
        if db_session:
            db_session.rollback() # Ensure rollback on critical errors
        raise # Re-raise to fail the Airflow task
    finally:
        if db_session:
            db_session.close() # Always close the session
            log.info("Database session closed.")

    # --- 4. Push Cleaned Data to XComs ---
    ti.xcom_push(key='transformed_log_data', value=fact_records_for_load)
    log.info(f"Pushed {len(fact_records_for_load)} fact records to XComs with key 'transformed_log_data'.")
    
    return fact_records_for_load

def convert_to_iso(timestamp_str):
    """
    Converts a timestamp string to a datetime object. Returns None on failure.
    """
    if not timestamp_str:
        log.debug("Received empty timestamp string for conversion.")
        return None
    try:
        # Assuming timestamp_str is in ISO format, e.g., "2023-10-26T10:30:00.123456"
        return datetime.fromisoformat(timestamp_str)
    except ValueError as e:
        log.error(f"Failed to convert timestamp '{timestamp_str}' to ISO format: {e}", exc_info=False)
        return None
    except TypeError as e:
        log.error(f"TypeError during timestamp conversion for '{timestamp_str}': {e}", exc_info=False)
        return None
