import json
import logging

log = logging.getLogger(__name__)

def read_json_log(file_path: str='/opt/airflow/dags/raw_data/raw_logs.json'): # Corrected default path
    """
    Reads a JSON file that contains a single JSON array of objects.
    Assumes the entire file is a valid JSON array.
    """
    log.info(f"Starting read_json_log for file: {file_path}")

    try:
        with open(file_path, 'r') as f:
            # Read the entire file content
            file_content = f.read()

        if not file_content.strip():
            log.warning(f"File {file_path} is empty or contains only whitespace.")
            return []

        try:
            # Parse the entire file content as a single JSON object (which should be an array)
            parsed_data = json.loads(file_content)

            # Ensure the parsed data is indeed a list (JSON array)
            if not isinstance(parsed_data, list):
                log.error(f"File {file_path} does not contain a JSON array as expected. Found type: {type(parsed_data)}")
                raise ValueError("Expected a JSON array in the file.")
            
            log.info(f"Successfully parsed {len(parsed_data)} JSON objects from {file_path}.")
            if not parsed_data:
                log.warning(f"The JSON array in {file_path} is empty.")
            return parsed_data

        except json.JSONDecodeError as e:
            log.critical(f"JSON decoding error for the entire file {file_path}: {e}. "
                         f"The file might not be a valid single JSON array.", exc_info=True)
            raise # Re-raise to ensure the Airflow task fails explicitly if the file is invalid JSON

    except FileNotFoundError:
        log.critical(f"Error: File not found at {file_path}. Please ensure the file exists and the volume mount is correct.", exc_info=True)
        raise # Re-raise to ensure the Airflow task fails explicitly

    except Exception as e:
        log.critical(f"An unexpected error occurred in read_json_log while processing {file_path}: {e}", exc_info=True)
        raise # Re-raise to ensure the Airflow task fails explicitly