import logging

# It's good practice to get a logger for the current module.
# In a real Airflow DAG, this 'log' object would be pre-configured
# by Airflow's logging system, making it consistent with Airflow logs.
log = logging.getLogger(__name__)

def check_nulls(data: list[dict], fields: list[str]) -> list[dict]:
    """
    Checks for records in the data where any of the specified fields are None.
    This function has been modified to return records that are *NOT* null
    in the specified fields.

    Args:
        data (list[dict]): The list of dictionaries (records) to check.
        fields (list[str]): A list of field names to check for null values.

    Returns:
        list[dict]: A list of records that DO NOT have null values in the specified fields.
    """
    log.info(f"Starting null check for {len(data)} records across fields: {fields}")
    
    # List to store records that are NOT null (i.e., valid records)
    not_null_records = []
    
    # List to store records that ARE null (for logging/warning)
    null_records_found = []

    for i, row in enumerate(data):
        # Check if ANY of the specified fields have a None value in the current row
        # using .get() to safely handle cases where a field might be missing from a row
        if any(row.get(field) is None for field in fields):
            null_records_found.append(row)
            log.debug(f"Null value found in record {i} for one of the fields {fields}. Record: {row}")
        else:
            not_null_records.append(row) # Add to the list of valid records

    if null_records_found:
        log.warning(f"Found {len(null_records_found)} record(s) with null values in specified fields. These will be excluded.")
    else:
        log.info("No null values found in the specified fields for any records. All records are valid.")
    
    # Return the records that DO NOT have nulls
    return not_null_records

def check_duplicates(data: list[dict], key_fields: list[str]) -> list[dict]:
    """
    Checks for duplicate records based on a combination of specified key fields.
    This function has been modified to return records that are *NOT* duplicates.

    Args:
        data (list[dict]): The list of dictionaries (records) to check.
        key_fields (list[str]): A list of field names whose combined values
                                will be used to identify duplicates.

    Returns:
        list[dict]: A list of records that are identified as NON-duplicates.
    """
    log.info(f"Starting duplicate check for {len(data)} records based on key fields: {key_fields}")
    seen_keys = set()
    non_duplicate_records = []
    duplicate_records_found = [] # For logging purposes

    for i, row in enumerate(data):
        # Create a tuple of values from the key_fields to use as a hashable key for the set
        try:
            key = tuple(row[field] for field in key_fields)
        except KeyError as e:
            log.error(f"Missing key field '{e}' in record {i}. Skipping this record for duplicate check. Record: {row}")
            continue # Skip record if a key field is missing

        if key in seen_keys:
            duplicate_records_found.append(row)
            log.debug(f"Duplicate found for key {key}. Record: {row}")
        else:
            seen_keys.add(key)
            non_duplicate_records.append(row) # Add to the list of non-duplicates
    
    if duplicate_records_found:
        log.warning(f"Found {len(duplicate_records_found)} duplicate record(s) based on key fields: {key_fields}. These will be excluded.")
    else:
        log.info("No duplicate records found based on the specified key fields. All records are unique.")
        
    # Return the records that are NOT duplicates
    return non_duplicate_records

# This is the function that will be called by the Airflow PythonOperator
# It orchestrates the data pulling and calls to check_nulls and check_duplicates.
def run_quality_checks(ti, **kwargs):
    """
    Airflow callable for the quality_check_task.
    Pulls data from upstream XComs and performs quality checks.
    Returns data that has passed both null and duplicate checks.

    Args:
        ti: The Airflow TaskInstance object, used to pull XComs.
        **kwargs: Contains 'key_fields_for_duplicates' passed via op_kwargs.
    """
    log.info("Starting run_quality_checks task.")

    # Data (dynamic): Pull from extract_task's XCom
    data_for_checks = ti.xcom_pull(task_ids='extract')

    # Key fields (static): From op_kwargs
    key_fields_for_duplicates = kwargs['key_fields_for_duplicates']

    if not data_for_checks:
        log.warning("No data received from upstream task for quality checks. Skipping checks.")
        return [] # Return empty list if no data

    log.info(f"Received {len(data_for_checks)} records for quality checks.")

    # Perform null checks: This now returns records without nulls
    null_check_fields = ['user_id', 'action_type', 'timestamp'] # These are critical fields
    
    # Filter out nulls first
    data_after_null_check = check_nulls(data_for_checks, null_check_fields)
    
    log.info(f"After null check, {len(data_after_null_check)} records remain.")

    # Perform duplicate checks on the data that passed the null check
    # This now returns records that are NOT duplicates
    final_cleaned_data = check_duplicates(data_after_null_check, key_fields_for_duplicates)

    log.info(f"Quality Check Summary: {len(data_for_checks) - len(data_after_null_check)} records removed due to nulls. "
             f"{len(data_after_null_check) - len(final_cleaned_data)} records removed due to duplicates. "
             f"Final {len(final_cleaned_data)} records passed all pre-transform checks.")
    
    # Return the final cleaned data so that the 'transform' task receives only valid, unique rows.
    return final_cleaned_data