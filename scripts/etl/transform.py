from datetime import datetime

def clean_logs(row_logs):
    clean_data = []

    for log in row_logs:
        if not log.get("user_id") or not log.get("action_type"):
            continue
        clean_log = {
            "user_id": log["user_id"],
            "action_type": log["action_type"],
            "timestamp": convert_to_iso(log["timestamp"]),
            "device": log.get("device"),
            "location": log.get("location"),
        }

        clean_data.append(clean_log)
    
    return clean_data

def convert_to_iso(timestamp_str):
    try:
        return datetime.fromisoformat(timestamp_str)
    except ValueError:
        return None
    