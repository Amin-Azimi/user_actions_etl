import json

def read_json_log(file_path: str):
    with open(file_path,'r') as f:
        return [json.load(line.strip()) for line in f if line.strip()]