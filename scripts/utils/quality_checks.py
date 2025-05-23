def check_nulls(data, fields):
    return [row for row in data if any(row.get(field) is None for field in fields)]

def check_duplicates(data, key_fields):
    seen = set()
    duplicates = []
    for row in data:
        key = tuple(row[field] for field in key_fields)
        if key in seen:
            duplicates.append(row)
        seen.add(key)
    return duplicates
