import json
import logging
import pandas as pd

def split_marc_serials(**kwargs):
    """
    Split MARC Serials

    Called by the Airflow workflow to split MARC serial editions into individual records
    and rewrite the output JSON file.
    """
    collection = kwargs["collection"]
    data_file = collection.datafile("json")
    df = pd.DataFrame()

    data = read_datafile(data_file)
    for record in data:
        leader = record['leader']
        shared_fields = get_fields_from_record(record['fields'], exclude_fields=['974'])
        issue_fields = get_fields_from_record(record['fields'], include_fields=['974'])

        for issue in issue_fields:
            new_record = {
                'leader': leader,
                'fields': shared_fields + [issue]
            }
            df = pd.concat([df, pd.json_normalize(new_record)], ignore_index=True)

    df.to_json(data_file, orient="records", force_ascii=False)

    return data_file

def read_datafile(datafile):
    """
    Read a JSON data file and return JSON data

    :param datafile: The path to the JSON data file
    :return: JSON Object
    """
    try:
        with open(datafile, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        logging.warning(f"split_marc_serials->read_datafile: File not found: {datafile}")
        return None

def get_fields_from_record(fields, include_fields=None, exclude_fields=None):
    """
    Get fields from a MARC record

    :param record: MARC record (JSON)
    :param include_fields: Fields to include, if blank include all fields except exclude fields
    :param ignore_fields: Fields to ignore
    :return: List of fields
    """
    filtered_fields = []
    for field in fields:
        key = list(field.keys())[0]
        if exclude_fields and key in exclude_fields:
            next

        if include_fields:
            if key in include_fields:
                filtered_fields.append(field)
        else:
            filtered_fields.append(field)

    return filtered_fields
