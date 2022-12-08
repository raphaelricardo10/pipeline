""" This is the Cloud function (Flask) entrypoint """
import json
import base64
from google.cloud import bigquery

""" Global variables"""
bq_project_name = "studious-matrix-355222"
bq_dataset_name = "studious-matrix-355222.test_dataset"
bq_table_name = "authors"
bq_table_full_path = f"""{bq_project_name}.{bq_dataset_name}.{bq_table_name}"""
bq_client = bigquery.Client(bq_project_name)


def decode_msg(event: str) -> dict:
    """
    Scope:  Extract data from pub/sub payload and decoded, saving it in a dict
    Input:  The raw event data
    Output: The decoded message in a dictionary format
    """
    message = event.get("data")
    decoded_message = json.loads(base64.b64decode(message).decode("utf-8"))
    return decoded_message


def write_to_bigquery(message: dict):
    """
    Scope:  Write the message in bigquery table
    Input:  The message
    Output: The restult of the client respons (can make error handling base on that)
    """
    rows_to_insert = [
        {"id": message["id"], "title": message["title"], "author": message["author"]}
    ]
    bq_client.insert_rows_json(bq_table_full_path, rows_to_insert)


def pipeline(event: dict, context: dict):
    message = decode_msg(event)
    write_to_bigquery(message)
    return 200, {"status": "success"}
