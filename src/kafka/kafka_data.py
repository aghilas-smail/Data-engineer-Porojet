# Get and stream the data.
# install kafka-python with this commande : pip install kafka-python

import kafka.errors


from src.data import(
    URL_API,
    Path,
    DB_FIELDS,
    Limit,
    offsetlimit,
)


import json
import datetime
import requests # pip install requests
from kafka import KafkaProducer
from typing import List
import logging


# def to get the lastest timestamp.

def get_latest_timestamp():
    """
    Gets the latest timestamp from the 
    """
    
    with open(Path, "r") as file: # Open the json file
        data = json.load(file)
        if "last_processed" in data:
            return data["last_processed"]
        else:
            return datetime.datetime.min
        
# Def to update the json file with the lastest timestamp

def update_last_processed_json(data: List[dict]):
    publication_dates_as_timestamps = [
        datetime.datetime.strftime(row["date_de_publication"], "%Y-%m-%d")
        for row in data
        
    ]
    last_processed = max(publication_dates_as_timestamps)
    last_processed_as_string = last_processed.strftime("%Y-%m-%d")
    with open(Path, "w") as file:
        json.dump({"last_processes": last_processed_as_string}, file)
        
        
def get_all_data(last_processed_timestamp: datetime.datetime) ->List[dict]:
    n_results = 0
    full_data = []
    while True:
        url = URL_API.format(last_processed_timestamp, n_results)
        response = requests.get(url)
        data = response.json()
        current_results = data["results"]
        full_data.extend(current_results)
        n_results += len(current_results)
        n_results = min(offsetlimit, n_results)
        if len(current_results) < Limit:
            break # We stop the execution.
    logging.info(f"Got {n_results} result from the API !!")
    
    return full_data

def deduplicate_data(data: List[dict]) -> List[dict]:
    return list({v["reference_fiche"]: v for v in data}.values())


def get_data_with_query() -> List[dict]:
    """
    Returns :
        List[dict]: A list of dictionaries containing the unique data retrieved from the API.
    """
    last_processed = get_latest_timestamp()
    full_data = get_all_data(last_processed)
    full_data = deduplicate_data(full_data)
    if full_data:
        update_last_processed_json(full_data)
    return full_data

def process_data(row):
    # get and process the data
    data_kafka = {}
    for i in DB_FIELDS:
        data_kafka[i] = row.get(i, None)
    return data_kafka

def create_kafka_producer():
    """
    create the kafka procedure object
    """
    
    try:
        procedur = KafkaProducer(bootstrap_servers=["kafak:9092"])
    except kafka.errors.NoBrokersAvailable:
        logging.info(
            "test"
        )
        procedur = KafkaProducer(bootstrap_servers=["localhost:9094"])
    
    return procedur


def stream():
    producer = create_kafka_producer()
    results = get_data_with_query()
    kafka_data_full = map(process_data, results)
    for kafka_data in kafka_data_full:
        producer.send("rappel_conso", json.dumps(kafka_data).encode("utf-8"))
      
  
if __name__ == "__main__":
    stream()      