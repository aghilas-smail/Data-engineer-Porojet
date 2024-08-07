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
    while true:
        url = URL_API.format(last_processed_timestamp, n_results)