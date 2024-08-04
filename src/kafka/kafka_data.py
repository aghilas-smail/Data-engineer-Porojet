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
        datetime.
    ]