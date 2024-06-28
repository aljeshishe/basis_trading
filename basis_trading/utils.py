from contextlib import contextmanager
import csv
import os

from datetime import datetime
from loguru import logger
import requests


def dt2ts(dt):
    return int(dt.timestamp() * 1000)

def ts2dt(ts):
    return datetime.fromtimestamp(ts / 1000)
def str2dt(s):
    return datetime.fromisoformat(s)
def dt2str(dt):
    return dt.isoformat()



@contextmanager
def supress(exchange_id):
    try:
        yield
    except Exception as e:
        logger.exception(f"{exchange_id} failed with {e}")
        return


def write_to_csv(file_path, data):
    is_empty = not file_path.exists() 
    with file_path.open("a") as file:
        writer = csv.writer(file)
        if is_empty:
            header = data.keys()
            writer.writerow(header)

        writer.writerow(data.values())
        
        
def message(msg):
    apiToken = '5803765903:AAH2ayWpVcook4JpoiHvMzgOvJCjsLItcmw'
    chatID = '99044115'
    apiURL = f'https://api.telegram.org/bot{apiToken}/sendMessage'

    response = requests.post(apiURL, json={'chat_id': chatID, 'text': msg})
    response.raise_for_status()
