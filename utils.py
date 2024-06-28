from contextlib import contextmanager

from datetime import datetime
from loguru import logger

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
import csv
import os
def write_to_csv(file_path, data):
    with file_path.open('a+', newline='') as file:
        # file.seek(0, os.SEEK_END)  # Move the cursor to the end of the file
        writer = csv.writer(file)
        is_empty = file.tell() == 0
        if is_empty:
            header = data[0].keys()
            writer.writerow(header)

        for row in data:
            writer.writerow(row.values())