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
def supress():
    try:
        yield
    except Exception as e:
        logger.exception(f"{exchange} failed with {e}")
        return