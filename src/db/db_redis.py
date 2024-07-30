# export PYTHONPATH=.
import sys, os
sys.path.insert(0, os.getcwd())
import src.custom_logger.custom_logger as custom_logger

import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def find_xdr(key: str):
    try:
        result = r.get(key)
        return result
    except redis.exceptions.ConnectionError:
        logger.debug('No connection to Redis')
        exit(1)

def set_xdr(key: str, xdr: str):
    try:
        r.set(key, xdr)
    except redis.exceptions.ConnectionError:
        logger.debug('No connection to Redis')
        exit(1)

def remove_xdr(key: str):
    try:
        r.delete(key)
    except redis.exceptions.ConnectionError:
        logger.debug('No connection to Redis')
        exit(1)

if __name__ == "__main__":
    find_xdr('toto')
