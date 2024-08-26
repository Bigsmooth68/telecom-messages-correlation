import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def find_xdr(key: str):
    """
    Find xDR in redis DB.

    :param str key: The key to look for
    :return: Found result
    """
    result = r.get(key)
    return result

def set_xdr(key: str, xdr: str):
    """
    Set the xDR to the given key in the redis DB.

    :param str key: The key to update/set
    :param str xdr: The value to associate to the key

    """
    r.set(key, xdr)

def remove_xdr(key: str):
    """
    Remove the key and associated value from redis DB.

    :param str key: The key to remove
    """
    r.delete(key)

if __name__ == "__main__":
    find_xdr('toto')
