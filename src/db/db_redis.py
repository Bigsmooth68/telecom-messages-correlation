import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def find_xdr(key: str):
    result = r.get(key)
    return result

def set_xdr(key: str, xdr: str):
    r.set(key, xdr)

def remove_xdr(key: str):
    r.delete(key)

if __name__ == "__main__":
    find_xdr('toto')
