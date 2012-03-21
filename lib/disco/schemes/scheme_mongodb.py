import threading

from pymongo import Connection
from bson import json_util
import json

cursor_cache = {}

def open(url):
    """Return cached cursor to collection
    """
    cache = cursor_cache.setdefault(threading.currentThread(), {})
    scheme, netloc, path = util.urlsplit(url)
    (db_name, collection_name, query) = path.split('/')
    query = json.loads(query, object_hook=json_util.object_hook)
    query = query.get('$query', {})
    if collection_name in cache:
        return cache[collection_name].find(query)

    conn = Connection(netloc.host, int(netloc.port))
    cursor = conn[db_name][collection_name]
    return cache.setdefault(collection_name, cursor).find(query)
    

def input_stream(fd, size, url, params):
    """Opens the url locally on the node."""
    return open(url)

