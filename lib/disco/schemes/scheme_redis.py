"""
This scheme uses the redis-py redis library.  It can be installed it with
$ pip install redis.
"""
from disco.util import schemesplit
import redis

class RedisStream(file):
    def __init__(self, url):
        _redis_scheme, rest = schemesplit(url)
        host, port, dbid = rest.split(":")
        self.redis = redis.StrictRedis(host=host, port=port, db=dbid)
        self.cursor = '0'
        self.url = url

    def __iter__(self):
        List = self.read()
        for k, v in List:
            yield k, v

    def __len__(self):
        """
        For redis scheme, it returns the number of keys in the db.
        """
        return self.redis.dbsize()

    def read(self, num_bytes=None):
        if self.cursor == None:
            raise EOFError
        [self.cursor, ret] = self.redis.scan(self.cursor, count = 100)
        if self.cursor == '0':
            self.cursor = None
        List = []
        for k in ret:
            vs = self.redis.lrange(k, 0, -1)
            for v in vs:
                List.append((k, v))
        return List

    def add(self, key, value):
        self.redis.lpush(key, value)

def input_stream(fd, size, url, params):
    file = RedisStream(url)
    return file, len(file), file.url

def redis_output_stream(stream, partition, url, params):
    file = RedisStream(params['url'])
    return file, len(file), file.url

class AtomicDict(file):
    def __init__(self, path, redis_url):
        _redis_scheme, rest = schemesplit(redis_url)
        host, port, dbid = rest.split(":")
        self.redis = redis.StrictRedis(host=host, port=port, db=dbid)
        self.redis.flushdb()
        self.cursor = '0'
        self.url = redis_url
        from collections import defaultdict
        self.Dict = defaultdict(list)
        super(AtomicDict, self).__init__(path, 'w')
        self.write(redis_url + "\n")

    def add(self, k, v):
        self.Dict[k].append(v)

    def close(self):
        for k, v in self.Dict.items():
            self.redis.lpush(k, *v)
        super(AtomicDict, self).close()

def redis_inter_stream_out(stream, partition, url, params, redis_server):
    return AtomicDict(url, redis_server(partition, url, params))

def redis_inter_stream_in(stream, partition, url, params):
    path = stream.read(1024).strip()
    file = RedisStream(path)
    return file, len(file), file.url
