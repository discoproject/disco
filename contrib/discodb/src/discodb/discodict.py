from cPickle import dump, dumps, load, loads
from discodb.tools import normalize

class DiscoDict(dict):
    def __init__(self, iter=(), unique_items=False):
        container = set if unique_items else tuple
        if hasattr(iter, 'items'):
            iter = iter.items()
        super(DiscoDict, self).__init__((k, container(vs))
                                        for k, vs in normalize(iter))

    def peek(self, key, default=None):
        try:
            return iter(self[key]).next()
        except (KeyError, StopIteration):
            return default

    def unique_values(self):
        return set(v for vs in self.itervalues() for v in vs)

    def dump(self, handle):
        return dump(self, handle)

    def dumps(self):
        return dumps(self)

    @classmethod
    def load(cls, handle):
        return load(handle)

    @classmethod
    def loads(cls, string):
        return loads(string)
