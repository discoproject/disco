import sys

from django.utils import simplejson as json
from discodex.mapreduce import parsers, demuxers, balancers

class JSONSerialized(object):
    @classmethod
    def loads(cls, string):
        return cls(json.loads(string))

    def dumps(self):
        return json.dumps(self)

class DataSet(dict, JSONSerialized):
    @property
    def nr_ichunks(self):
        return self.get('nr_ichunks', 10)

    @property
    def input(self):
        return [str(input) for input in self['input']]

    @property
    def parser(self):
        return self.__getcallable__(parsers, self['parser'])

    @property
    def demuxer(self):
        return self.__getcallable__(demuxers, self['demuxer'])

    @property
    def balancer(self):
        return self.__getcallable__(balancers, self['balancer'])

    def __getcallable__(self, module, name):
        if hasattr(module, name):
            return getattr(module, name)
        __import__(name)
        return sys.modules[name]

class Indices(list, JSONSerialized):
    pass

class Index(dict, JSONSerialized):
    @property
    def ichunks(self):
        return [str(ichunk) for ichunk in self['ichunks']]

class Keys(list, JSONSerialized):
    pass

class Values(list, JSONSerialized):
    pass

class Query(dict, JSONSerialized):
    pass
