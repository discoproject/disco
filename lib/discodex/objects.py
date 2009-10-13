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
        __import__(self['parser'])
        return sys.modules[self['parser']]

    @property
    def demuxer(self):
        __import__(self['demuxer'])
        return sys.modules[self['demuxer']]

    @property
    def balancer(self):
        __import__(self['balancer'])
        return sys.modules[self['balancer']]

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
