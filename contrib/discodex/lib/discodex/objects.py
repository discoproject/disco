"""
:mod:`discodex.objects` -- objects passed to and from the discodex server
=========================================================================
"""
import sys

from discodex import json
from discodex.mapreduce import parsers, demuxers, balancers
from discodex.mapreduce.func import reify

class JSONSerialized(object):
    required_fields = []

    @classmethod
    def loads(cls, string):
        self = cls(json.loads(string))
        for field in self.required_fields:
            if not field in self:
                raise TypeError("Required field '%s' missing" % field)
        return self

    def dumps(self):
        return json.dumps(self, default=str)

    def response(self, request):
        from django.http import HttpResponse
        return HttpResponse(self.dumps(), content_type='application/json')

    def __getcallable__(self, module, name):
        if hasattr(module, name):
            return getattr(module, name)
        return reify(name)

class Dict(dict, JSONSerialized):
    pass

class DataSet(dict, JSONSerialized):
    """Specifies a collection of inputs to be indexed and how to index them."""
    required_fields = ['input']

    @property
    def options(self):
        return self.get('options', {})

    @property
    def nr_ichunks(self):
        return int(self.options.get('nr_ichunks', 1))

    @property
    def input(self):
        from disco.util import iterify
        return [[str(url) for url in iterify(input)] for input in self['input']]

    @property
    def stream(self):
        from disco.func import default_stream
        return [reify(stream) for stream in self.options.get('streams', ())] or default_stream

    @property
    def parser(self):
        return self.__getcallable__(parsers, self.options.get('parser', 'rawparse'))

    @property
    def demuxer(self):
        return self.__getcallable__(demuxers, self.options.get('demuxer', 'nodemux'))

    @property
    def balancer(self):
        return self.__getcallable__(balancers, self.options.get('balancer', 'nchunksbalance'))

    @property
    def profile(self):
        return bool(self.options.get('profile', False))

    @property
    def required_files(self):
        return self.options.get('required_files', ())

    @property
    def unique_items(self):
        return bool(self.options.get('unique_items', False))

class IChunks(list, JSONSerialized):
    pass

class Indices(list, JSONSerialized):
    pass

class Index(dict, JSONSerialized):
    """A collection of `ichunks` resulting from a :class:`DataSet` previously indexed by discodex."""
    @property
    def ichunks(self):
        return IChunks([ichunk for ichunk in self['urls']])

class Results(list, JSONSerialized):
    pass
