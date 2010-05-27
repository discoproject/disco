"""
:mod:`discodex.objects` -- objects passed to and from the discodex server
=========================================================================
"""
import sys

from discodex import json
from discodex.mapreduce import parsers, demuxers, balancers, metakeyers
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

    def __getcallable__(self, module, name):
        if hasattr(module, name):
            return getattr(module, name)
        return reify(name)

class DataSet(dict, JSONSerialized):
    """Specifies a collection of inputs to be indexed and how to index them."""
    required_fields = ['input']

    @property
    def options(self):
        return self.get('options', {})

    @property
    def nr_ichunks(self):
        return self.options.get('nr_ichunks', 1)

    @property
    def input(self):
        return [str(input) for input in self['input']]

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
    def sort(self):
        return not bool(self.options.get('no_sort', False))

    @property
    def k_viter(self):
        return bool(self.options.get('k_viter', False))

class Indices(list, JSONSerialized):
    pass

class Index(dict, JSONSerialized):
    """A collection of `ichunks` resulting from a :class:`DataSet` previously indexed by discodex."""
    @property
    def ichunks(self):
        return [ichunk for ichunk in self['urls']]

class MetaSet(Index):
    """
    An :class:`Index` together with a `metakeyer` function used to describe how the index should be metaindexed.

    See :mod:`discodex.mapreduce.metakeyers` and :class:`discodex.mapreduce.MetaIndexer`.
    """
    required_fields = ['urls']

    @property
    def options(self):
        return self.get('options', {})

    @property
    def metakeyer(self):
        return self.__getcallable__(metakeyers, self.options.get('metakeyer', 'prefixkeyer'))

class Results(list, JSONSerialized):
    pass

class Query(dict, JSONSerialized):
    pass
