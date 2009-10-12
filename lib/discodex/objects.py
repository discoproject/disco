from django.utils import simplejson as json

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
