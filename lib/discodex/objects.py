import json

class JSONSerialized(object):
    @classmethod
    def loads(cls, string):
        return cls(json.loads(string))

    def dumps(self):
        return json.dumps(self)

class DataSet(dict, JSONSerialized):
    pass

class Indices(list, JSONSerialized):
    pass

class Index(dict, JSONSerialized):
    pass

class Keys(list, JSONSerialized):
    pass

class Values(list, JSONSerialized):
    pass
