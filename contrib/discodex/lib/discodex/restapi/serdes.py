"""
Serializers/Deserializers are the main tools for automatic RESTful APIs.
Users can use the builtins automagically or define their own.
"""

from django.core import serializers

class SerDes(object):
    """
    Base class for a serializer/deserializer. Methods are implemented just for demonstration, should use a subclass.
    """
    opener, separator, closer = ('[', ', ', ']')

    def serialize(self, obj):
        return '%r' % obj

    def serialize_all(self, obj_iter):
        return '%s%s%s' % (self.opener, self.separator.join([self.serialize(obj) for obj in obj_iter]), self.closer)

    def deserialize(self, string):
        return eval(string)

    def deserialize_all(self, composite_string):
        return (deserialize(string) for string in composite_string.lstrip(self.opener).rstrip(self.closer).split(self.separator))


class BuiltinSerDes(SerDes):
    def __init__(self, format, fields=None):
        self.format = format
        self.fields = fields

    def serialize(self, obj):
        return serializers.serialize(self.format, [obj], fields=self.fields)

    def serialize_all(self, obj_iter):
        return serializers.serialize(self.format, obj_iter, fields=self.fields)

    def deserialize(self, string):
        deserialized_objects = list(serializers.deserialize(self.format, string))
        assert len(deserialized_objects) == 1
        return deserialized_objects.pop()

    def deserialize_all(self, composite_string):
        return serializers.deserialize(self.format, composite_string)

# Convenience singletons
XMLSerDes = BuiltinSerDes('xml')
JSONSerDes = BuiltinSerDes('json')
