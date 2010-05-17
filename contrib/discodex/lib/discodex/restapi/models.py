"""
Resources tied to models. A Model maps to a Collection of Model object (instance) resources.
"""

from django.http import HttpResponse, HttpResponseNotAllowed, Http404
from django.core.exceptions import ObjectDoesNotExist

from .resource import HttpResponseCreated
from .resource import Resource, Collection

class ModelObjectResource(Resource):
    """
    Abstract base class resource for wrapping the actual Model instance objects, as opposed to the Model class.
    """
    def __init__(self, model_obj, serdes):
        self.model_obj = model_obj
        self.serdes = serdes

    def create(self, request, *args, **kwargs):
        # explicitly disallow instead of defaulting to 501
        return HttpResponseNotAllowed(self.responses)

    def read(self, request, *args, **kwargs):
        return HttpResponse('%s' % self.serdes.serialize(self.model_obj))

    def delete(self, request, *args, **kwargs):
        self.model_obj.delete()
        return HttpResponse('Successfully deleted object.')


# We might want to use the model's primary key to make some more educated guesses
class ModelCollection(Collection):
    def __init__(self, model, serdes, model_obj_resource=ModelObjectResource):
        self.model = model
        self.model_obj_resource = model_obj_resource
        self.serdes = serdes

    def delegate(self, request, *args, **kwargs):
        try:
            return self.model_obj_resource(self.model.objects.get(**kwargs), self.serdes)(request, *args, **kwargs)
        except AssertionError:
            return self.relegate(request, *args, **kwargs)
        except ObjectDoesNotExist:
            raise Http404

    def create(self, request, *args, **kwargs):
        self.serdes.deserialize(request.raw_post_data).save()
        return HttpResponseCreated('%s/%s' % (request.build_absolute_uri(), ))

    def read(self, request, *args, **kwargs):
        return HttpResponse('%s' % self.serdes.serialize_all(self.model.objects.filter(**kwargs)))

    def delete(self, request, *args, **kwargs):
        self.model.objects.filter(**kwargs).delete()
        return HttpResponse('Successfully deleted collection.')

class ModelObjectList(list):
    """
    Wrapper class to form atomic groups of model objects.
    Useful for a deserialize to return so we can do an atomic save.
    """
    def save(self):
        for model_obj in self:
            model_obj.save()
