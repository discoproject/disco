import errno, os

from django.db import models
from django.http import Http404, HttpResponse, HttpResponseServerError
from django.utils import simplejson

from restapi.resource import Resource, Collection
from restapi.resource import (HttpResponseAccepted,
                              HttpResponseCreated,
                              HttpResponseNoContent,
                              HttpResponseServiceUnavailable)

from discodex import settings, parsers
from discodex.objects import DataSet, Indices, Index

from disco.core import Disco
from disco.error import DiscoError

discodex_settings = settings.DiscodexSettings()
disco_master      = discodex_settings['DISCODEX_DISCO_MASTER']
disco_prefix      = discodex_settings['DISCODEX_DISCO_PREFIX']
index_root        = discodex_settings.safedir('DISCODEX_INDEX_ROOT')
disco             = Disco(disco_master)

NOT_FOUND, OK, ACTIVE, DEAD = 'unknown job', 'ready', 'active', 'dead'

class IndexCollection(Collection):
    allowed_methods = ('GET', 'POST')

    def delegate(self, request, *args, **kwargs):
        name = str(kwargs.pop('name'))
        return IndexResource(name)(request, *args, **kwargs)

    @property
    def names(self):
        return os.listdir(index_root)

    def __iter__(self):
        for name in self.names:
            yield IndexResource(name)

    def create(self, request, *args, **kwargs):
        from uuid import uuid1
        dataset = DataSet.loads(request.raw_post_data)
        name    = ('%s%s' % (disco_prefix, uuid1())).replace('-', '')
        try:
            job = disco.new_job(name=name,
                                input=['http://localhost:8080/'],
                                map=parsers.map)
            # start job...
        except DiscoError, e:
            # failed to submit job
            return HttpResponseServerError('%s: %s' % (name, e))
        return HttpResponseAccepted(name)

    def read(self, request, *args, **kwargs):
        return HttpResponse(Indices(self.names).dumps())

class IndexResource(Collection):
    allowed_methods = ('GET', 'PUT', 'DELETE')

    def __init__(self, name):
        self.name = name

    def delegate(self, request, *args, **kwargs):
        property = str(kwargs.pop('property'))
        return getattr(self, property)(request, *args, **kwargs)

    @property
    def exists(self):
        return os.path.exists(self.path)

    @property
    def isdisco(self):
        return self.name.startswith(disco_prefix)

    @property
    def path(self):
        return os.path.join(index_root, self.name)

    @property
    @models.permalink
    def url(self):
        return 'index', (), {'name': self.name}

    @property
    def keys(self):
        return Keys(self)

    @property
    def values(self):
        return Values(self)

    @property
    def status(self):
        if self.exists:
            return OK

        if self.isdisco:
            status, results = disco.results(self.name)
            if self.exists:
                return OK

            if status == OK:
                self.write(Index(ichunks=results))
                disco.clean(self.name)
            return status
        return NOT_FOUND

    def read(self, request, *args, **kwargs):
        status = self.status
        if status == OK:
            return HttpResponse(open(self.path))
        if status == ACTIVE:
            return HttpResponseServiceUnavailable(100)
        if status == DEAD:
            return HttpResponseServerError("Indexing failed.")
        raise Http404

    def update(self, request, *args, **kwargs):
        index = Index.loads(request.raw_post_data)
        self.write(index)
        return HttpResponseCreated(self.url)

    def delete(self, request, *args, **kwargs):
        try:
            os.remove(self.path)
        except OSError, e:
            if e.errno == errno.ENOENT:
                raise Http404
            raise
        else:
            if self.isdisco:
                disco.purge(self.name)
        return HttpResponseNoContent()

    def write(self, index):
        from tempfile import NamedTemporaryFile
        with NamedTemporaryFile(delete=False) as handle:
            handle.write(index.dumps())
        os.rename(handle.name, self.path)

class Keys(Resource):
    def __init__(self, index):
        self.index = index

    def read(self, request, *args, **kwargs):
        return HttpResponse('index %s keys' % self.index.name)

class Values(Collection):
    def __init__(self, index):
        self.index = index

    def delegate(self, request, *args, **kwargs):
        query_path = str(kwargs.pop('query_path'))
        return QueryResult(query_path)(request, *args, **kwargs)

    def read(self, request, *args, **kwargs):
        return HttpResponse('index %s values' % self.index.name)

class QueryResult(Resource):
    def __init__(self, query_path):
        self.query_path = query_path

    def read(self, request, *args, **kwargs):
        return HttpResponse('query')
