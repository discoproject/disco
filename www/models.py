import errno, os

from django.db import models
from django.http import Http404, HttpResponse, HttpResponseServerError

from restapi.resource import Resource, Collection
from restapi.resource import (HttpResponseAccepted,
                              HttpResponseCreated,
                              HttpResponseNoContent,
                              HttpResponseServiceUnavailable)

from discodex import settings
from discodex.mapreduce import Indexer, Queryer, KeyIterator, ValuesIterator, ItemsIterator
from discodex.objects import DataSet, Indices, Index, Keys, Values, Items, Query

from disco.core import Disco
from disco.error import DiscoError
from disco.util import flatten, parse_dir

from discodb import Q

discodex_settings = settings.DiscodexSettings()
disco_master      = discodex_settings['DISCODEX_DISCO_MASTER']
disco_prefix      = discodex_settings['DISCODEX_DISCO_PREFIX']
purge_file        = discodex_settings['DISCODEX_PURGE_FILE']
index_root        = discodex_settings.safedir('DISCODEX_INDEX_ROOT')
index_temp        = discodex_settings.safedir('DISCODEX_INDEX_TEMP')
disco_master      = Disco(disco_master)

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
        dataset = DataSet.loads(request.raw_post_data)
        try:
            job = Indexer(dataset).run(disco_master, disco_prefix)
        except ImportError, e:
            return HttpResponseServerError("Callable object not found: %s" % e)
        except DiscoError, e:
            return HttpResponseServerError("Failed to run indexing job: %s" % e)
        return HttpResponseAccepted(job.name)

    def read(self, request, *args, **kwargs):
        return HttpResponse(Indices(self.names).dumps())

class IndexResource(Collection):
    allowed_methods = ('GET', 'PUT', 'DELETE')

    def __init__(self, name):
        self.name = name

    def delegate(self, request, *args, **kwargs):
        if self.status == NOT_FOUND:
            raise Http404
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
    def ichunks(self):
        return Index.loads(open(self.path).read()).ichunks

    @property
    def keys(self):
        return KeysResource(self)

    @property
    def values(self):
        return ValuesResource(self)

    @property
    def items(self):
        return ItemsResource(self)

    @property
    def query(self):
        return QueryCollection(self)

    @property
    def status(self):
        if self.exists:
            return OK

        if self.isdisco:
            status, results = disco_master.results(self.name)
            if self.exists:
                return OK

            if status == OK:
                ichunks = list(flatten(parse_dir(result) for result in results))
                self.write(Index(id=self.name, ichunks=ichunks))
                disco_master.clean(self.name)
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
        if self.isdisco:
            disco_master.purge(self.name)
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
                disco_master.purge(self.name)
        return HttpResponseNoContent()

    def write(self, index):
        from tempfile import mkstemp
        fd, filename = mkstemp(dir=index_temp)
        os.write(fd, index.dumps())
        os.rename(filename, self.path)

class DiscoDBResource(Resource):
    result_type = Keys

    def __init__(self, index):
        self.index = index

    @property
    def job(self):
        return KeyIterator(self.index.ichunks, self.mapfilters, self.reducefilters)

    def read(self, request, *args, **kwargs):
        try:
            self.mapfilters    = filter(None, kwargs.pop('mapfilters').split('|'))
            self.reducefilters = filter(None, kwargs.pop('reducefilters').split('}'))
            job = self.job.run(disco_master, disco_prefix)
        except DiscoError, e:
            return HttpResponseServerError("Failed to run DiscoDB job: %s" % e)

        try:
            results = self.result_type(job.results).dumps()
        except DiscoError, e:
            return HttpResponseServerError("DiscoDB job failed: %s" % e)
        finally:
            if os.path.exists(purge_file):
                disco_master.purge(job.name)

        return HttpResponse(results)

class KeysResource(DiscoDBResource):
    pass

class ValuesResource(DiscoDBResource):
    result_type = Values

    @property
    def job(self):
        return ValuesIterator(self.index.ichunks, self.mapfilters, self.reducefilters)


class ItemsResource(DiscoDBResource):
    result_type = Items

    @property
    def job(self):
        return ItemsIterator(self.index.ichunks, self.mapfilters, self.reducefilters)

class QueryCollection(Collection):
    allowed_methods = ('GET', 'POST')

    def __init__(self, index):
        self.index = index

    def delegate(self, request, *args, **kwargs):
        query_path = str(kwargs.pop('query_path'))
        return QueryResource(self.index, query_path)(request, *args, **kwargs)

    def read(self, request, *args, **kwargs):
        return HttpResponse(Values().dumps())

    def create(self, request, *args, **kwargs):
        query = Query.loads(request.raw_post_data)
        return QueryResource(self.index, query['query_path']).read(request, *args, **kwargs)

class QueryResource(DiscoDBResource):
    result_type = Values

    def __init__(self, index, query_path):
        self.index = index
        self.query = Q.urlscan(query_path)

    @property
    def job(self):
        return Queryer(self.index.ichunks, self.mapfilters, self.reducefilters, self.query)
