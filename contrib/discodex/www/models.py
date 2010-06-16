import errno, os

from django.db import models
from django.http import Http404, HttpResponseServerError

from discodex.restapi.resource import Resource, Collection
from discodex.restapi.resource import (HttpResponseAccepted,
                                       HttpResponseCreated,
                                       HttpResponseNoContent,
                                       HttpResponseServiceUnavailable)

from discodex import settings
from discodex.mapreduce import (Indexer,
                                MetaIndexer,
                                Queryer,
                                DiscoDBIterator,
                                KeyIterator,
                                ValuesIterator,
                                ItemsIterator)
from discodex.objects import (DataSet,
                              MetaSet,
                              Indices,
                              Index,
                              Results,
                              Query)

from disco.core import Disco
from disco.ddfs import DDFS
from disco.error import DiscoError
from disco.util import ddfs_name, flatten, parse_dir

from discodb import Q

discodex_settings = settings.DiscodexSettings()
disco_master_url  = discodex_settings['DISCODEX_DISCO_MASTER']
disco_prefix      = discodex_settings['DISCODEX_DISCO_PREFIX']
index_prefix      = discodex_settings['DISCODEX_INDEX_PREFIX']
purge_file        = discodex_settings['DISCODEX_PURGE_FILE']
disco_master      = Disco(disco_master_url)
ddfs              = DDFS(disco_master_url)

NOT_FOUND, OK, ACTIVE, DEAD = 'unknown job', 'ready', 'active', 'dead'

class IndexCollection(Collection):
    allowed_methods = ('GET', 'POST')

    def delegate(self, request, *args, **kwargs):
        name = str(kwargs.pop('name'))
        return IndexResource(name)(request, *args, **kwargs)

    @property
    def names(self):
        return ddfs.list(index_prefix)

    def __iter__(self):
        for name in self.names:
            yield IndexResource(name)

    def create(self, request, *args, **kwargs):
        try:
            dataset = DataSet.loads(request.raw_post_data)
            prefix  = '%s:discodb:' % disco_prefix
            job     = Indexer(disco_master, prefix, dataset)
        except TypeError:
            metaset = MetaSet.loads(request.raw_post_data)
            prefix  = '%s:metadb:' % disco_prefix
            job     = MetaIndexer(disco_master, prefix, metaset)
        try:
            job.run()
        except ImportError, e:
            return HttpResponseServerError("Callable object not found: %s" % e)
        except DiscoError, e:
            return HttpResponseServerError("Failed to run indexing job: %s" % e)
        return HttpResponseAccepted(job.name)

    def read(self, request, *args, **kwargs):
        return Indices(self.names).response(request)

class IndexResource(Collection):
    allowed_methods = ('GET', 'POST', 'PUT', 'DELETE')

    def __init__(self, name):
        self.name = name
        self.responses['POST'] = 'append'

    def delegate(self, request, *args, **kwargs):
        if self.status == NOT_FOUND:
            raise Http404
        property = str(kwargs.pop('property'))
        return getattr(self, property)(request, *args, **kwargs)

    @property
    def exists(self):
        return ddfs.exists(self.tag)

    @property
    def isdisco(self):
        return self.name.startswith(disco_prefix)

    @property
    def isindex(self):
        return self.name.startswith(index_prefix)

    @property
    def jobname(self):
        if self.isdisco:
            return self.name
        if self.isindex:
            return self.name.replace(index_prefix, disco_prefix, 1)
        return '%s:%s' % (disco_prefix, self.name)

    @property
    def tag(self):
        return self.jobname.replace(disco_prefix, index_prefix, 1)

    @property
    @models.permalink
    def url(self):
        return 'index', (), {'name': self.name}

    @property
    def ichunks(self):
        return ddfs.blobs(self.tag)

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

            if status == OK:
                _prefix, type, id = self.name.split(':', 2)
                ddfs.put(self.tag, [[url.replace('disco://', '%s://' % type, 1)
                                     for url in urls]
                                    for urls in ddfs.blobs(results)])
                disco_master.purge(self.jobname)
            return status
        return NOT_FOUND

    def read(self, request, *args, **kwargs):
        status = self.status
        if status == OK:
            return Index(ddfs.get(self.tag)).response(request)
        if status == ACTIVE:
            return HttpResponseServiceUnavailable(2)
        if status == DEAD:
            return HttpResponseServerError("Indexing failed.")
        raise Http404

    def append(self, request, *args, **kwargs):
        ddfs.tag(self.tag, [['tag://%s' % IndexResource(request.raw_post_data).tag]])
        return HttpResponseCreated(self.url)

    def update(self, request, *args, **kwargs):
        ddfs.put(self.tag, Index.loads(request.raw_post_data).ichunks)
        return HttpResponseCreated(self.url)

    def delete(self, request, *args, **kwargs):
        ddfs.delete(self.tag)
        ddfs.delete(ddfs_name(self.jobname))
        return HttpResponseNoContent()

class DiscoDBResource(Resource):
    job_type    = DiscoDBIterator

    def __init__(self, index):
        self.index = index

    @property
    def job(self):
        return self.job_type(disco_master,
                             disco_prefix,
                             self.index.ichunks,
                             self.target,
                             self.mapfilters,
                             self.reducefilters,
                             self.resultsfilters)

    def read(self, request, *args, **kwargs):
        try:
            self.target         = str(kwargs.pop('target') or '')
            self.mapfilters     = filter(None, kwargs.pop('mapfilters').split('|'))
            self.reducefilters  = filter(None, kwargs.pop('reducefilters').split('}'))
            self.resultsfilters = filter(None, kwargs.pop('resultsfilters').split(']'))
            job = self.job.run()
        except DiscoError, e:
            return HttpResponseServerError("Failed to run DiscoDB job: %s" % e)

        try:
            results = Results(job.results)
        except DiscoError, e:
            return HttpResponseServerError("DiscoDB job failed: %s" % e)
        finally:
            if os.path.exists(purge_file):
                disco_master.purge(job.name)

        return results.response(request)

class KeysResource(DiscoDBResource):
    job_type    = KeyIterator

class ValuesResource(DiscoDBResource):
    job_type    = ValuesIterator

class ItemsResource(DiscoDBResource):
    job_type    = ItemsIterator

class QueryCollection(Collection):
    allowed_methods = ('GET', 'POST')

    def __init__(self, index):
        self.index = index

    def delegate(self, request, *args, **kwargs):
        query_path = str(kwargs.pop('query_path'))
        return QueryResource(self.index, query_path)(request, *args, **kwargs)

    def read(self, request, *args, **kwargs):
        return Results().response(request)

    def create(self, request, *args, **kwargs):
        query = Query.loads(request.raw_post_data)
        return QueryResource(self.index, query['query_path']).read(request, *args, **kwargs)

class QueryResource(DiscoDBResource):
    def __init__(self, index, query_path):
        self.index = index
        self.query = Q.urlscan(query_path)

    @property
    def job(self):
        return Queryer(disco_master,
                       disco_prefix,
                       self.index.ichunks,
                       self.target,
                       self.mapfilters,
                       self.reducefilters,
                       self.resultsfilters,
                       self.query)
