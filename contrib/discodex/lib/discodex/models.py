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
                                DiscoDBIterator)
from discodex.objects import (DataSet,
                              IChunks,
                              Indices,
                              Index,
                              Results,
                              Dict)

from disco.core import Disco
from disco.ddfs import DDFS
from disco.error import DiscoError
from disco.util import flatten, parse_dir

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
        dataset = DataSet.loads(request.raw_post_data)
        prefix  = '%s:discodb:' % disco_prefix
        job     = Indexer(disco_master, prefix, dataset)
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
        return DiscoDBResource(self)(request, *args, **kwargs)

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
        ddfs.put(self.tag, IChunks.loads(request.raw_post_data))
        return HttpResponseCreated(self.url)

    def delete(self, request, *args, **kwargs):
        ddfs.delete(self.tag)
        ddfs.delete(ddfs.job_tag(self.jobname))
        return HttpResponseNoContent()

class DiscoDBResource(Resource):
    allowed_methods = ('GET', 'POST')

    def __init__(self, index):
        self.index = index

    def read(self, request, *args, **kwargs):
        from discodex.mapreduce.func import reify

        method = str(kwargs.pop('method', None) or '')
        arg    = str(kwargs.pop('arg', None) or '')

        streams = [reify(s) for s in kwargs.pop('streams').split('|') if s]
        reduce  = reify((kwargs.pop('reduce') or 'None').strip('}'))

        try:
            job = DiscoDBIterator(disco_master,
                                  disco_prefix,
                                  self.index,
                                  method,
                                  arg,
                                  streams,
                                  reduce,
                                  **dict(request.GET.items())).run()
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

    def create(self, request, *args, **kwargs):
        kwargs.update(Dict.loads(request.raw_post_data))
        return self.read(request, *args, **kwargs)
