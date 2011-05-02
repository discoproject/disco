import httplib, urllib, urlparse

from disco.comm import HTTPConnection
from disco.util import urlresolve

from core import DiscodexError
from objects import DataSet, Indices, Index, Results, Dict
from settings import DiscodexSettings

class ResourceNotFound(DiscodexError):
    def __init__(self, resource):
        super(ResourceNotFound, self).__init__("%s not found" % resource)

class DiscodexServiceUnavailable(DiscodexError):
    def __init__(self, retry_after):
        self.retry_after = int(retry_after)

class DiscodexServerError(DiscodexError):
    pass

class DiscodexClient(object):
    def __init__(self, host=None, port=None, settings=DiscodexSettings()):
        self.host = host or settings['DISCODEX_HTTP_HOST']
        self.port = port or settings['DISCODEX_HTTP_PORT']

    @property
    def netloc(self):
        return '%s:%s' % (self.host, self.port)

    def indexurl(self, indexspec):
        resource = urlparse.urlparse(indexspec)
        if resource.netloc:
            return urlresolve(indexspec)
        path = '/indices/%s' % indexspec
        return urlparse.urlunparse(('http', self.netloc, path, '', '', ''))

    def request(self, method, url, body=None):
        resource = urlparse.urlparse(url)
        conn     = HTTPConnection(resource.netloc or self.netloc, timeout=None)
        path     = urlparse.urlunparse(('', '') + resource[2:])
        conn.request(method, path, body)
        response = conn.getresponse()
        if response.status == httplib.NOT_FOUND:
            raise ResourceNotFound(url)
        if response.status == httplib.SERVICE_UNAVAILABLE:
            raise DiscodexServiceUnavailable(response.getheader('Retry-After'))
        if response.status == httplib.INTERNAL_SERVER_ERROR:
            raise DiscodexServerError("request failed.\n\n%s""" % response.read())
        return response

    def list(self):
        return Indices.loads(self.request('GET', self.indexurl('')).read())

    def get(self, indexspec):
        return Index.loads(self.request('GET', self.indexurl(indexspec)).read())

    def append(self, indexaspec, indexbspec):
        self.request('POST', self.indexurl(indexaspec), indexbspec)

    def put(self, indexspec, index):
        self.request('PUT', self.indexurl(indexspec), index.ichunks.dumps())

    def delete(self, indexspec):
        self.request('DELETE', self.indexurl(indexspec))

    def index(self, dataset):
        return self.request('POST', self.indexurl(''), dataset.dumps()).read()

    def inquire(self, indexspec, inquiry, query=None, streams=(), reduce='', params={}):
        method = 'GET' if query is None else 'POST'
        body   = Dict(arg=query.urlformat()).dumps() if query else None
        url    = '%s/%s%s' % (self.indexurl(indexspec),
                              inquiry,
                              ''.join('|%s' % stream for stream in streams))
        if reduce:
            url += '}%s' % reduce
        if params:
            url += '?%s' % '&'.join('='.join(item) for item in params.items())

        return Results.loads(self.request(method, url, body).read())

    def items(self, indexspec, **kwargs):
        return self.inquire(indexspec, 'items', **kwargs)

    def keys(self, indexspec, **kwargs):
        return self.inquire(indexspec, 'keys', **kwargs)

    def values(self, indexspec, **kwargs):
        return self.inquire(indexspec, 'values', **kwargs)

    def query(self, indexspec, query, **kwargs):
        return self.inquire(indexspec, 'query', query=query, **kwargs)

    def metaquery(self, indexspec, query, **kwargs):
        return self.inquire(indexspec, 'metaquery', query=query, **kwargs)
