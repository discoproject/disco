import httplib, urllib, urlparse

from disco.comm import HTTPConnection

from core import DiscodexError
from objects import DataSet, MetaSet, Indices, Index, Results, Query
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
            return indexspec
        path = '/indices/%s' % indexspec
        return urlparse.urlunparse(('http', self.netloc, path, '', '', ''))

    def request(self, method, url, body=None):
        resource = urlparse.urlparse(url)
        conn     = HTTPConnection(resource.netloc or self.netloc)
        conn.request(method, resource.path, body)
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
        self.request('PUT', self.indexurl(indexspec), index.dumps())

    def delete(self, indexspec):
        self.request('DELETE', self.indexurl(indexspec))

    def index(self, dataset):
        return self.request('POST', self.indexurl(''), dataset.dumps()).read()

    def metaindex(self, metaset):
        return self.request('POST', self.indexurl(''), metaset.dumps()).read()

    def clone(self, indexaspec, indexbspec):
        index = self.get(indexaspec)
        index['origin'] = self.indexurl(indexaspec)
        self.put(indexbspec, index)

    def keys(self, indexspec):
        return Results.loads(self.request('GET', '%s/keys' % self.indexurl(indexspec)).read())

    def values(self, indexspec):
        return Results.loads(self.request('GET', '%s/values' % self.indexurl(indexspec)).read())

    def query(self, indexspec, query):
        query = Query(query_path=query.urlformat())
        return Results.loads(self.request('POST', '%s/query/' % self.indexurl(indexspec), query.dumps()).read())
