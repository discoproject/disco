import fileinput, httplib, urllib, urlparse

from discodb import Q

from core import DiscodexError
from objects import json, DataSet, MetaSet, Indices, Index, Keys, Values, Query

class ResourceNotFound(DiscodexError):
    pass

class DiscodexServiceUnavailable(DiscodexError):
    def __init__(self, retry_after):
        self.retry_after = int(retry_after)

class DiscodexServerError(DiscodexError):
    pass

class DiscodexClient(object):
    def __init__(self, host, port, options=None):
        self.host = host
        self.port = port
        self.options = options

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
        conn     = httplib.HTTPConnection(resource.netloc or self.netloc)
        conn.request(method, resource.path, body)
        response = conn.getresponse()
        if response.status == httplib.NOT_FOUND:
            raise ResourceNotFound()
        if response.status == httplib.SERVICE_UNAVAILABLE:
            raise DiscodexServiceUnavailable(response.getheader('Retry-After'))
        if response.status == httplib.INTERNAL_SERVER_ERROR:
            raise DiscodexServerError("request failed.\n\n%s""" % response.read())
        return response

    def list(self):
        return Indices.loads(self.request('GET', self.indexurl('')).read())

    def get(self, indexspec):
        return Index.loads(self.request('GET', self.indexurl(indexspec)).read())

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
        return Keys.loads(self.request('GET', '%s/keys' % self.indexurl(indexspec)).read())

    def values(self, indexspec):
        return Values.loads(self.request('GET', '%s/values' % self.indexurl(indexspec)).read())

    def query(self, indexspec, query):
        query = Query(query_path=query.urlformat())
        return Values.loads(self.request('POST', '%s/query/' % self.indexurl(indexspec), query.dumps()).read())

class CommandLineClient(DiscodexClient):
    def put(self, indexspec, *args):
        index = Index.loads(''.join(fileinput.input(args)))
        return super(CommandLineClient, self).put(indexspec, index)

    def index(self, *args):
        dataset = DataSet(options=self.options,
                          input=[line.strip() for line in fileinput.input(args)])
        return super(CommandLineClient, self).index(dataset)

    def metaindex(self, indexspec):
        metaset = MetaSet(options=self.options,
                          ichunks=self.get(indexspec).ichunks)
        return super(CommandLineClient, self).metaindex(metaset)

    def clone(self, indexaspec, indexbspec):
        index = self.get(indexaspec)
        index['origin'] = self.indexurl(indexaspec)
        super(CommandLineClient, self).put(indexbspec, index)

    def query(self, indexspec, *args):
        query = Q.scan(' '.join(args), and_op=' ', or_op=',')
        return super(CommandLineClient, self).query(indexspec, query)

    def send(self, command, *args):
        result = getattr(self, command)(*args)
        if result is not None:
            yield json.dumps(result)
