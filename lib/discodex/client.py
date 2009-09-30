import httplib, urlparse

from objects import Indices, Index, IndexMonitor

class DiscodexException(Exception):
    pass

class ResourceNotFound(DiscodexException):
    pass

class DiscodexServiceUnavailable(DiscodexException):
    def __init__(self, retry_after):
        self.retry_after = int(retry_after)

class DiscodexServerError(DiscodexException):
    pass

class DiscodexClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port

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
            raise DiscodexServerError()
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

    def clone(self, indexaspec, indexbspec):
        index = self.get(indexaspec)
        index['origin'] = self.indexurl(indexaspec)
        self.put(indexbspec, index)

    def query(self, indexspec, query):
        pass
