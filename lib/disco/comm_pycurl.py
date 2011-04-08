import httplib, time, base64
from cStringIO import StringIO

import pycurl

from disco.error import CommError

class CurlResponse(object):
    def __init__(self):
        self.headers = {}
        self.buffer = StringIO()

    def getheader(self, header, default=None):
        return self.headers.get(header.lower(), default)

    def getheaders(self):
        return self.headers.items()

    def header_function(self, header):
        if ':' in header:
            k, v = header.split(':', 1)
            self.headers[k.lower().strip()] = v.strip()

    def read(self):
        return self.buffer.getvalue()

class HTTPConnection(object):
    defaults = {'CONNECTTIMEOUT': 20,
                'FRESH_CONNECT': 1,
                'LOW_SPEED_LIMIT': 1024, # 1kbps
                'NOSIGNAL': 1}

    def __init__(self, netloc, timeout=120):
        self.handle = pycurl.Curl()
        for k, v in self.defaults.items():
            self[k] = v

        if timeout:
            self['LOW_SPEED_TIME'] = timeout

        self.netloc = netloc
        self.response = CurlResponse()
        self['HEADERFUNCTION'] = self.response.header_function
        self['WRITEFUNCTION']  = self.response.buffer.write

    def __getitem__(self, key):
        return self.handle.getinfo(getattr(pycurl, key))

    def __setitem__(self, key, value):
        if isinstance(value, basestring):
            value = value.encode('ascii')
        self.handle.setopt(getattr(pycurl, key), value)

    def getresponse(self):
        self.response.status = self['HTTP_CODE']
        if self.response.status in httplib.responses:
            self.response.reason = httplib.responses[self.response.status]
        else:
            self.response.reason = "Unknown status code %d" % self.response.status
        return self.response

    def prepare(self, method, url, body=None, headers={}):
        self['URL'] = '%s%s' % (self.netloc, url)

        if method == 'DELETE':
            self['CUSTOMREQUEST'] = method
        elif method == 'HEAD':
            self['NOBODY'] = 1
        elif method == 'PUT':
            self['UPLOAD'] = 1
        elif method == 'POST':
            self['POST'] = 1

        if body is not None:
            size = len(body) if hasattr(body, '__len__') else None
            read = body.read if hasattr(body, 'read') else StringIO(body).read

            if method == 'PUT':
                if size:
                    self['INFILESIZE'] = size
            elif method == 'POST':
                self['POSTFIELDSIZE'] = size if size is not None else -1
            self['READFUNCTION'] = read

        self['HTTPHEADER'] = ['%s:%s' % item for item in headers.items()] +\
                             ['Expect:'] # work-around for lighttpd
        return self

    def request(self, method, url, body=None, headers={}):
        if isinstance(body, unicode):
            body = body.encode('utf8')
        self.prepare(method, url, body=body, headers=headers)
        try:
            self.handle.perform()
        except pycurl.error, e:
            raise httplib.HTTPException(self.handle.errstr())

class MultiPut(object):
    def __init__(self, urls, source, token=None):
        from disco.util import urlresolve
        self.multi = pycurl.CurlMulti()
        headers = self.auth_header(token)
        self.pending = [(url, HTTPConnection('').prepare('PUT',
                                                         urlresolve(url),
                                                         body=source,
                                                         headers=headers))
                        for url in urls]
        for url, conn in self.pending:
            self.multi.add_handle(conn.handle)

    def auth_header(self, token):
        if token != None:
            return {'Authorization': 'Basic ' + base64.b64encode("token:" + token)}
        return {}

    def perform(self):
        num_handles = True
        while num_handles:
            ret = self.multi.select(1.0)
            if ret == -1:
                continue
            while True:
                ret, num_handles = self.multi.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
        for url, conn in self.pending:
            yield url, conn.getresponse()

def upload(urls, source, token, retries=10):
    unavailable = []

    for url, response in MultiPut(urls, source, token).perform():
        status = response.status
        if str(status).startswith('2'):
            yield response.read()
        elif status == httplib.SERVICE_UNAVAILABLE:
            if not retries:
                raise CommError("Maximum number of retries reached", url)
            unavailable.append(url)
        else:
            raise CommError("Upload failed: %s" % response.read(), url, status)

    if unavailable:
        for response in upload(unavailable, source, token, retries=retries-1):
            yield response
