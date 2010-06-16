import struct, time, sys, os, random
from cStringIO import StringIO
from httplib import HTTPException

import pycurl

from disco.events import Message
from disco.error import CommError

class CurlResponse(object):
    def __init__(self, connection):
        self.connection = connection
        self.headers = {}
        self.buffer = StringIO()

    def getheader(self, header, default=None):
        return self.headers.get(header, default)

    def getheaders(self):
        return self.headers.items()

    def header_function(self, header):
        if ':' in header:
            k, v = header.split(':', 1)
            self.headers[k.lower().strip()] = v.strip()

    def read(self):
        return self.buffer.getvalue()

    @property
    def status(self):
        return self.connection['HTTP_CODE']

class HTTPConnection(object):
    defaults = {'CONNECTTIMEOUT': 20,
                'FRESH_CONNECT': 1,
                'LOW_SPEED_LIMIT': 1024, # 1kbps
                'LOW_SPEED_TIME': 2 * 60,
                'NOSIGNAL': 1}

    def __init__(self, netloc):
        self.handle = pycurl.Curl()
        for k, v in self.defaults.items():
            self[k] = v

        self.netloc = netloc
        self.response = CurlResponse(self)
        self['HEADERFUNCTION'] = self.response.header_function
        self['WRITEFUNCTION']  = self.response.buffer.write

    def __getitem__(self, key):
        return self.handle.getinfo(getattr(pycurl, key))

    def __setitem__(self, key, value):
        self.handle.setopt(getattr(pycurl, key), value)

    def getresponse(self):
        return self.response

    def prepare(self, method, url, body=None, headers={}):
        self['CUSTOMREQUEST'] = method
        self['URL'] = '%s%s' % (self.netloc, url)

        if body is not None:
            if method == 'PUT':
                self.source = body
                self['READFUNCTION'] = body.makefile().read
                self['INFILESIZE']   = body.size
                self['UPLOAD'] = 1
            else:
                self['READFUNCTION']  = StringIO(body).read
                self['POSTFIELDSIZE'] = len(body)
                self['POST'] = 1

        self['HTTPHEADER'] = ['%s:%s' % item for item in headers.items()]
        return self

    def request(self, method, url, body=None, headers={}):
        self.prepare(method, url, body=body, headers=headers)
        try:
            self.handle.perform()
        except pycurl.error, e:
            raise HTTPException(self.handle.errstr())

class MultiPut(object):
    def __init__(self, pending):
        from disco.util import urlresolve
        self.multi = pycurl.CurlMulti()
        self.connections = dict((url,
                                 HTTPConnection('').prepare('PUT',
                                                            urlresolve(url),
                                                            body=source))
                                for url, source in pending)
        for connection in self.connections.values():
            self.multi.add_handle(connection.handle)

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

        success, retry, fail = [], [], []
        for url, conn in self.connections.iteritems():
            response = conn.getresponse()
            ret = response.status
            if str(ret).startswith('2'):
                success.append((url, response))
            elif ret == 503:
                retry.append((url, conn.source))
            else:
                fail.append((url, response))
        return success, retry, fail

def upload(urls, sources, retries=10):
    rounds = 0
    pending = zip(urls, sources)
    while pending and rounds <= retries:
        successful, pending, failed = MultiPut(pending).perform()
        rounds = 0 if successful else rounds + 1
        for url, response in successful:
            yield response.read()
        if failed:
            url, response = failed[0]
            raise CommError("Upload failed: %s" % response.read(),
                            url, response.status)
        if pending:
            time.sleep(1)
    if pending:
        raise CommError("Maximum number of retries reached. "
                        "The following URLs were unreachable: %s" %
                        " ".join(url for url, source in pending), pending[0][0])

