import os, random, struct, time, socket, base64
from disco.compat import BytesIO, file, httplib, basestring, str_to_bytes
from disco.error import CommError
from disco.settings import DiscoSettings
from disco.util import iterify, urlresolve, urlsplit

BUFFER_SIZE = int(1024**2)
CHUNK_SIZE = int(10 * 1024**2)

settings = DiscoSettings()
nocurl = 'nocurl' in settings['DISCO_FLAGS'].lower().split()

try:
    import pycurl
except ImportError:
    nocurl = True

if nocurl:
    HTTPConnection = httplib.HTTPConnection
else:
    from disco import comm_pycurl
    from disco.comm_pycurl import HTTPConnection

def isredirection(status):
    return str(status).startswith('3')

def issuccessful(status):
    return str(status).startswith('2')

def isunavailable(status):
    return status == httplib.SERVICE_UNAVAILABLE

def range_header(offset):
    def httprange(start='', end=''):
        return '{0}-{1}'.format(start, end)
    if offset:
        return {'Range': 'bytes={0}'.format(httprange(*tuple(iterify(offset))))}
    return {}

def auth_header(token):
    if token != None:
        return {'Authorization': b'Basic ' + base64.b64encode(b"token:" + token.encode('utf-8'))}
    return {}

def resolveuri(baseuri, uri):
    if uri.startswith('/'):
        scheme, netloc, _path = urlsplit(baseuri)
        return '{0}://{1}{2}'.format(scheme, netloc, uri)
    return '{0}/{1}'.format(baseuri, uri)

def request(method, url, data=None, headers={}, sleep=0):
    scheme, netloc, path = urlsplit(urlresolve(url))

    try:
        conn = HTTPConnection(str(netloc))
        conn.request(method, '/{0}'.format(path), body=data, headers=headers)
        response = conn.getresponse()
        status = response.status
        errmsg = response.reason
    except httplib.HTTPException as e:
        status = None
        errmsg = str(e) or repr(e)
    except (httplib.socket.error, socket.error) as e:
        status = None
        errmsg = e if isinstance(e, basestring) else str(e) or repr(e)

    if not status or isunavailable(status):
        if sleep == 9:
            raise CommError(errmsg, url, status)
        time.sleep(random.randint(1, 2**sleep))
        return request(method, url, data=data, headers=headers, sleep=sleep + 1)
    elif isredirection(status):
        loc = response.getheader('location')
        return request(method,
                       loc if loc.startswith('http:') else resolveuri(url, loc),
                       data=data,
                       headers=headers,
                       sleep=sleep)
    elif not issuccessful(status):
        raise CommError(response.read(), url, status)
    return response

def download(url, method='GET', data=None, offset=(), token=None):
    headers = range_header(offset)
    headers.update(auth_header(token))
    return request(method if data is None else 'POST',
                   url,
                   data=data,
                   headers=headers).read()

def upload(urls, source, token=None, **kwargs):
    data = FileSource(source).read()
    headers = auth_header(token)
    if nocurl:
        return [request('PUT', url, data=data, headers=headers).read()
                for url in urls]
    return list(comm_pycurl.upload(urls, data, token, **kwargs))

def open_url(url, *args, **kwargs):
    from disco.util import schemesplit
    scheme, rest = schemesplit(url)
    if not scheme or scheme == 'file':
        return open_local(rest, *args, **kwargs)
    return open_remote(url, *args, **kwargs)

def open_local(path):
    return File(path, 'rb', BUFFER_SIZE)

def open_remote(url, token=None):
    return Connection(urlresolve(url), token)

class FileSource(object):
    def __init__(self, source):
        self.isopen = hasattr(source, 'read')
        self.source = source.read() if self.isopen else source

    def __len__(self):
        if self.isopen:
            return len(self.source)
        return os.stat(self.source).st_size

    @property
    def read(self):
        if self.isopen:
            return BytesIO(str_to_bytes(self.source)).read
        return open(self.source, 'rb').read

class File(file):
    def __len__(self):
        return os.path.getsize(self.name)

    @property
    def url(self):
        return 'file://{0}'.format(self.name)

# should raise DataError
class Connection(object):
    def __init__(self, url, token=None):
        self.url = url
        self.token = token
        self.buf = None
        self.offset = 0
        self.orig_offset = 0
        self.eof = False
        self.response = None
        self.read(1)
        self.i = 0

    def __iter__(self):
        chunk = self._read_chunk(CHUNK_SIZE)
        while chunk:
            next_chunk = self._read_chunk(CHUNK_SIZE)
            lines = list(BytesIO(chunk))
            last  = lines.pop() if next_chunk else b''
            for line in lines:
                yield line
            chunk = last + next_chunk

    def __len__(self):
        if self.response == None:
            return 0
        if self.response.getheader('content-range', None) != None:
            return int(self.response.getheader('content-range').split('/')[1])
        elif self.response.getheader('transfer-encoding') == 'chunked':
            return len(self.buf)
        return int(self.response.getheader('content-length', 0))

    def close(self):
        pass

    def read(self, size=-1):
        buf = BytesIO()
        while size:
            bytes = self._read_chunk(size if size > 0 else CHUNK_SIZE)
            if not bytes:
                break
            size -= len(bytes)
            buf.write(bytes)
        return buf.getvalue()

    def _read_chunk(self, n):
        if self.buf is None or self.i >= len(self.buf):
            if self.eof:
                return b''
            self.i = 0
            if len(self):
                end = min(len(self), self.offset + CHUNK_SIZE) - 1
            else:
                end = self.offset + CHUNK_SIZE - 1
            headers = auth_header(self.token)
            headers.update(range_header((self.offset, end)))
            self.response = request('GET',
                                    self.url,
                                    headers=headers)
            self.buf = self.response.read()
            self.orig_offset = self.offset
            self.offset += len(self.buf)
            if len(self) and self.offset >= len(self):
                self.eof = True
            elif self.buf == b'':
                self.eof = True
        ret = self.buf[self.i:self.i + n]
        self.i += len(ret)
        return ret

    def tell(self):
        return self.orig_offset + self.i

    def seek(self, pos, mode=0):
        if mode == 0:
            self.offset = pos
        elif mode == 1:
            self.offset = self.tell() + pos
        else:
            self.offset = len(self) - pos
        self.eof = False
        self.buf = None
        self.orig_offset = self.offset
        self.i = 0
