import os, time, cStringIO, struct

from disco.error import CommError
from disco.settings import DiscoSettings
from disco.util import urlresolve

BUFFER_SIZE = int(1024**2)
CHUNK_SIZE = int(10 * 1024**2)

settings = DiscoSettings()
nocurl = "nocurl" in settings["DISCO_FLAGS"].lower().split()

try:
    import pycurl
except ImportError:
    nocurl = True

if nocurl:
    from disco.comm_httplib import download as real_download
    def upload(urls, retries = 10):
        return [download(url, data=source.makefile().read(), method='PUT')
                    for url, source in urls]
else:
    from disco.comm_curl import upload, download as real_download

# get rid of this for python2.6+
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import cjson
        class Dummy(object):
            pass
        json = Dummy()
        json.loads = cjson.decode
        json.dumps = cjson.encode

def download(url, **kwargs):
    code, body = real_download(urlresolve(url), **kwargs)
    if code == 503:
        sleep = kwargs.get('sleep', 0)
        if sleep == 9:
            raise CommError("Too many 503 replies", url)
        else:
            time.sleep(2**sleep)
            kwargs['sleep'] = sleep + 1
            return download(url, **kwargs)
    elif not str(code).startswith('2'):
        raise CommError(body, url, code)
    return body

def open_local(path, url):
    # XXX: wouldn't it be polite to give a specific error message if this
    # operation fails
    fd = open(path, 'r', BUFFER_SIZE)
    sze = os.stat(path).st_size
    return (fd, sze, 'file://' + path)

def open_remote(url):
    conn = Conn(urlresolve(url))
    return conn, conn.length(), conn.url

class Conn(object):
    def __init__(self, url):
        self.url = url
        self.buf = None
        self.offset = 0
        self.orig_offset = 0
        self.eof = False
        self.header = {}
        self.size = None
        self.read(1)
        self.i = 0

    def close(self):
        pass

    def read(self, n = None):
        if n == None:
            return self.read_all()
        buf = ""
        while n > 0:
            b = self.read_chunk(n)
            if not b:
                break
            n -= len(b)
            buf += b
        return buf

    def read_all(self):
        buf = cStringIO.StringIO()
        ret = True
        while ret:
            ret = self.read(CHUNK_SIZE)
            buf.write(ret)
        return buf.getvalue()

    def read_chunk(self, n):
        if self.buf == None or self.i >= len(self.buf):
            if self.eof:
                return ''
            self.i = 0
            if self.size:
                end = min(self.size, self.offset + CHUNK_SIZE) - 1
            else:
                end = self.offset + CHUNK_SIZE - 1
            self.buf = download(self.url, header = self.header,
                offset = (self.offset, end))
            if 'content-range' in self.header:
                self.size = int(self.header['content-range'].split('/')[1])
            else:
                self.size = int(self.header['content-length'])
            self.orig_offset = self.offset
            self.offset += len(self.buf)
            if self.size and self.offset >= self.size:
                self.eof = True
            elif self.buf == "":
                self.eof = True
        ret = self.buf[self.i:self.i + n]
        self.i += len(ret)
        return ret

    def getheader(self, header):
        return self.header.get(header, None)

    def length(self):
        return self.size

    def tell(self):
        return self.orig_offset + self.i

    def seek(self, pos, mode = 0):
        bef = self.offset
        if mode == 0:
            self.offset = pos
        elif mode == 1:
            self.offset = self.tell() + pos
        else:
            self.offset = self.size - pos
        self.eof = False
        self.buf = None
        self.orig_offset = self.offset
        self.i = 0
