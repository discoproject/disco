import os, time, cStringIO, struct
from disco.error import CommError

import disco.settings

BUFFER_SIZE = int(1024**2)
CHUNK_SIZE = int(10 * 1024**2)

settings = disco.settings.DiscoSettings()
nocurl = "nocurl" in settings["DISCO_FLAGS"].lower().split()

try:
    import pycurl
except ImportError:
    nocurl = True

if nocurl:
    from disco.comm_httplib import upload, download as real_download
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
    code, body = real_download(url, **kwargs)
    if code == 503:
        sleep = kwargs.get('sleep', 0)
        if sleep == 9:
            raise CommError("Too many 503 replies", url)
        else:
            time.sleep(2**sleep)
            kwargs['sleep'] = sleep + 1
            return download(url, **kwargs)
    elif str(code)[0] != '2':
        raise CommError("Invalid HTTP reply (expected 200 or 206 got %s)" %
            code, url, code)
    return body

def open_local(path, url):
    # XXX: wouldn't it be polite to give a specific error message if this
    # operation fails
    fd = file(path, "r", BUFFER_SIZE)
    sze = os.stat(path).st_size
    return (fd, sze, "file://" + path)

def open_remote(url, expect = 200):
    conn = Conn(url)
    return conn, conn.length(), url   

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

    def read_all(self):
        buf = cStringIO.StringIO()
        ret = True
        while ret:
            ret = self.read(CHUNK_SIZE)
            buf.write(ret)
        return buf.getvalue()

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
