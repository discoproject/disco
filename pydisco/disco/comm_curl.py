import cStringIO, struct, time, sys, os
from pycurl import *
from pycurl import error as PyCurlError

from disco.error import CommError

MAX_BUF = 1024**2
MAX_RETRIES = 10
CONNECT_TIMEOUT = 20
TRANSFER_TIMEOUT = 10 * 60

def check_code(c, expected, url):
    code = c.getinfo(HTTP_CODE)
    if code != expected:
        raise CommError("Invalid HTTP reply (expected %s got %s)" %
                    (expected, code), url)

def download(url, data = None, redir = False, offset = 0):
    dl_handle = Curl()
    dl_handle.setopt(NOSIGNAL, 1)
    dl_handle.setopt(CONNECTTIMEOUT, CONNECT_TIMEOUT)
    dl_handle.setopt(TIMEOUT, TRANSFER_TIMEOUT)
    if redir:
        dl_handle.setopt(FOLLOWLOCATION, 1)
    retry = 0
    if offset:
        dl_handle.setopt(RANGE, "%d-" % offset)
    while True:
        dl_handle.setopt(URL, str(url))
        outbuf = cStringIO.StringIO()
        dl_handle.setopt(WRITEFUNCTION, outbuf.write)
        if data != None:
            inbuf = cStringIO.StringIO(data)
            dl_handle.setopt(READFUNCTION, inbuf.read)
            dl_handle.setopt(POSTFIELDSIZE, len(data))
            dl_handle.setopt(HTTPHEADER, ["Expect:"])
            dl_handle.setopt(POST, 1)
        try:
            dl_handle.perform()
            break
        except PyCurlError, e:
            if retry == MAX_RETRIES:
                raise CommError("Download failed "
                        "after %d attempts: %s" %
                        (MAX_RETRIES, dl_handle.errstr()), url)
            dl_handle.setopt(FRESH_CONNECT, 1)
            retry += 1

    if offset:
        dl_handle.setopt(RANGE, "")
        check_code(dl_handle, 206, url)
    else:
        check_code(dl_handle, 200, url)
    return outbuf.getvalue()

class CurlConn:
    def __init__(self, url, handle = None, expect = 200):
        if handle:
            self.handle = handle
        else:
            self.handle = Curl()
        self.url = url

        for i in range(MAX_RETRIES):
            self.init_handle(url)
            self.perform()
            x, succ, fail = self.multi.info_read(1)
            if not fail:
                break
            self.handle = Curl()
            time.sleep(1.0)
        else:
            raise CommError("Couldn't connect after %d attempts: %s" %
                        (MAX_RETRIES, fail[0][2]), url)

        # make sure all headers are read
        while self.cont and not self.body:
            self.perform()

        code = self.handle.getinfo(HTTP_CODE)
        if code == 0:
            raise CommError("Couldn't receive http response", url)
        check_code(self.handle, expect, url)

    def init_handle(self, url):
        self.handle.setopt(URL, str(url))
        self.handle.setopt(NOSIGNAL, 1)
        self.handle.setopt(CONNECTTIMEOUT, 20)
        self.handle.setopt(TIMEOUT, 10 * 60)
        self.handle.setopt(WRITEFUNCTION, self.write)
        self.handle.setopt(HEADERFUNCTION, self.head)
        self.multi = CurlMulti()
        self.multi.add_handle(self.handle)
        self.buf = ""
        self.cont = 1
        self.length = None
        self.body = False

    def perform(self):
        if not self.cont:
            return
        r = -1
        timeout = CONNECT_TIMEOUT
        while r == -1 and timeout > 0:
            r = self.multi.select(1.0)
            timeout -= 1
        if timeout == 0:
            raise CommError("Connection timeout (1)", self.url)

        timeout = CONNECT_TIMEOUT
        ret = E_CALL_MULTI_PERFORM
        while ret == E_CALL_MULTI_PERFORM and timeout > 0:
            ret, num_handles = self.multi.perform()
            self.cont = num_handles
            timeout -= 1
        if timeout == 0:
            raise CommError("Connection timeout (2)", self.url)
    
    def head(self, buf):
        buf = buf.lower()
        if buf.startswith("content-length:"):
            self.length = int(buf.split(":")[1])
        elif buf.startswith("location:"):
            self.location = buf.split(":", 1)[1].strip()

    def getheader(self, x):
        if x == "location":
            return self.location

    def write(self, buf):
        self.body = True
        self.buf += buf

    def read(self, bytes = None):
        if bytes == None:
            while self.cont:
                self.perform()
            bytes = len(self.buf)
        else:
            while len(self.buf) < bytes and self.cont:
                self.perform()
        
        r = self.buf[:bytes]
        self.buf = self.buf[bytes:]
        return r

    def disco_stats(self):
        pass

def open_remote(url, expect = 200):
    c = Curl()
    conn = CurlConn(url, handle = c, expect = expect)
    return (conn, conn.length, url)












