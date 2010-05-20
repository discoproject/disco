import cStringIO, struct, time, sys, os, random
from pycurl import *
from pycurl import error as PyCurlError

from disco.events import Message
from disco.error import CommError

FILE_BUFFER_SIZE = 10 * 1024**2
MAX_BUF = 1024**2
MAX_RETRIES = 10
D_CONNECT_TIMEOUT = 20
D_LOW_SPEED_LIMIT = 1024 # 1kbps
D_LOW_SPEED_TIME = 2 * 60

BLOCK_SIZE = 1024**2

def new_handle(url):
    handle = Curl()
    handle.setopt(FRESH_CONNECT, 1)
    handle.setopt(URL, str(url))
    handle.setopt(NOSIGNAL, 1)
    handle.setopt(CONNECTTIMEOUT, D_CONNECT_TIMEOUT)
    handle.setopt(LOW_SPEED_LIMIT, D_LOW_SPEED_LIMIT)
    handle.setopt(LOW_SPEED_TIME, D_LOW_SPEED_TIME)
    # Workaround for Lighty (http://redmine.lighttpd.net/issues/1017)
    handle.setopt(HTTPHEADER, ["Expect:"])
    return handle

def download_handle(url, data, redir, offset, method, header):
    def headfun(h):
        try:
            k, v = h.strip().split(":", 1)
            header[k.lower()] = v
        except Exception, e:
            pass
    dl_handle = new_handle(url)
    outbuf = cStringIO.StringIO()
    dl_handle.setopt(HEADERFUNCTION, headfun)
    dl_handle.setopt(WRITEFUNCTION, outbuf.write)
    if redir:
        dl_handle.setopt(FOLLOWLOCATION, 1)
    if offset:
        if type(offset) == tuple:
            dl_handle.setopt(RANGE, "%d-%d" % offset)
        else:
            dl_handle.setopt(RANGE, "%d-" % offset)
    if data != None:
        inbuf = cStringIO.StringIO(data)
        dl_handle.setopt(READFUNCTION, inbuf.read)
        dl_handle.setopt(POSTFIELDSIZE, len(data))
        dl_handle.setopt(POST, 1)
    elif method != None:
        dl_handle.setopt(CUSTOMREQUEST, method)
    return dl_handle, outbuf

def download(url,
             data=None,
             redir=False,
             offset=0,
             method=None,
             sleep=0,
             header=None):

    header = header if header != None else {}
    dl_handle, outbuf =\
        download_handle(url, data, redir, offset, method, header)
    try:
        dl_handle.perform()
    except PyCurlError, e:
        raise CommError("Download failed: %s" % dl_handle.errstr(), url)
    code = dl_handle.getinfo(HTTP_CODE)
    return code, outbuf.getvalue()

class MultiPut:
    def __init__(self, urls):
        self.handles = dict(
            (url, self.init_handle(url, source)) for url, source in urls)
        self.multi = CurlMulti()
        for handle, out, source in self.handles.values():
            self.multi.add_handle(handle)

    def init_handle(self, url, source):
        handle = new_handle(url)
        out = cStringIO.StringIO()
        handle.setopt(WRITEFUNCTION, out.write)
        handle.setopt(READFUNCTION, source.makefile().read)
        handle.setopt(INFILESIZE, source.size)
        handle.setopt(UPLOAD, 1)
        return handle, out, source

    def perform(self):
        num_handles = True
        while num_handles:
            ret = self.multi.select(1.0)
            if ret == -1:
                continue
            while True:
                ret, num_handles = self.multi.perform()
                if ret != E_CALL_MULTI_PERFORM:
                    break
        success = []
        retry = []
        fail = []
        for url, (handle, out, source) in self.handles.iteritems():
            ret = handle.getinfo(HTTP_CODE)
            if ret == 201:
                success.append(out.getvalue())
            elif ret == 503:
                retry.append((url, source))
            else:
                fail.append((url, ret, out.getvalue()))
        return success, retry, fail

def upload(urls, retries=None):
    retries = MAX_RETRIES if retries == None else retries
    success = []
    rounds = 0
    while urls and rounds <= retries:
        succ, urls, fail = MultiPut(urls).perform()
        rounds += 1
        if succ:
            rounds = 0
        success += succ
        if fail:
            url, ret, out = fail[0]
            raise CommError("PUT failed (%d): %s" % (ret, out), url, ret)
        if urls:
            time.sleep(1)
    if urls:
        raise CommError("Maximum number of PUT retries reached. "
            "The following URLs were unreachable: %s" %
                " ".join(url for url, source in urls), urls[0][0])
    return success
