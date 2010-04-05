import cStringIO, struct, time, sys, os, random
from pycurl import *
from pycurl import error as PyCurlError

from disco.events import Message
from disco.error import CommError

FILE_BUFFER_SIZE = 10 * 1024**2
MAX_BUF = 1024**2
MAX_RETRIES = 10
CONNECT_TIMEOUT = 20
TRANSFER_TIMEOUT = 10 * 60

BLOCK_SIZE = 1024**2

def download(url, data = None, redir = False, offset = 0,
            method = None, sleep = 0, header = {}):

    def headfun(h):
        try:
            k, v = h.strip().split(":", 1)
            header[k.lower()] = v
        except:
            pass

    dl_handle = Curl()
    dl_handle.setopt(NOSIGNAL, 1)
    dl_handle.setopt(CONNECTTIMEOUT, CONNECT_TIMEOUT)
    #dl_handle.setopt(TIMEOUT, TRANSFER_TIMEOUT)
    if redir:
        dl_handle.setopt(FOLLOWLOCATION, 1)
    retry = 0
    if offset:
        if type(offset) == tuple:
            dl_handle.setopt(RANGE, "%d-%d" % offset)
        else:
            dl_handle.setopt(RANGE, "%d-" % offset)
    while True:
        dl_handle.setopt(URL, str(url))
        outbuf = cStringIO.StringIO()
        dl_handle.setopt(WRITEFUNCTION, outbuf.write)
        dl_handle.setopt(HEADERFUNCTION, headfun)
        if data != None:
            inbuf = cStringIO.StringIO(data)
            dl_handle.setopt(READFUNCTION, inbuf.read)
            dl_handle.setopt(POSTFIELDSIZE, len(data))
            dl_handle.setopt(HTTPHEADER, ["Expect:"])
            dl_handle.setopt(POST, 1)
        elif method != None:
            dl_handle.setopt(CUSTOMREQUEST, method)
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
    code = dl_handle.getinfo(HTTP_CODE)
    dl_handle.close()
    return code, outbuf.getvalue()

class MultiPut:
    def __init__(self, input, urls):
        if type(input) == tuple:
            data, size = input
            new_fd = lambda: cStringIO.StringIO(data)
        else:
            size = os.stat(input).st_size
            new_fd = lambda: file(input, "r", FILE_BUFFER_SIZE)
        self.handles = dict((url, self.init_handle(url, size, new_fd()))\
                                for url in urls)
        self.multi = CurlMulti()
        [self.multi.add_handle(handle) for handle, out in self.handles.values()]
    
    def init_handle(self, url, size, fd):
        out = cStringIO.StringIO()
        handle = Curl()
        handle.setopt(URL, str(url))
        handle.setopt(NOSIGNAL, 1)
        handle.setopt(CONNECTTIMEOUT, 20)
        handle.setopt(TIMEOUT, 10 * 60)
        handle.setopt(WRITEFUNCTION, out.write)
        handle.setopt(READFUNCTION, fd.read)
        handle.setopt(UPLOAD, 1)
        handle.setopt(INFILESIZE, size)
        # Workaround for Lighty (http://redmine.lighttpd.net/issues/1017)
        handle.setopt(HTTPHEADER, ["Expect:"])
        return handle, out

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
        for url, (handle, out) in self.handles.iteritems():
            ret = handle.getinfo(HTTP_CODE)
            if ret == 201:
                success.append(out.getvalue())
            elif ret == 503:
                retry.append(url)
            else:
                fail.append((url, ret, out.getvalue()))
        return success, retry, fail

def upload(fname, urls, retries = 10):
    success = []
    rounds = 0
    while urls and rounds <= retries:
        succ, urls, fail = MultiPut(fname, urls).perform()
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
            "The following URLs were unreachable: %s" % " ".join(urls), urls[0])
    return success







    






