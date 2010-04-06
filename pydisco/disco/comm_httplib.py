import httplib, urllib2

from disco.error import CommError

MAX_RETRIES = 10

def download(url,
             data=None,
             redir=False,
             offset=0,
             method=None,
             sleep=0,
             header={},
             ttl=MAX_RETRIES):
    from disco.util import urlsplit
    while True:
        try:
            scheme, netloc, path = urlsplit(url)
            http = httplib.HTTPConnection(netloc)
            h = {}
            if offset:
                if type(offset) == tuple:
                    offs = 'bytes=%d-%d' % offset
                else:
                    offs = 'bytes=%d-' % offset
                h = {'Range': offs}
            if not method:
                method = 'POST' if data else 'GET'
            http.request(method, '/%s' % path, data, headers = h)
            fd = http.getresponse()
            if fd.status == 302:
                loc = fd.getheader('location')
                if loc.startswith('http://'):
                    url = loc
                elif loc.startswith('/'):
                    url = 'http://%s%s' % (netloc, loc)
                else:
                    url = '%s/%s' % (url, loc)
                continue
            header.update(fd.getheaders())
            return fd.status, fd.read()
        except (httplib.HTTPException, httplib.socket.error), e:
            if not ttl:
                raise CommError("Transfer %s failed "
                                "after %d attempts: %s" %
                                (url, MAX_RETRIES, e), url)
            ttl -= 1

def upload(fname, urls, retries = 10):
    success = []
    blob    = open(fname).read()
    for url in urls:
        download(url, data=blob, method='PUT')
    return success

