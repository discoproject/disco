import httplib, urllib2

from disco.error import CommError

def download(url,
             data=None,
             redir=False,
             offset=0,
             method=None,
             sleep=0,
             header=None):

    header = header if header != None else {}

    from disco.util import urlsplit
    try:
        scheme, netloc, path = urlsplit(url)
        http = httplib.HTTPConnection(str(netloc))
        h = {}
        if offset:
            if type(offset) == tuple:
                offs = 'bytes=%d-%d' % offset
            else:
                offs = 'bytes=%d-' % offset
            h = {'Range': offs}
        if not method:
            method = 'POST' if data != None else 'GET'
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
            return download(url, data, redir, offset, method, sleep, header)
        header.update(fd.getheaders())
        return fd.status, fd.read()
    except (httplib.HTTPException, httplib.socket.error), e:
        raise CommError("Transfer %s failed: %s" % (url, e), url)
