import httplib, urllib2, cStringIO, struct

http_pool = {}

class CommException(Exception):
        def __init__(self, http_code, msg = None):
                self.http_code = http_code
                self.msg = msg

        def __str__(self):
                if self.msg:
                        return "HTTP exception (http status '%s'): %s" %\
                                (self.http_code, self.msg)
                else:
                        return "HTTP exception (http status '%s')" %\
                                self.http_code

def download(url, data = None, redir = False, offset = 0):
        if redir:
                req = urllib2.Request(url, data)
                if offset:
                        req.add_header("Range", "bytes=%d-" % offset)
                try:
                        c = urllib2.urlopen(req)
                except urllib2.HTTPError, x:
                        raise CommException(x.msg)
                r = c.read()
                c.close()
                return r
        else:
                sze, fd = open_remote(url, data = data, offset = offset)
                return fd.read()


def check_code(fd, expected):
        if fd.status != expected:
                raise CommException(fd.status)

def open_remote(url, data = None, expect = 200, offset = 0):
        try:
                ext_host, ext_file = url[7:].split("/", 1)
                ext_file = "/" + ext_file

                # We can't open a new HTTP connection for each intermediate
                # result -- this would result to M * R TCP connections where
                # M is the number of maps and R the number of reduces. Instead,
                # we pool connections and reuse them whenever possible. HTTP 
                # 1.1 defaults to keep-alive anyway.
                if ext_host in http_pool:
                        http = http_pool[ext_host]
                        if http._HTTPConnection__response:
                                http._HTTPConnection__response.read()
                else:
                        http = httplib.HTTPConnection(ext_host)
                        http_pool[ext_host] = http

                h = {}
                if offset:
                        h = {"Range": "bytes=%d-" % offset}
                        expect = 206

                if data:
                        http.request("POST", ext_file, data, headers = h)
                        fd = http.getresponse()
                        check_code(fd, expect)
                else:
                        http.request("GET", ext_file, None, headers = h)
                        fd = http.getresponse()
                        check_code(fd, expect)
               
                sze = fd.getheader("content-length")
                if sze:
                        sze = int(sze)
                return sze, fd

        except httplib.BadStatusLine:
                # BadStatusLine is caused by a closed connection. Re-open a new
                # connection by deleting this connection from the pool and
                # calling this function again. Note that this might result in
                # endless recursion if something went seriously wrong.
                http.close()
                del http_pool[ext_host]
                return open_remote(url, data)
