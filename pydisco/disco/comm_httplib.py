import httplib, urllib2

from disco.error import CommError

MAX_RETRIES = 10

def download(url, data = None, offset = 0, method = None,
        sleep = 0, header = {}):
    while True:
        try:
            ext_host, ext_file = url[7:].split("/", 1)
            ext_file = "/" + ext_file
            http = httplib.HTTPConnection(ext_host)
            h = {}
            if offset:
                if type(offset) == tuple:
                    offs = "bytes=%d-%d" % offset
                else:
                    offs = "bytes=%d-" % offset
                h = {"Range": offs}
            if data:
                meth = "POST"
            elif method != None:
                meth = method
            else:
                meth = "GET"
            http.request(meth, ext_file, data, headers = h)
            fd = http.getresponse()
            if fd.status == 302:
                loc = fd.getheader("location")
                if loc.startswith("http://"):
                    url = loc
                elif loc.startswith("/"):
                    url = "http://%s%s" % (ext_host, loc)
                else:
                    url = "%s/%s" % (url, loc)
                continue
            header.update(fd.getheaders())
            return fd.status, fd.read()
        except (httplib.HTTPException, httplib.socket.error), e:
            if not ttl:
                raise CommError("Downloading %s failed "
                        "after %d attempts: %s" %
                        (url, MAX_RETRIES, e), url)
            ttl -= 1

def upload(fname, urls, retries = 10):
    raise CommError("Uploading with httplib is not currently supported. Install pycurl.", urls[0])
