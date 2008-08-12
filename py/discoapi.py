import urllib, httplib, cjson, time
from netstring import decode_netstring_fd
import disco

class JobException(Exception):
        def __init__(self, msg, master, name):
                self.msg = msg
                self.name = name
                self.master = master

        def __str__(self):
                return "Job %s/%s failed: %s" %\
                        (self.master, self.name, self.msg)

class Disco(object):

        def __init__(self, host):
                self.host = disco.disco_host(host)[7:]
                self.conn = httplib.HTTPConnection(self.host)

        def request(self, url, data = None, raw_handle = False):
                try:
                        if data:
                                self.conn.request("POST", url, data)
                        else:
                                self.conn.request("GET", url, None)
                        r = self.conn.getresponse()
                        if raw_handle:
                                return r
                        else:
                                return r.read()
                except httplib.BadStatusLine:
                        self.conn.close()
                        self.conn = httplib.HTTPConnection(self.host)
                        return self.request(url, data)

        def nodeinfo(self):
                return cjson.decode(self.request("/disco/ctrl/nodeinfo"))

        def joblist(self):
                return cjson.decode(self.request("/disco/ctrl/joblist"))
        
        def kill(self, name):
                self.request("/disco/ctrl/kill_job", '"%s"' % name)
        
        def clean(self, name):
                self.request("/disco/ctrl/clean_job", '"%s"' % name)
        
        def jobspec(self, name):
                # Parameters request is handled with a separate connection that
                # knows how to handle redirects.
                r = urllib.urlopen("http://%s/disco/ctrl/parameters?name=%s"\
                        % (self.host, name))
                return decode_netstring_fd(r)

        def results(self, name):
                r = self.request("/disco/ctrl/get_results?name=" + name)
                if r:
                        return cjson.decode(r)
                else:
                        return None

        def jobinfo(self, name):
                r = self.request("/disco/ctrl/jobinfo?name=" + name)
                if r:
                        return cjson.decode(r)
                else:
                        return r

        def wait(self, name, poll_interval = 5, timeout = None):
                t = time.time()
                while True:
                        time.sleep(poll_interval)
                        status = self.results(name)
                        if status == None:
                                raise JobException("Unknown job", self.host, name)
                        if status[0] == "ready":
                                return status[1]
                        if status[0] != "active":
                                raise JobException("Job failed", self.host, name)
                        if timeout and time.time() - t > timeout:
                                raise JobException("Timeout", self.host, name)

if __name__ == "__main__":
        import sys
        d = Disco(sys.argv[1])
        if len(sys.argv) > 3:
                print getattr(d, sys.argv[2])(sys.argv[3])
        else:
                print getattr(d, sys.argv[2])()
