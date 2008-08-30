import sys, re, os, marshal, urllib, httplib, cjson, time, cPickle
from disco import func, util
from netstring import *


class JobException(Exception):
        def __init__(self, msg, master, name):
                self.msg = msg
                self.name = name
                self.master = master

        def __str__(self):
                return "Job %s/%s failed: %s" %\
                        (self.master, self.name, self.msg)


class Params:
        def __init__(self, **kwargs):
                self._state = {}
                for k, v in kwargs.iteritems():
                        setattr(self, k, v)

        def __setattr__(self, k, v):
                if k[0] == '_':
                        self.__dict__[k] = v
                        return
                st_v = v
                st_k = "n_" + k
                try:
                        st_v = marshal.dumps(v.func_code)
                        st_k = "f_" + k
                except AttributeError:
                        pass
                self._state[st_k] = st_v
                self.__dict__[k] = v
        
        def __getstate__(self):
                return self._state

        def __setstate__(self, state):
		self._state = {}
		for k, v in state.iteritems():
                        if k.startswith('f_'):
                                t = lambda x: x
                                t.func_code = marshal.loads(v)
                                v = t
                        self.__dict__[k[2:]] = v


class Disco(object):

        def __init__(self, host):
                self.host = util.disco_host(host)[7:]
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
        
        def new_job(self, **kwargs):
                return Job(self, **kwargs)
        
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

        def wait(self, name, poll_interval = 5, timeout = None, clean = False):
                t = time.time()
                while True:
                        time.sleep(poll_interval)
                        status = self.results(name)
                        if status == None:
                                raise JobException("Unknown job", self.host, name)
                        if status[0] == "ready":
                                if clean:
                                        self.clean(name)
                                return status[1]
                        if status[0] != "active":
                                raise JobException("Job failed", self.host, name)
                        if timeout and time.time() - t > timeout:
                                raise JobException("Timeout", self.host, name)


class Job(object):

        defaults = {"map_reader": func.map_line_reader,
                    "reduce": None,
                    "partition": func.default_partition,
                    "combiner": None,
                    "nr_maps": None,
                    "nr_reduces": None,
                    "sort": False,
                    "params": Params(),
                    "mem_sort_limit": 256 * 1024**2,
                    "chunked": None,
                    "ext_params": None}

        def __init__(self, master, **kwargs):
                self.master = master
                if "name" not in kwargs:
                        raise "Argument name is required"
                if re.search("\W", kwargs["name"]):
                        raise "Only characters in [a-zA-Z0-9_] "\
                              "are allowed in the job name"
                self.name = "%s@%d" % (kwargs["name"], int(time.time()))
                self._run(**kwargs)

        def __getattr__(self, name):
                def r(f):
                        def g(**kw):
                                return f(self.name, **kw)
                        return g
                if name in ["kill", "clean", "jobspec", "results",
                            "jobinfo", "wait"]:
                        return r(getattr(self.master, name))
                raise AttributeError("%s not found" % name)
       
        def _run(self, **kw):
                d = lambda x: kw.get(x, Job.defaults[x])

                if "fun_map" in kw:
                        kw["map"] = kw["fun_map"]
                
                if "input_files" in kw:
                        kw["input"] = kw["input_files"]
                
                if not ("map" in kw and "input" in kw):
                        raise "Arguments 'map' and 'input' are required"
                
                if len(kw["input"]) < 1:
                        raise "Must have at least one input file"

                inputs = []
                for inp in kw["input"]:
                        if inp.startswith("dir://"):
                                inputs += util.parse_dir(inp)
                        else:
                                inputs.append(inp)

                req = {"name": self.name,
                       "input": " ".join(inputs),
                       "version": ".".join(map(str, sys.version_info[:2])),
                       "map_reader": marshal.dumps(d("map_reader").func_code),
                       "partition": marshal.dumps(d("partition").func_code),
                       "params": cPickle.dumps(d("params")),
                       "sort": str(int(d("sort"))),
                       "mem_sort_limit": str(d("mem_sort_limit"))}

                if type(kw["map"]) == dict:
                        req["ext_map"] = marshal.dumps(kw["map"])
                else:
                        req["map"] = marshal.dumps(kw["map"].func_code)
        
                if "ext_params" in kw:
                        if type(kw["ext_params"]) == dict:
                                req["ext_params"] =\
                                        encode_netstring_fd(kw["ext_params"])
                        else:
                                req["ext_params"] = kw["ext_params"]
        
                if "nr_maps" not in kw or kw["nr_maps"] > len(inputs):
                        nr_maps = len(inputs)
                else:
                        nr_maps = kw["nr_maps"]
                req["nr_maps"] = str(nr_maps)
        
                nr_reduces = d("nr_reduces")
                if "reduce" in kw:
                        if type(kw["reduce"]) == dict:
                                req["ext_reduce"] = marshal.dumps(kw["reduce"])
                                req["reduce"] = ""
                        else:
                                req["reduce"] = marshal.dumps(
                                        kw["reduce"].func_code)
                        nr_reduces = nr_reduces or max(nr_maps / 2, 1)
                        req["chunked"] = "True"
                else:
                        nr_reduces = nr_reduces or 1
                req["nr_reduces"] = str(nr_reduces)

                if d("chunked") != None:
                        if d("chunked"):
                                req["chunked"] = "True"
                        elif "chunked" in req:
                                del req["chunked"]

                if "combiner" in kw:
                        req["combiner"] =\
                                marshal.dumps(kw["combiner"].func_code)

                self.msg = encode_netstring_fd(req)
                reply = self.master.request("/disco/job/new", self.msg)
                        
                if reply != "job started":
                        raise "Failed to start a job. Server replied: " + reply



def result_iterator(results, notifier = None, proxy = None):
        
        if not proxy:
                proxy = os.environ.get("DISCO_PROXY", None)
        if proxy:
                if proxy.startswith("disco://"):
                        proxy = "%s:%s" % (proxy[8:], util.HTTP_PORT)
                elif proxy.startswith("http://"):
                        proxy = proxy[7:]
        res = []
        for dir_url in results:
                if dir_url.startswith("dir://"):
                        res += util.parse_dir(dir_url, proxy)
                else:
                        res.append(dir_url)
        
        for url in res:
                if url.startswith("file://"):
                        fname = url[7:]
                        fd = file(fname)
                        sze = os.stat(fname).st_size
                        http = None
                else:
                        host, fname = url[8:].split("/", 1)
                        if proxy:
                                ext_host = proxy
                                fname = "/disco/node/%s/%s" % (host, fname)
                        else:
                                ext_host = host + ":" + util.HTTP_PORT
                        ext_file = "/" + fname

                        http = httplib.HTTPConnection(ext_host)
                        http.request("GET", ext_file, "")
                        fd = http.getresponse()
                        if fd.status != 200:
                                raise "HTTP error %d" % fd.status
                
                        sze = int(fd.getheader("content-length"))

                if notifier:
                        notifier(url)

                for x in func.netstr_reader(fd, sze, fname):
                        yield x
                
                if http:
                        http.close()
                else:
                        fd.close()

