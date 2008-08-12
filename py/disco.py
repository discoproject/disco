
from netstring import *
import sys, os, marshal, traceback, time, re, urllib, httplib, cPickle
import discoapi

DISCO_NEW_JOB = "/disco/job/new"

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


def load_conf():
        port = root = None
        
        conf = (os.path.exists("disco.conf") and "disco.conf") or\
               (os.path.exists("/etc/disco/disco.conf") and\
                        "/etc/disco/disco.conf")
        if conf:
                txt = file(conf).read()
                port = re.search("DISCO_PORT\s*=\s*(\d+)", txt)
                root = re.search("DISCO_ROOT\s*=\s*(.+)", txt)
        
        port = (port and port.group(1)) or "8989"
        root = (root and root.group(1)) or "/srv/disco/"
        
        return os.environ.get("DISCO_PORT", port.strip()),\
               os.environ.get("DISCO_ROOT", root.strip()) + "/data/"


def disco_host(addr):
        if addr.startswith("disco:"):
                addr = addr.split("/")[-1]
                if ":" in addr:
                        addr = addr.split(":")[0]
                        print >> sys.stderr, "NOTE! disco://host:port format "\
                                "is deprecated.\nUse disco://host instead, or "\
                                "http://host:port if master doesn't run at "\
                                "DISCO_PORT."
                return "http://%s:%s" % (addr.split("/")[-1], HTTP_PORT)
        elif addr.startswith("http:"):
                return addr
        else:
                raise "Unknown host specifier: %s" % master


def parse_dir(dir_url):
        x, x, host, mode, name = dir_url.split('/')
        html = urllib.urlopen("http://%s:%s/%s" %\
                (host, HTTP_PORT, name)).read()
        inputs = re.findall(">(%s-(.+?)-.*?)</a>" % mode, html)
        return ["%s://%s/%s/%s" % (prefix, host, name, x)\
                        for x, prefix in inputs if "partial" not in x]


def netstr_reader(fd, content_len, fname):
        if content_len == None:
                err("Content-length must be defined for netstr_reader")
        def read_netstr(idx, data, tot):
                ldata = len(data)
                i = 0
                lenstr = ""
                if ldata - idx < 11:
                        data = data[idx:] + fd.read(8192)
                        ldata = len(data)
                        idx = 0

                i = data.find(" ", idx, idx + 11)
                if i == -1:
                        err("Corrupted input (%s). Could not "\
                               "parse a value length at %d bytes."\
                                        % (fname, tot))
                else:
                        lenstr = data[idx:i + 1]
                        idx = i + 1

                if ldata < i + 1:
                        data_err("Truncated input (%s). "\
                                "Expected %d bytes, got %d" %\
                                (fname, content_len, tot), fname)
                
                try:
                        llen = int(lenstr)
                except ValueError:
                        err("Corrupted input (%s). Could not "\
                                "parse a value length at %d bytes."\
                                        % (fname, tot))

                tot += len(lenstr)

                if ldata - idx < llen + 1:
                        data = data[idx:] + fd.read(llen + 8193)
                        ldata = len(data)
                        idx = 0

                msg = data[idx:idx + llen]
                
                if idx + llen + 1 > ldata:
                        data_err("Truncated input (%s). "\
                                "Expected a value of %d bytes "\
                                "(offset %u bytes)" %\
                                (fname, llen + 1, tot), fname)

                tot += llen + 1
                idx += llen + 1
                return idx, data, tot, msg
        
        data = fd.read(8192)
        tot = idx = 0
        while tot < content_len:
                key = val = ""
                idx, data, tot, key = read_netstr(idx, data, tot)
                idx, data, tot, val = read_netstr(idx, data, tot)
                yield key, val



def default_partition(key, nr_reduces, params):
        return hash(str(key)) % nr_reduces


def make_range_partition(min_val, max_val):
        r = max_val - min_val
        f = "lambda k, n, p: int(round(float(int(k) - %d) / %d * (n - 1)))" %\
                (min_val, r)
        return eval(f)


def nop_reduce(iter, out, params):
        for k, v in iter:
                out.add(k, v)


def map_line_reader(fd, sze, fname):
        for x in re_reader("(.*?)\n", fd, sze, fname, output_tail = True):
                yield x[0]


def chain_reader(fd, sze, fname):
        for x in netstr_reader(fd, sze, fname):
                yield x


def external(files):
        msg = {"op": file(files[0]).read()}
        for f in files[1:]:
                msg[os.path.basename(f)] = file(f).read()
        return msg


def job(master, name, input_files, fun_map = None, map_reader = map_line_reader,\
        reduce = None, partition = default_partition, combiner = None,\
        nr_maps = None, nr_reduces = None, sort = True, params = Params(),\
        mem_sort_limit = 256 * 1024**2, async = False, clean = True,\
        chunked = None, ext_params = None):

        if len(input_files) < 1:
                raise "Must have at least one input file"

        if re.search("\W", name):
                raise "Only characters in [a-zA-Z0-9_] are allowed in job name"

        inputs = []
        for inp in input_files:
                if inp.startswith("dir://"):
                        inputs += parse_dir(inp)
                else:
                        inputs.append(inp)

        req = {}
        req["name"] = "%s@%d" % (name, int(time.time()))
        req["input"] = " ".join(inputs)
        req["map_reader"] = marshal.dumps(map_reader.func_code)
        if type(fun_map) == dict:
                req["ext_map"] = marshal.dumps(fun_map)
        else:
                req["map"] = marshal.dumps(fun_map.func_code)
        
        if ext_params:
                if type(ext_params) == dict:
                        req["ext_params"] = encode_netstring_fd(ext_params)
                else:
                        req["ext_params"] = ext_params
        
        req["params"] = cPickle.dumps(params)
        req["partition"] = marshal.dumps(partition.func_code)

        if not nr_maps or nr_maps > len(inputs):
                nr_maps = len(inputs)
        req["nr_maps"] = str(nr_maps)
        
        req["sort"] = str(int(sort))
        req["mem_sort_limit"] = str(mem_sort_limit)

        if reduce:
                if type(reduce) == dict:
                        req["ext_reduce"] = marshal.dumps(reduce)
                        req["reduce"] = ""
                else:
                        req["reduce"] = marshal.dumps(reduce.func_code)
                nr_reduces = nr_reduces or max(nr_maps / 2, 1)
                req["chunked"] = "True"
        else:
                nr_reduces = nr_reduces or 1

        if chunked != None:
                if chunked:
                        req["chunked"] = "True"
                elif "chunked" in req:
                        del req["chunked"]

        req["nr_reduces"] = str(nr_reduces)

        if combiner:
                req["combiner"] = marshal.dumps(combiner.func_code)

        msg = encode_netstring_fd(req)
        if master.startswith("stdout:"):
                async = True
                print msg,
                return
        elif master.startswith("debug:"):
                return req
        
        reply = urllib.urlopen(disco_host(master) + DISCO_NEW_JOB, msg)
        r = reply.read()
        if "job started" not in r:
                raise "Failed to start a job. Server replied: " + r
        reply.close()

        if async:
                return req["name"]
        else:
                d = discoapi.Disco(master)
                results = d.wait(req['name'])
                if clean:
                        d.clean(req['name'])
                return results


def result_iterator(results, notifier = None):
        res = []
        for dir_url in results:
                if dir_url.startswith("dir://"):
                        res += parse_dir(dir_url)
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
                        ext_host = host + ":" + HTTP_PORT
                        ext_file = "/" + fname

                        http = httplib.HTTPConnection(ext_host)
                        http.request("GET", ext_file, "")
                        fd = http.getresponse()
                        if fd.status != 200:
                                raise "HTTP error %d" % fd.status
                
                        sze = int(fd.getheader("content-length"))

                if notifier:
                        notifier(url)

                for x in netstr_reader(fd, sze, fname):
                        yield x
                
                if http:
                        http.close()
                else:
                        fd.close()

HTTP_PORT, tmp = load_conf()








                


        


        





        




