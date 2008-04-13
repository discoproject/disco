
from netstring import *
import marshal, traceback, time, re, urllib, httplib
from disco_worker import re_reader, netstr_reader, parse_dir

DISCO_NEW_JOB = "/disco/job/new"
DISCO_CLEAN_JOB = "/disco/ctrl/clean_job"
DISCO_KILL_JOB = "/disco/ctrl/kill_job"
DISCO_RESULTS = "/disco/ctrl/get_results"
HTTP_PORT = "8989"

class JobException(Exception):
        def __init__(self, msg, name, master):
                self.msg = msg
                self.name = name
                self.master = master

        def __str__(self):
                return "Job %s@%s failed: %s" %\
                        (self.name, self.master, self.msg)

def default_partition(key, nr_reduces):
        return hash(str(key)) % nr_reduces

def make_range_partition(min_val, max_val):
        r = max_val - min_val
        f = "lambda k, n: int(round(float(int(k) - %d) / %d * (n - 1)))" %\
                (min_val, r)
        return eval(f)

def map_line_reader(fd, sze, fname):
        for x in re_reader("(.*?)\n", fd, sze, fname, output_tail = True):
                yield x[0]

def chain_reader(fd, sze, fname):
        #for x in re_reader("(.*?) (.*?)\000", fd, sze, fname):
        for x in netstr_reader(fd, sze, fname):
                yield x
        
def job(master, name, input_files, fun_map, map_reader = map_line_reader,\
        reduce = None, partition = default_partition, combiner = None,\
        nr_maps = None, nr_reduces = None, sort = True, params = {},\
        mem_sort_limit = 256 * 1024**2, async = False, clean = True):

        if len(input_files) < 1:
                raise "Must have at least one input file"

        if re.search("\W", name):
                raise "Only characters in [a-zA-Z0-9_] are allowed in job name"

        req = {}
        req["name"] = "%s@%d" % (name, int(time.time()))
        req["input"] = " ".join(input_files)
        req["map_reader"] = marshal.dumps(map_reader.func_code)
        req["map"] = marshal.dumps(fun_map.func_code)
        req["partition"] = marshal.dumps(partition.func_code)
        req["params"] = marshal.dumps(params)

        if not nr_maps or nr_maps > len(input_files):
                nr_maps = len(input_files)
        req["nr_maps"] = str(nr_maps)
        
        req["sort"] = str(int(sort))
        req["mem_sort_limit"] = str(mem_sort_limit)

        if reduce:
                req["reduce"] = marshal.dumps(reduce.func_code)
                nr_reduces = nr_reduces or max(nr_maps / 2, 1)
        else:
                nr_reduces = nr_reduces or 1

        req["nr_reduces"] = str(nr_reduces)

        if combiner:
                req["combiner"] = marshal.dumps(combiner.func_code)

        msg = encode_netstring_fd(req)
        if master.startswith("stdout:"):
                print msg,
        elif master.startswith("disco:"):
                reply = urllib.urlopen(master.replace("disco:", "http:", 1)\
                        + DISCO_NEW_JOB, msg)
                r = reply.read()
                if "job started" not in r:
                        raise "Failed to start a job. Server replied: " + r
                reply.close()
        else:
                raise "Unknown host specifier: %s" % master

        if async:
                return req["name"]
        else:
                results = wait_job(master, req['name'])
                if clean:
                        clean_job(master, req['name'])
                return results

def wait_job(master, name, poll_interval = 5, timeout = None):
        url = master.replace("disco:", "http:", 1)\
                + DISCO_RESULTS + "?name=" + name
        t = time.time()
        while True:
                time.sleep(poll_interval)
                R = urllib.urlopen(url).read()
                R = eval(R)
                if R[0] == "ready":
                        return R[1]
                if R[0] != "active":
                        raise JobException("Job failed", master, name)
                if timeout and time.time() - t > timeout:
                        raise JobException("Timeout", master, name)

def clean_job(master, name):
        urllib.urlopen(master.replace("disco:", "http:", 1)\
                        + DISCO_CLEAN_JOB, '"%s"' % name)

def kill_job(master, name):
        urllib.urlopen(master.replace("disco:", "http:", 1)\
                        + DISCO_KILL_JOB, '"%s"' % name)

def result_iterator(results, notifier = None):
        res = []
        for dir_url in results:
                res += parse_dir(dir_url)
        for url in res:
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

                #for x in re_reader("(.*?) (.*?)\000", fd, sze, fname):
                for x in netstr_reader(fd, sze, fname):
                        yield x
                http.close()







                


        


        





        




