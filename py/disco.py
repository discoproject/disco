
from netstring import *
import marshal, traceback, time, re, urllib

DISCO_NEW_JOB_URL = "/disco/job/new"

def default_partition(key, nr_reduces):
        return hash(str(key)) % nr_reduces

def make_range_partition(min_val, max_val):
        r = max_val - min_val
        f = "lambda k, n: int(round(float(int(k) - %d) / %d * (n - 1)))" %\
                (min_val, r)
        return eval(f)

def map_line_reader(fd, sze, fname):
        for x in re_reader("(.*?)\n", fd, sze, fname):
                yield x[0]
        
def job(master, name, input_files, fun_map, map_reader = map_line_reader,\
        reduce = None, partition = default_partition, combiner = None,\
        nr_maps = None, nr_reduces = None, sort = True,\
        mem_sort_limit = 256 * 1024**2, async = False):

        if len(input_files) < 1:
                raise "Must have at least one input file"

        req = {}
        req["name"] = "%s@%d" % (name, int(time.time()))
        req["input"] = " ".join(input_files)
        req["map_reader"] = marshal.dumps(map_reader.func_code)
        req["map"] = marshal.dumps(fun_map.func_code)
        req["partition"] = marshal.dumps(partition.func_code)

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
                        + DISCO_NEW_JOB_URL, msg)
                r = reply.read()
                if "job started" not in r:
                        raise "Failed to start a job. Server replied: " + r
                reply.close()
        else:
                raise "Unknown host specifier: %s" % master

        if not async:
                return wait_job(master, name)
        else:
                return name

def wait_job(master, name):
        pass
                


        


        





        




