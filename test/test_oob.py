
import tserver, sys, string
from disco.core import Params, Disco, result_iterator

def data_gen(path):
        return path[1:]

def fun_map(e, params):
        v = "value:" + e
        put(e, v)
        return [(e, v)]

def fun_reduce(iter, out, params):
        for k, v in iter:
                x = get(k)
                if x != v:
                        raise "Invalid value for key (%s): "\
                              "got (%s) expected (%s)" % (k, x, v)
        x = "reduce:%d" % this_partition()
        put(x, "value:" + x)
        out.add("all", "ok")

def fun_map2(e, params):
        x = get(e, params.job)
        if x != "value:" + e:
                raise "Invalid value for key %s: %s" % (e, x)
        return [("good", "")]

tserver.run_server(data_gen)

inputs = list(string.ascii_lowercase)
disco = Disco(sys.argv[1])

job1 = disco.new_job(name = "test_oob1",
        input = tserver.makeurl(inputs),
        map = fun_map,
        reduce = fun_reduce,
        nr_reduces = 10)

res = list(result_iterator(job1.wait()))
if [("all", "ok")] * 10 != res:
        raise "Invalid result: %s" % res

keys = ["reduce:%d" % i for i in range(10)] + inputs
lst = job1.oob_list()

if len(lst) != len(keys):
        raise "Invalid number of OOB keys: got %d, expected %d" %\
                (len(lst), len(keys))

for key in job1.oob_list():
        if key not in keys:
                raise "Invalid key: %s" % key
        x = job1.oob_get(key)
        if x != "value:" + key:
                raise "Invalid value: got '%s', expected '%s'" %\
                        (x, "value:" + key)

job2 = disco.new_job(name = "test_oob2",
        input = ["raw://a", "raw://b", "raw://c"],
        map = fun_map2,
        params = Params(job = job1.name))

res = list(result_iterator(job2.wait()))
if [("good", "")] * 3 != res:
        raise "Invalid result: %s" % res

job1.purge()
job2.purge()

print "ok"

