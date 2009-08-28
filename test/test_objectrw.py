
import tserver, sys, math
from disco.core import Params, Disco, result_iterator, func

def data_gen(path):
        return "\n".join([path[1:]] * 10)

def fun_map(e, params):
        return [({"PI": math.pi}, time.strptime(e, "%d/%m/%Y"))]

def fun_reduce(iter, out, params):
        for k, v in iter:
                out.add({"PI2": k["PI"]}, datetime.datetime(*v[0:6]))

tserver.run_server(data_gen)

inputs = ["01/11/1965", "14/03/1983", "12/12/2002"]

job = Disco(sys.argv[1]).new_job(name = "test_objectrw",
                input = tserver.makeurl(inputs),
                map = fun_map,
                map_writer = func.object_writer,
                reduce = fun_reduce, 
                reduce_reader = func.object_reader,
                reduce_writer = func.object_writer,
                required_modules = ["math", "datetime", "time"],
                nr_reduces = 1,
                sort = False)

i = 0
for k, v in result_iterator(job.wait(), reader = func.object_reader):
        if k["PI2"] != math.pi:
                raise "Invalid key: %s" % k
        if v.strftime("%d/%m/%Y") not in inputs:
                raise "Invalid value: %s" % v
        i += 1

if i != 30:
        raise "Wrong number of results, got %d, expected 30" % i

job.purge()

print "ok"
