
import tserver, sys
from disco.core import Params, Disco, result_iterator

def fun1(a, b):
        return a + b

def fun2(x):
        if x > 10:
                return 1
        else:
                return 0

def data_gen(path):
        return "\n".join([path[1:]] * 10)

def fun_map(e, params):
        return [(e, params.f1(int(e), params.x))]

def fun_reduce(iter, out, params):
        for k, v in iter:
                out.add(k, params.f2(int(v)))

tserver.run_server(data_gen)

inputs = range(10)
job = Disco(sys.argv[1]).new_job(name = "test_params",
                input = tserver.makeurl(inputs),
                map = fun_map, 
                params = Params(x = 5, f1 = fun1, f2 = fun2),
                reduce = fun_reduce, 
                nr_reduces = 1,
                sort = False)

for x, y in result_iterator(job.wait()):
        if fun2(int(x) + 5) != int(y):
                raise "Invalid result: %s and %s" % (x, y)

job.purge()

print "ok"
