
import tserver, disco, sys

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
results = disco.job(sys.argv[1], "test_params", tserver.makeurl(inputs),
                fun_map, 
                params = disco.Params(x = 5, f1 = fun1, f2 = fun2),
		reduce = fun_reduce, 
		nr_reduces = 1,
		sort = False)

for x, y in disco.result_iterator(results):
        if fun2(int(x) + 5) != int(y):
                raise "Invalid result: %s and %s" % (x, y)

print "ok"
