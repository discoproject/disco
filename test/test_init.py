
import tserver, sys
from disco.core import Params, Disco, result_iterator

def data_gen(path):
        return "skipthis\n" + "\n".join([path[1:]] * 10)

def fun_map(e, params):
        return [(e, int(e) + params.x)]

def fun_reduce(iter, out, params):
        for k, v in iter:
                out.add(k, int(v) + params.y)

def fun_mapinit(input_iter, params):
        input_iter.next()
        params.x += 100

def fun_reduceinit(input_iter, params):
        params.y = 1000

tserver.run_server(data_gen)

inputs = range(10)
job = Disco(sys.argv[1]).new_job(name = "test_init",
                input = tserver.makeurl(inputs),
                map = fun_map, 
                map_init = fun_mapinit,
                params = Params(x = 10),
                reduce = fun_reduce, 
                reduce_init = fun_reduceinit,
                nr_reduces = 1,
                sort = False)

i = 0
for k, v in result_iterator(job.wait()):
        if int(k) + 1110 != int(v):
                raise "Invalid result: %s and %s" % (k, v)
        i += 1

if i != 100:
        raise "Invalid number of results %d, should be 10" % i

job.purge()

print "ok"
