
from disco.core import Disco, result_iterator
import tserver, disco, sys

def data_gen(path):
        return "\n".join([path[1:]] * 10)

def fun_map(e, params):
        return [(e, 1)]

tserver.run_server(data_gen)

inputs = range(1000)
disco = Disco(sys.argv[1])
job = disco.new_job(\
        name = "test_onlymap", 
        input = tserver.makeurl(inputs),
        map = fun_map)

d = {}
for k, v in result_iterator(job.wait()):
        k = int(k)
        v = int(v)
        if k in d:
                d[k] += v
        else:
                d[k] = v

if len(d) != len(inputs):
        raise Exception("Unexpected number of results: Got %d, expected %d" %\
                (len(d), len(inputs)))

for inp in inputs:
        if d[inp] != 10:
                raise Exception("Unexpected result for key %s: %s != 10" %\
                        (inp, d[inp]))

job.purge()
print "ok"
