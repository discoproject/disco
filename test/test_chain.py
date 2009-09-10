
import tserver, sys
from disco.core import Disco, result_iterator
from disco.func import chain_reader
from disco.util import jobname

ani = ["horse", "sheep", "whale", "tiger"]

def data_gen(path):
        return "\n".join(ani)

def fun_map(e, params):
        if type(e) == tuple:
                return [(e[0] + params['suffix'], int(e[1]) + 1)]
        else:
                return [(e + params['suffix'], 0)]

def fun_reduce(iter, out, params):
        for k, v in iter:
                out.add(k + "-", v)

tserver.run_server(data_gen)
disco = Disco(sys.argv[1])

results = disco.new_job(name = "test_chain_0", input = tserver.makeurl([""] * 100),
                map = fun_map, reduce = fun_reduce, nr_reduces = 4,
                sort = False, params = {'suffix': '0'}).wait()

i = 1
while i < 10:
        nresults = disco.new_job(name = "test_chain_%d" % i, input = results,
                map = fun_map, reduce = fun_reduce, nr_reduces = 4,
                map_reader = chain_reader, sort = False,
                params = {'suffix': str(i)}).wait()

        disco.purge(jobname(results[0]))
        results = nresults
        i += 1

for key, value in result_iterator(results):
        if key[:5] not in ani or key[5:] != "0-1-2-3-4-5-6-7-8-9-":
                raise "Corrupted key: %s" % key
        if value != "9":
                raise "Corrupted value: %s" % value

disco.purge(jobname(results[0]))

print "ok"




