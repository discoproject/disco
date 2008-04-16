
import tserver, sys, disco

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

results = disco.job(sys.argv[1], "test_chain_0", tserver.makeurl([""] * 100),
                fun_map, reduce = fun_reduce, nr_reduces = 4,
                sort = False, clean = True, params = {'suffix': '0'})

i = 1
while i < 10:
        results = disco.job(sys.argv[1], "test_chain_%d" % i, results,
                fun_map, reduce = fun_reduce, nr_reduces = 4,
                map_reader = disco.chain_reader,  
                sort = False, clean = True, params = {'suffix': str(i)})
        i += 1

for key, value in disco.result_iterator(results):
        if key[:5] not in ani or key[5:] != "0-1-2-3-4-5-6-7-8-9-":
                raise "Corrupted key: %s" % key
        if value != "9":
                raise "Corrupted value: %s" % value

print "ok"




