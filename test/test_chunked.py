
import tserver, sys
from disco import Disco, result_iterator

ANS = "1028380578493512611198383005758052057919386757620401"\
      "58350002406688858214958513887550465113168573010369619140625"

def data_gen(path):
        return path[1:] + "\n"

def fun_map(e, params):
        k, v = e.split(":")
        return [(int(k), v)]

def fun_partition(key, nr_reduces, params):
        return key

def fun_reduce(iter, out, params):
        s = 0
        for k, v in iter:
                s += int(v)
        out.add(k, s)

tserver.run_server(data_gen)

N = 100
results = {}
inputs = []
for i in range(N):
        a = [i] * 10
        b = range(i, i + 10)
        inputs += ["%d:%d" % x for x in zip(a, b)]
        results[str(i)] = str(sum(b))

disco = Disco(sys.argv[1])
job = disco.new_job(\
                name = "test_chunked",
                input = tserver.makeurl(inputs),
                map = fun_map,
                partition = fun_partition,
                reduce = fun_reduce,
		chunked = False, 
                nr_reduces = N,
                sort = False)

for k, v in result_iterator(job.wait()):
        if results[k] != v:
                raise "Invalid result, got %s, expected %s" %\
                        (v, results[k])
        del results[k]

if results:
        raise "Not enough results"

job.purge()

print "ok"
