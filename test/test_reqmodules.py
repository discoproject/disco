
import tserver, sys, base64, math
from disco import Disco, result_iterator

def data_gen(path):
        return path[1:] + "\n"

def fun_map(e, params):
        k = str(int(math.ceil(float(e))) ** 2)
        return [(base64.encodestring(k), "")]

tserver.run_server(data_gen)
disco = Disco(sys.argv[1])

inputs = [1, 485, 3245]
job = disco.new_job(name = "test_reqmodules",
                nr_reduces = 1,
                input = tserver.makeurl(inputs),
                map = fun_map,
                sort = False)

res = list(result_iterator(job.wait()))
if len(res) != len(inputs):
        raise Exception("Too few results: Got: %d Should be %d" %
                (len(res), len(inputs)))

cor = map(lambda x: base64.encodestring(str(int(math.ceil(x)) ** 2)), inputs)

for k, v in res:
        if k not in cor:
                raise Exception("Invalid answer: %s" % k)
        cor.remove(k)	

job.purge()
print "ok"
