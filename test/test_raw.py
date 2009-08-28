
import sys
from disco.core import Disco, result_iterator

def fun_map(e, params):
        return [("", e + ":map")]

inputs = ["raw://eeny", "raw://meeny", "raw://miny", "raw://moe"]

job = Disco(sys.argv[1]).new_job(name = "test_raw",
        input = inputs,
        map = fun_map)

res = dict((x[6:] + ":map", True) for x in inputs)

for x in result_iterator(job.wait()):
        if x[1] not in res:
                raise "Invalid result: <%s> " % x[1]
        del res[x[1]]

if res:
        raise "Invalid number of results %d" %\
                (len(inputs) - len(res))

job.purge()

print "ok"

