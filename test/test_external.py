import tserver, sys
from disco.core import Disco, result_iterator
from disco.util import external

def data_gen(path):
        return "test_%s\n" % path[1:]

def fun_reduce(iter, out, params):
        for k, v in iter:
                out.add("red_" + k, "red_" + v)
        
tserver.run_server(data_gen)

inputs = ["ape", "cat", "dog"]
params = {"test1": "1,2,3",\
          "one two three": "dim\ndam\n",\
          "dummy": "value"}

job = Disco(sys.argv[1]).new_job(
            name = "test_external",
            input = tserver.makeurl(inputs),
            map = external(["ext_test"]), 
            reduce = fun_reduce, 
            ext_params = params,
            nr_reduces = 1,
            sort = False)

results = sorted([(v, k) for k, v in result_iterator(job.wait())])
for i, e in enumerate(results): 
        v, k = e
        if k != "red_dkey" or v != "red_test_%s" % inputs[i / 3]:
                raise Exception("Invalid answer: %s, %s" % (k, v))

if len(results) != 9:
        raise Exception("Wrong number of results: %u vs. 9" % len(results))

job.purge()

print "ok"
