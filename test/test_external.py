
import tserver, disco, sys

def data_gen(path):
        return "test_%s\n" % path[1:]

def fun_reduce(iter, out, params):
        for k, v in iter:
                out.add("red_" + k, "red_" + v)
        
tserver.run_server(data_gen)

inputs = ["dog", "cat", "ape"]
params = {"test1": "1,2,3",\
          "one two three": "dim\ndam\n",\
          "dummy": "value"}

results = disco.job(sys.argv[1], "test_external", tserver.makeurl(inputs),
                fun_map = disco.external(["ext_test"]), 
		reduce = fun_reduce, 
                ext_params = params,
		nr_reduces = 1,
		sort = False)

results = list(disco.result_iterator(results))
for k, v in results: 
        if k != "red_dkey" and v != "red_test_dog":
                raise Exception("Invalid answer: %s, %s" % (k, v))

if len(results) != 9:
        raise Exception("Wrong number of results: %u vs. 9" % len(results))

print "ok"
