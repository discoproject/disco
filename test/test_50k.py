
import tserver, sys, discoapi, disco

def data_gen(path):
        #import time, random
        #time.sleep(random.random())
        return "Gutta cavat capidem\n" * 500

def fun_map(e, params):
        return [(w, 1) for w in re.sub("\W", " ", e).lower().split()]

def fun_reduce(iter, out, params):
        s = {}
        for k, v in iter:
                if k in s:
                        s[k] += int(v)
                else:
                        s[k] = int(v)
        for k, v in s.iteritems():
                out.add(k, v)

tserver.run_server(data_gen)
name = disco.job(sys.argv[1], "test_50k", tserver.makeurl([""] * int(5e4)),
                       fun_map, reduce = fun_reduce, nr_reduces = 320)

for key, value in disco.result_iterator(results):
	print key, value





