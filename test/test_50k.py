
import tserver, sys, disco

def data_gen(path):
        return "Gutta cavat cavat capidem\n" * 100

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
results = disco.job(sys.argv[1], "test_50k", tserver.makeurl([""] * int(5e4)),
                       fun_map, reduce = fun_reduce, nr_reduces = 300,
                       sort = False)

ANS = {"gutta": int(5e6), "cavat": int(1e7), "capidem": int(5e6)}
i = 0
for key, value in disco.result_iterator(results):
        i += 1
        if ANS[key] == int(value):
                print "Correct: %s %s" % (key, value)
        else:
                raise "Results don't match"
if i != 3:
        raise "Too few results"
                

print "ok"



