import tserver, sys
from disco.core import util
from disco.core import Disco, result_iterator

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
job = Disco(sys.argv[1]).new_job(name="test_50k",
                        input=tserver.makeurl([""] * int(5e4)),
                        map=fun_map,
                        reduce=fun_reduce,
                        nr_reduces=300,
                        sort=False)

ANS = {"gutta": int(5e6), "cavat": int(1e7), "capidem": int(5e6)}
i = 0
for key, value in result_iterator(job.wait()):
        i += 1
        if ANS[key] == int(value):
                print "Correct: %s %s" % (key, value)
        else:
                raise "Results don't match"
if i != 3:
        raise "Wrong number of results: Got %d expected 3" % i

job.purge()

print "ok"



