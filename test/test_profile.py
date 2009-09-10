
import tserver, sys, disco, cStringIO
from disco.core import Disco, result_iterator

def data_gen(path):
        return "Gutta cavat cavat capidem\n" * 100

def really_unique_function_name(e, params):
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

job = Disco(sys.argv[1]).new_job(\
        name = "test_profile",\
        input = tserver.makeurl([""] * int(100)),\
        map = really_unique_function_name,\
        reduce = fun_reduce,\
        nr_reduces = 30,\
        sort = False,\
        profile = True)

ANS = {"gutta": int(1e4), "cavat": int(2e4), "capidem": int(1e4)}
i = 0
for key, value in result_iterator(job.wait()):
        i += 1
        if ANS[key] == int(value):
                print "Correct: %s %s" % (key, value)
        else:
                raise "Results don't match (%s): Got %d expected %d" %\
                        (key, int(value), ANS[key])
if i != 3:
        raise "Too few results"

buf = cStringIO.StringIO()
sys.stdout = buf
job.profile_stats().print_stats()
sys.stdout = sys.__stdout__

#stats = job.profile_stats()
#stats.sort_stats('cumulative')
#stats.print_stats()

if "really_unique_function_name" not in buf.getvalue():
        raise "Corrupted profile results: %s" % buf.getvalue()

job.purge()

print "ok"



