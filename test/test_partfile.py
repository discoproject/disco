
import tserver, sys
from disco.core import Disco, result_iterator

def data_gen(path):
        return path[1:] + "\n"

def fun_map(e, params):
        k, v = e.split(":")
        return [(k, v)]

def check_results(job):
        res = job.wait()
        s = {}
        for k, v in result_iterator(res):
                if k in s:
                        s[k] += int(v)
                else:
                        s[k] = int(v)
        if len(s) != len(results):
                raise "%s: Invalid number of keys, got %d expected %d" %\
                        (job.name, len(s), len(results))
        for k, v in s.iteritems():
                if v != results[k]:
                        raise "%s: Invalid result for key %s, got %s, "\
                        "expected %s" % (job.name, k, v, results[k])

tserver.run_server(data_gen)

N = 10
results = {}
inputs = []
for i in range(N):
        a = [i] * 10
        b = range(i, i + 10)
        inputs += ["%d:%d" % x for x in zip(a, b)]
        results[str(i)] = sum(b)

disco = Disco(sys.argv[1])

# map results in individual files, one per input file (default mode)
job1 = disco.new_job(\
                name = "test_partfile1",
                input = tserver.makeurl(inputs),
                map = fun_map)

# map results in one big partition file per host
job2 = disco.new_job(\
                name = "test_partfile2",
                input = tserver.makeurl(inputs),
                map = fun_map,
                nr_reduces = 1)

check_results(job1)
check_results(job2)
job1.purge()
job2.purge()

print "ok"
