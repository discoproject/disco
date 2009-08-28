import tserver, sys, random, time
from disco.core import Disco, result_iterator

def data_gen(path):
        return "\n".join([path[1:]] * 10)

def fun_map(e, params):
        import time, random
        time.sleep(random.randint(1, 3))
        return [(e, 0)]

def fun_reduce(iter, out, params):
        for k, v in iter:
                out.add("[%s]" % k, v)

tserver.run_server(data_gen)

disco = Disco(sys.argv[1])
num = sum(x['max_workers'] for x in disco.nodeinfo()['available'])
print >> sys.stderr, num, "slots available"
inputs = tserver.makeurl(range(num * 10))
random.shuffle(inputs)

jobs = []
for i in range(5):
        jobs.append(disco.new_job(name = "test_async_%d" % i,
                       input = inputs[i * (num * 2):(i + 1) * (num * 2)],
                       map = fun_map, reduce = fun_reduce, nr_reduces = 11,
                       sort = False))
        time.sleep(1)

all = dict(("[%s]" % i, 0) for i in range(num * 10))
while jobs:
        ready, jobs = disco.results(jobs)
        for name, results in ready:
                for k, v in result_iterator(results[1]):
                        all[k] += 1
                disco.purge(name)

for v in all.values():
        if v != 10:
                raise "Invalid results: %s" % all

print "ok"

