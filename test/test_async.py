
import tserver, sys, discoapi, disco, random, time

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

d = discoapi.Disco(sys.argv[1])
num = sum(x['max_workers'] for x in d.nodeinfo()['available'])
print >> sys.stderr, num, "slots available"
inputs = tserver.makeurl(range(num * 10))
random.shuffle(inputs)

jobs = []
for i in range(5):
        jobs.append(disco.job(sys.argv[1], "test_queue_%d" % i,
                       inputs[i * (num * 2):(i + 1) * (num * 2)],
                       fun_map, reduce = fun_reduce, nr_reduces = 11,
                       sort = False, async = True))
        time.sleep(1)

all = dict(("[%s]" % i, 0) for i in range(num * 10))
for job in jobs:
        results = d.wait(job)
        print "Job", job, "done"
        for k, v in disco.result_iterator(results):
                all[k] += 1

for v in all.values():
        if v != 10:
                raise "Invalid results: %s" % all

print "ok"

