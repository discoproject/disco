import tserver, sys
from disco.core import Disco, result_iterator
import random

def data_gen(path):
        return path[1:].replace("_", " ") + "\n"

def fun_map(e, params):
        k, v = e.split(":")
        return [(int(k), v)]

def fun_partition(key, nr_reduces, params):
        return key

def fun_reduce(iter, out, params):
        s = 0
        for k, v in iter:
                s += int(v)
        out.add(k, s)

def fun_reduce_reader(fd, sze, fname):
        for x in re_reader("(.*?)\n", fd, sze, fname, output_tail = True):
                yield (None, x[0])

tserver.run_server(data_gen)

N = 100
results = {}
inputs = []
for i in range(N):
        a = [i] * 10
        b = range(i, i + 10)
        inputs += ["%d:%d" % x for x in zip(a, b)]
        results[str(i)] = str(sum(b))

random.shuffle(inputs)

disco = Disco(sys.argv[1])

print "Running two map jobs.."

map1 = disco.new_job(\
                name = "test_onlyreduce1",
                input = tserver.makeurl(inputs[:len(inputs) / 2]),
                map = fun_map,
                partition = fun_partition,
                nr_reduces = N)

map2 = disco.new_job(\
                name = "test_onlyreduce2",
                input = tserver.makeurl(inputs[len(inputs) / 2:]),
                map = fun_map,
                partition = fun_partition,
                nr_reduces = N)

results1 = map1.wait()
print "map1 done"
results2 = map2.wait()
print "map2 done"

print "reduce1: merge map1 and map2"

red1 = disco.new_job(\
                name = "test_onlyreduce3",
                input = results1 + results2,
                reduce = fun_reduce,
                nr_reduces = N)

for k, v in result_iterator(red1.wait()):
        if results[k] != v:
                raise "Invalid result, got %s, expected %s" %\
                        (v, results[k])
        del results[k]

if results:
        raise "Not enough results"

print "reduce1: ok"

print "reduce2: merge external inputs"

# hack to get tserver to produce netstr-formatted data
inputs = ["100", "20", "2"]

red2 = disco.new_job(\
                name = "test_onlyreduce4",
                input = tserver.makeurl(inputs),
                reduce = fun_reduce,
                reduce_reader = fun_reduce_reader,
                nr_reduces = 1)

s = sum([int(v) for k, v in result_iterator(red2.wait())])
if s != 122:
        raise Exception("Invalid result, got %d, expected 122" % s)

print "reduce2: ok"

map1.purge()
map2.purge()
red1.purge()
red2.purge()

print "ok"
