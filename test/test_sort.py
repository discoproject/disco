
import tserver, sys, disco

def data_gen(path):
        return "\n"

def fun_map(e, params):
        import random
        return [(random.randint(0, 2**31), 0) for x in range(100)]

def fun_reduce(iter, out, params):
        prev = 0
        for k, v in iter:
                if k < prev:
                        raise "Val %s < %s -> sorting failed" % (k, prev)
                prev = k
                out.add(k, v)

def run_disco(limit):
        results = disco.job(sys.argv[1], "test_sort",
                                tserver.makeurl([""] * int(1e3)),
                                fun_map, reduce = fun_reduce, nr_reduces = 50,
                                sort = True, mem_sort_limit = limit)
        k = len(list(disco.result_iterator(results)))
        if k != int(1e5): 
                raise "not enough results: Got %d, expected %d" % (k, 1e5)

tserver.run_server(data_gen)

print "Testing in-memory sort"
run_disco(1024**4)
print "in-memory ok"

print "Testing external sort"
run_disco(0)
print "external ok"
print "ok"







