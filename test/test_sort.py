import tserver, sys, string, base64
from disco.core import Disco, result_iterator

def data_gen(path):
        return "\n"

def fun_map(e, params):
        r = []
        for x in string.ascii_lowercase:
                r += list(x * 10)
        for x in range(10):
                r += [str(i) for i in range(10)]
        random.shuffle(r)
        # test base64 encoding at the same time
        # no other reason for using base64 here
        return [(base64.encodestring(x), '') for x in r]

def fun_reduce(iter, out, params):
        d = {}
        for k, v in itertools.groupby(iter, key = lambda x: x[0]):
                if k in d:
                        raise Exception("Key %s seen already. "\
                                "Sorting failed." % k)
                d[k] = len(list(v))
        for k, v in d.iteritems():
                out.add(k, v)

def run_disco(limit, name):
        job = disco.new_job(
                        name = "test_sort_%s" % name,
                        input = tserver.makeurl([""] * int(100)),
                        map = fun_map,
                        reduce = fun_reduce,
                        nr_reduces = 1,
                        sort = True,
                        mem_sort_limit = limit)

        ANS = dict((str(x), True)\
                for x in list(string.ascii_lowercase) + range(10))

        for k, v in result_iterator(job.wait()):
                if v != "1000":
                        raise Exception("Incorrect result: "\
                                "Expected 1000, got %s" % v)
                del ANS[base64.decodestring(k)]
        if ANS:
                raise Exception("Missing keys: %s" % " ".join(ANS.keys()))
        job.purge()

disco = Disco(sys.argv[1])
tserver.run_server(data_gen)

print "Testing in-memory sort"
run_disco(1024**4, "inmemory")
print "in-memory ok"

print "Testing external sort"
run_disco(0, "external")
print "external ok"
print "ok"







