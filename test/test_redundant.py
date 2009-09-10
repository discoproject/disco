import tserver, sys, thread
from disco.core import Disco, result_iterator

lock = thread.allocate_lock()
fail = ["1", "2", "3"]

def data_gen(path):
        if "fail" in path:
                raise tserver.FailedReply()
        else:
                return str(int(path[2:]) * 10) + "\n"

def fun_map(e, params):
        return [(e, "")]

def fun_reduce(iter, out, params):
        s = 0
        for k, v in iter:
                s += int(k)
        out.add(s, "")

tserver.run_server(data_gen)

inputs = ["X1", ["2_fail", "2_still_fail", "X200"], "X3", ["4_fail", "X400"]]

job = Disco(sys.argv[1]).new_job(
        name = "test_redundant",
        input = tserver.makeurl(inputs),
        map = fun_map,
        reduce = fun_reduce,
        nr_reduces = 1)

if result_iterator(job.wait()).next()[0] != "6040":
        raise Exception("Invalid result: Got %s, expected 6040" % res)

job.purge()
print "ok"

