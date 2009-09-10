import tserver, sys, thread
from disco.core import Disco, result_iterator

lock = thread.allocate_lock()
fail = ["1", "2", "3"]

def data_gen(path):
        lock.acquire()
        e = path[1:]
        if e in fail:
                fail.remove(e)
                lock.release()
                raise tserver.FailedReply()
        else:
                lock.release()
                return str(int(e) * 10) + "\n"

def fun_map(e, params):
        return [(int(e) * 10, "")]

tserver.run_server(data_gen)

job = Disco(sys.argv[1]).new_job(
        name = "test_tempfail",
        input = tserver.makeurl(map(str, range(10))),
        map = fun_map)

res = sum(int(x) for x, y in result_iterator(job.wait()))
if res != 4500:
        raise Exception("Invalid result: Got %d, expected 4500" % res)

job.purge()
print "ok"

