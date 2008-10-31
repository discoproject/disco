
import tserver, sys, random, time
from disco import Disco

def data_gen(path):
        return "badger\n" * 100

def fun_map(e, params):
        msg(e)
        return []

tserver.run_server(data_gen)
inputs = tserver.makeurl([1])
job = Disco(sys.argv[1]).new_job(name = "test_ratelimit",
        input = inputs, map = fun_map)

time.sleep(5)

if job.jobinfo()['active'] == "dead":
        print "ok"
        job.purge()
else:
        raise Exception("Rate limit failed")


