
import tserver, sys, random, time
from disco.core import Disco

def check_dead(job):
        if job.jobinfo()['active'] == "dead":
                job.purge()
        else:
                raise Exception("Rate limit failed")

def data_gen(path):
        return "badger\n" * 1000000

def fun_map(e, params):
        msg(e)
        return []

def fun_map2(e, params):
        return []

tserver.run_server(data_gen)
inputs = tserver.makeurl([1])
job = Disco(sys.argv[1]).new_job(name = "test_ratelimit",
        input = inputs, map = fun_map)

time.sleep(5)
check_dead(job)

job = Disco(sys.argv[1]).new_job(name = "test_ratelimit2",
        input = inputs, map = fun_map2, status_interval = 1)

time.sleep(5)
check_dead(job)

job = Disco(sys.argv[1]).new_job(name = "test_ratelimit3",
        input = inputs, map = fun_map2, status_interval = 0)
job.wait()
job.purge()

print "ok"



