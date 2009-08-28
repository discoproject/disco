
import tserver, sys, time
from disco.core import Disco

def data_gen(path):
        return "1 2 3\n"

def fun_map(e, params):
        import time
        time.sleep(100)
        return []

disco = Disco(sys.argv[1])
num = sum(x['max_workers'] for x in disco.nodeinfo()['available'])
print >> sys.stderr, num, "slots available"
tserver.run_server(data_gen)
job = disco.new_job(name = "test_kill",
        input = tserver.makeurl([""] * num * 2), map = fun_map)

time.sleep(10)
print >> sys.stderr, "Killing", job.name
job.kill()
time.sleep(5)
if job.jobinfo()['active'] == "dead":
        print "ok"
        job.purge()
else:
        raise Exception("Killing failed")






