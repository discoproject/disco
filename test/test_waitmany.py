
import tserver, sys
from disco.core import Disco, result_iterator

def data_gen(path):
        return "foo"

def fun_map1(e, params):
        time.sleep(2)
        return []

def fun_map2(e, params):
        time.sleep(3)
        return []

def fun_map3(e, params):
        fail

def fun_map4(e, params):
        time.sleep(4)
        return []

tserver.run_server(data_gen)
disco = Disco(sys.argv[1])

jobs = []
for i, m in enumerate([fun_map1, fun_map2, fun_map3, fun_map4]):
        jobs.append(disco.new_job(
                name = "test_waitmany_%d" % (i + 1),
                input = tserver.makeurl([""] * 5),
                map = m))

res = []
while jobs:
        cont = False
        ready, jobs = disco.results(jobs, timeout = 2000)
        res += ready

for n, r in res:
        if n.startswith("test_waitmany_3"):
                if r[0] != "dead":
                        raise Exception("Invalid job status: %s" % n)
        elif r[0] != "ready":
                raise Exception("Invalid job status: %s" % n)
        disco.purge(n)

if len(res) != 4:
        raise Exception("Invalid number of results")






print "ok"
                        
         





