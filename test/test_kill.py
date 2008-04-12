
import tserver, sys, discoapi, disco, time

def data_gen(path):
        return "1 2 3\n"

def fun_map(e, params):
        import time
        time.sleep(100)
        return []

d = discoapi.Disco(sys.argv[1])
num = sum(x['max_workers'] for x in d.nodeinfo()['available'])
print >> sys.stderr, num, "slots available"
tserver.run_server(data_gen)
name = disco.job(sys.argv[1], "test_kill", tserver.makeurl([""] * num * 2),
                        fun_map, async = True)
time.sleep(10)
print >> sys.stderr, "Killing", name
d.kill(name)
time.sleep(5)
if d.jobinfo(name)['active'] == "dead":
        print "ok"
        d.clean(name)
else:
        raise Exception("Killing failed")






