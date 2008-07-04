
import tserver, sys, discoapi, disco, random, time

def data_gen(path):
        return "badger\n" * 100

def fun_map(e, params):
        msg(e)
        return []

tserver.run_server(data_gen)
inputs = tserver.makeurl([1])
name = disco.job(sys.argv[1], "test_ratelimit", inputs, fun_map, async = True)

time.sleep(5)

d = discoapi.Disco(sys.argv[1])
if d.jobinfo(name)['active'] == "dead":
        print "ok"
        d.clean(name)
else:
        raise Exception("Rate limit failed")


