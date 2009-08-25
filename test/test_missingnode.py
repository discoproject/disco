import sys
import tserver
from disco.comm import json
from disco.core import Disco, result_iterator

def data_gen(path):
        return path[1:]

def fun_map(e, params):
        # sleep some time, otherwise nodes may finish so fast
        # that the missing node won't be used at all.
        time.sleep(0.5)
        return [(int(e), "")]

def add_node():
        orig_config = json.loads(
                disco.request("/disco/ctrl/load_config_table"))
        config = orig_config[:]
        config.append(["missingnode", "2"])
        r = disco.request("/disco/ctrl/save_config_table",
                json.dumps(config))
        if r != "\"table saved!\"":
                raise Exception("Couldn't add a dummy node: %s" % r)
        return orig_config

def test():
        num = sum(x['max_workers'] for x in disco.nodeinfo()['available'])
        inputs = range(num * 2)
        job = disco.new_job(
                name = "test_missingnode",
                map = fun_map,
                input = tserver.makeurl(inputs))
        results = job.wait()
        s = sum(int(k) for k, v in result_iterator(results))
        correct = sum(range(num * 2))
        if s != correct:
                raise Exception("Invalid result. Got %d, expected %d" %\
                        (s, correct))
        job.purge()
        

disco = Disco(sys.argv[1])
tserver.run_server(data_gen)
orig_config = add_node()

try:
        test()
except:
        raise
finally:
        disco.request("/disco/ctrl/save_config_table",
                json.dumps(orig_config))

print "ok"

