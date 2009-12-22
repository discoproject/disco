import sys
from disco.core import Disco, result_iterator, Params
from disco.error import JobError

def map_input_stream(stream, size, url, params):
        host, fname = url[9:].split("/", 1)
        if (host == Task.host) != params.islocal:
                err("Input is at %s but the node is %s "
                    "when locality was forced to be %s" %
                        (host, Task.host, params.islocal))
        return cStringIO.StringIO(host), len(host), url

def fun_map(e, params):
        time.sleep(0.2)
        return [(e, "")]

def run(locality, inputs, name):
        key = {True: "force_local", False: "force_remote"}
        return Disco(sys.argv[1]).new_job(
                name = "test_%s" % name,
                input = inputs,
                map = fun_map,
                params = Params(islocal = locality),
                scheduler = {key[locality]: True},
                map_input_stream = map_input_stream)

def check_results(job, nodes):
        seen = {}
        for k, v in result_iterator(job.wait()):
                if k in seen:
                        seen[k] += 1
                else:
                        seen[k] = 1

        if len(seen) != len(nodes):
                raise "Expected to see %d nodes, got %d" % (len(nodes), len(seen))

        for node, c in seen.iteritems():
                if nodes[node] * 2 != c:
                        raise "Node %s was seen only %d times, expected %d" %\
                                        (node, seen[node], nodes[node])
        job.purge()

def check_fail(job, mode):
        try:
                job.wait()
                if mode:
                        raise "Input at an unknown node should "\
                              "not work when force_local = True"
                else:
                        job.purge()
        except JobError:
                if mode:
                        job.purge()
                else:
                        raise "Input at an unknown node should "\
                              "work when force_remote = True"

disco = Disco(sys.argv[1])
nodes = dict((n['node'], n['max_workers'])
                for n in disco.nodeinfo()['available']
                        if not n['blacklisted'])

inputs = []
for n, m in nodes.iteritems():
        inputs += ["foobar://%s/" % n] * m * 2

check_results(run(True, inputs, "forcelocal"), nodes)
check_results(run(False, inputs, "forceremote"), nodes)
check_fail(run(True, ["disco://nonexistent_node/"], "forcelocal_nonode"), True)
check_fail(run(False, ["disco://nonexistent_node/"], "forceremote_nonode"), False)

print "ok"








