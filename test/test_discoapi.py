
import sys
from disco import Disco, result_iterator

def fun_map(e, params):
        for i in range(3):
                msg("--special_test_string_%d--" % i)
        return [(e, "")]

inputs = ["raw://discoapi"]

job = Disco(sys.argv[1]).new_job(name = "test_discoapi",
        input = inputs,
        map = fun_map)

r = list(result_iterator(job.wait()))
if [("discoapi", "")] != r:
        raise Exception("Invalid result: <%s> " % r)

n = job.jobspec()["name"]
if not n.startswith("test_discoapi"):
        raise Exception("Invalid jobspec: Expected name prefix test_discoapi, "\
                        "got %s" % n)

events = [ev[2] for offs, ev in job.events()]

for i in range(3):
        m = "--special_test_string_%d--" % i
        if not [x for x in events if m in x]:
                raise Exception("Message '%s' not found in events" % m)

job.purge()

print "ok"

