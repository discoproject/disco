
import sys, tserver
from disco.core import Disco, result_iterator

def data_gen(path):
        return "\n".join([path[1:]])

def fun_map(e, params):
        x = extramodule1.magic(int(e))
        y = extramodule2.kungfu(x)
        return [("", y)]

inputs = ["123"]

tserver.run_server(data_gen)

job = Disco(sys.argv[1]).new_job(name = "test_requiredfiles",
        input = tserver.makeurl(inputs),
        required_files = ["extramodule1.py", "extramodule2.py"],
        required_modules = ["extramodule1", "extramodule2"],
        map = fun_map)

exp = int(inputs[0]) ** 2 + 2
got = int([y for x, y in result_iterator(job.wait())][0])
if exp != got:
        raise "Wrong result! expected %d, got %d" % (exp, got)

job.purge()

print "ok"

