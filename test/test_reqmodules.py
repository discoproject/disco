
import tserver, sys, md5, math
from disco import Disco, result_iterator

def data_gen(path):
        return path[1:] + "\n"

def fun_map(e, params):
        return [(md5.new(str(math.pow(int(e), 2))).digest(), "")]

tserver.run_server(data_gen)
disco = Disco(sys.argv[1])

inputs = [1, 485, 3245]
job = disco.new_job(name = "test_reqmodules",
                nr_reduces = 1,
                input = tserver.makeurl(inputs),
                map = fun_map,
                required_modules = ["math", "md5"],
                sort = False)

for i, r in zip(inputs, result_iterator(job.wait())):
        c = md5.new(str(math.pow(int(i), 2))).digest()
        if c != r[0]:
                raise Exception("Invalid answer: Correct: %s Got: %s"\
                        % (c, r[0]))

job.clean()
print "ok"
