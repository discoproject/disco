import tserver, sys
from disco.core import Disco, result_iterator

ANS = "1028380578493512611198383005758052057919386757620401"\
      "58350002406688858214958513887550465113168573010369619140625"

def data_gen(path):
        return "\n".join([path[1:]] * 10)

def fun_map(e, params):
        return [('=' + e, e)]

def fun_reduce(iter, out, params):
        s = 1
        for k, v in iter:
                if k != "=" + v:
                        raise Exception("Corrupted key")
                s *= int(v)
        out.add("result", s)

tserver.run_server(data_gen)

inputs = [3, 5, 7, 11, 13, 17, 19, 23, 29, 31]
job = Disco(sys.argv[1]).new_job(name="test_simple",
                                 input=tserver.makeurl(inputs),
                                 map=fun_map,
                                 reduce=fun_reduce,
                                 nr_reduces=1,
                                 sort=False)

if list(result_iterator(job.wait())) != [("result", ANS)]:
        raise Exception("Invalid answer")

job.purge()
print "ok"
