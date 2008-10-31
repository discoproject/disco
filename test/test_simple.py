
import tserver, disco, sys

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
results = disco.job(sys.argv[1], "test_simple", tserver.makeurl(inputs),
                fun_map, 
                reduce = fun_reduce, 
                nr_reduces = 1,
                sort = False)

if list(disco.result_iterator(results)) != [("result", ANS)]:
        raise Exception("Invalid answer")

print results

disco.Disco(sys.argv[1]).purge(disco.util.jobname(results[0]))

print "ok"
