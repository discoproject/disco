import tserver, sys
from disco.core import Disco, result_iterator

ANS = "1028380578493512611198383005758052057919386757620401"\
      "58350002406688858214958513887550465113168573010369619140625"

def data_gen(path):
        return "\n".join([path[1:]] * 10)

def fun_map(e, params):
        return [('=' + e, e)]

def fun_map_writer(fd, key, value, params):
        fd.write("%s|%s\n" % (key, value))

def fun_reduce_reader(fd, sze, fname):
        for x in re_reader("(.*?)\n", fd, sze, fname, output_tail = True):
                yield x[0].split("|")

def fun_reduce_writer(fd, key, value, params):
        fd.write("<%s>" % value)

def result_reader(fd, sze, fname):
        yield fd.read(sze)[1:-1]

def fun_reduce(iter, out, params):
        s = 1
        for k, v in iter:
                if k != "=" + v:
                        raise Exception("Corrupted key")
                s *= int(v)
        out.add("result", s)

tserver.run_server(data_gen)

inputs = [3, 5, 7, 11, 13, 17, 19, 23, 29, 31]

job = Disco(sys.argv[1]).new_job(
                name = "test_writers", 
                input = tserver.makeurl(inputs),
                map = fun_map,
                map_writer = fun_map_writer,
                reduce = fun_reduce, 
                reduce_reader = fun_reduce_reader,
                reduce_writer = fun_reduce_writer,
                nr_reduces = 1,
                sort = False)

res = list(result_iterator(job.wait(), reader = result_reader))

if res != [ANS]:
        raise Exception("Invalid answer: %s" % res)

job.purge()

print "ok"
