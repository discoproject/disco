import tserver, sys
from disco.core import Disco, result_iterator
from disco.func import map_input_stream

def map_input1(stream, size, url, params):
        r = stream.read()
        fd = cStringIO.StringIO("a" + r)
        return fd, len(r) + 1, "a:%d" % Task.num_partitions

def map_input2(stream, size, url, params):
        r = stream.read()
        fd = cStringIO.StringIO("b" + r)
        return fd, len(r) + 1, url + "b:%d" % Task.num_partitions

def map_input3(stream, size, url, params):
        r = stream.read()
        fd = cStringIO.StringIO("c" + r)
        return fd, len(r) + 1, url + "c:%d" % Task.num_partitions

def map_reader(stream, size, url):
        n = Task.num_partitions
        exp = "a:%db:%dc:%d" % (n, n, n)
        if url != exp:
                err("Invalid url. Got '%s' expected '%s'" % (url, exp))
        yield stream.read()

def reduce_output1(stream, size, url, params):
        return "fd", "url:%d" % Task.num_partitions

def reduce_output2(stream, size, url, params):
        if stream != "fd" or url != "url:%d" % Task.num_partitions:
                err("Incorrect stream (%s) and url (%s) in output_stream2" %\
                        (stream, url))
        path, url = Task.reduce_output()
        return disco.fileutils.AtomicFile(path, "w"),\
                url.replace("disco://", "foobar://")

def resultiter_input1(stream, size, url, params):
        if not url.startswith("foobar://"):
                raise Exception("Invalid url %s (expected foobar://))" % url)
        return stream, size, url.replace("foobar://", "disco://")

def fun_map(e, params):
        return [(e, "")]

def fun_reduce(iter, out, params):
        for k, v in iter:
                out.add("red:" + k, v)

def data_gen(path):
        return path[1:]

tserver.run_server(data_gen)

inputs = ["apple", "orange", "pear"]

job = Disco(sys.argv[1]).new_job(
        name="test_streams",
        input=tserver.makeurl(inputs),
        map=fun_map,
        reduce=fun_reduce,
        nr_reduces=1,
        map_reader = map_reader,
        map_input_stream =
                [map_input_stream, map_input1, map_input2, map_input3],
        reduce_output_stream = [reduce_output1, reduce_output2])

for k, v in result_iterator(job.wait(),
                input_stream = [resultiter_input1, map_input_stream]):

        if not k.startswith("red:cba"):
                raise Exception("Invalid prefix in key. Got '%s' "\
                        "expected prefix 'red:cba'" % k)

        if k[7:] not in inputs:
                raise Exception("Invalid result '%s'" % k)
        inputs.remove(k[7:])

if inputs:
        raise Exception("Expected 3 results, got %d" % 3 - len(inputs))

print "ok"
















