import os, sys, imp, cStringIO
from disco.netstring import decode_netstring_fd
from disco.core import Job, result_iterator, util

def parse_dir(dir):
    x, x, LOCAL_PATH = util.load_conf()
    x, x, x, mode, path = dir.split("/", 4)
    res = []
    for f in os.listdir(LOCAL_PATH + path):
        if not (f.startswith(mode) or "." in f):
            continue
        proto = f.split("-")[1]
        if proto == "chunk":
            res.append("chunkfile://%s%s" % (path, f))
        elif proto == "disco":
            res.append("disco://localhost/%s%s" % (path, f))
    return res

class MsgStream:
    def __init__(self):
        self.out = []
    def write(self, msg):
        if msg.startswith("**<OUT>"):
            addr = msg.split()[-1]
            self.out = parse_dir(addr)
        print msg,

class DummyDisco:
    def request(*args, **kwargs):
        return "job started"

class HomeDisco:
    
    def __init__(self, mode, partition = "0"):
        self.mode = mode
        self.partition = partition
    
    def new_job(self, *args, **kwargs):
        job = Job(DummyDisco(), **kwargs)
        req = decode_netstring_fd(cStringIO.StringIO(job.msg))

        argv_backup = sys.argv[:]
        out_backup = sys.stderr
        sys.argv = ["", "", job.name, "localhost",
                "http://nohost", self.partition]
        sys.argv += kwargs["input"]
        
        from disco.node import worker

        sys.stderr = out = MsgStream()
        try:
            if self.mode == "map":
                worker.op_map(req)
            elif self.mode == "reduce":
                worker.op_reduce(req)
            else:
                raise "Unknown mode: %s "\
                      "(must be 'map' or 'reduce')"\
                    % self.mode
        finally:
            sys.argv = argv_backup
            sys.stderr = out_backup
        return out.out

if __name__ == "__main__":

    def fun_map(e, params):
        return [(e, e)]
    
    def fun_reduce(iter, out, params):
        for k, v in iter:
            out.add("red:" + k, v)
    
    f = file("homedisco-test", "w")
    print >> f, "dog\ncat\npossum"
    f.close()

    map_hd = HomeDisco("map")
    reduce_hd = HomeDisco("reduce")
    
    res = map_hd.new_job(name = "homedisco",
                 input = ["homedisco-test"],
                 map = fun_map,
                 reduce = fun_reduce)
    
    res = reduce_hd.new_job(name = "homedisco",
                input = res,
                map = fun_map,
                reduce = fun_reduce)

    for k, v in result_iterator(res):
        print "KEY", k, "VALUE", v

