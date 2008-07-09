import os
os.environ['DISCO_HOME'] = "./"
import sys, disco, disco_worker
from netstring import decode_netstring_fd

class MsgStream:
        def __init__(self):
                self.out = []
        def write(self, msg):
                if msg.startswith("**<OUT>"):
                        addr = msg.split()[-1]
                        fname = "/".join(addr.split("/")[-2:])
                        if addr.startswith("chunk://"):
                                self.out.append("chunkfile://" + fname)
                        else:
                                self.out.append("file://" + fname)
                print msg,

class HomeDisco:
        
        def __init__(self, mode, partition = "0"):
                self.mode = mode
                self.partition = partition
        
        def job(self, *args, **kwargs):
                args = list(args)
                args[0] = "debug:"
                req = disco.job(*args, **kwargs)

                argv_backup = sys.argv[:]
                out_backup = sys.stderr
                sys.argv = ["", "", "", "", "", ""]
                sys.argv[3] = "localhost"
                sys.argv[5] = self.partition
                sys.argv += args[2]
                
                sys.stderr = out = MsgStream()
                try:
                        if self.mode == "map":
                                disco_worker.op_map(req)
                        elif self.mode == "reduce":
                                disco_worker.op_reduce(req)
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
        res = map_hd.job("disco://localhost:5000", "homedisco",\
                        ["homedisco-test"], fun_map, reduce = fun_reduce)
        res = reduce_hd.job("disco://localhost:5000", "homedisco",\
                        res, fun_map, reduce = fun_reduce)
        for k, v in disco.result_iterator(res):
                print "KEY", k, "VALUE", v

