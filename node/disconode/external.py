import os, os.path, time, struct, marshal
from subprocess import *
from disco.netstring import decode_netstring_str
from disconode.util import *

try:
        import disco.comm_curl as comm
except:
        import disco.comm_httplib as comm

MAX_ITEM_SIZE = 1024**3
MAX_NUM_OUTPUT = 1000000

proc = None

def pack_kv(k, v):
        return struct.pack("I", len(k)) + k +\
               struct.pack("I", len(v)) + v

def unpack_kv():
        le = struct.unpack("I", out_fd.read(4))[0]
        if le > MAX_ITEM_SIZE:
                raise "External key size exceeded: %d bytes" % le
        k = out_fd.read(le)
        le = struct.unpack("I", out_fd.read(4))[0]
        if le > MAX_ITEM_SIZE:
                raise "External key size exceeded: %d bytes" % le
        v = out_fd.read(le)
        return k, v

def ext_map(e, params):
        if type(e) == str:
                k = ""
                v = e
        else:
                k, v = e
        external.in_fd.write(external.pack_kv(k, v))
        external.in_fd.flush()
        num = struct.unpack("I", external.out_fd.read(4))[0]
        r = [external.unpack_kv() for i in range(num)]
        return r

def ext_reduce(red_in, red_out, params):
        import select
        p = select.poll()
        eof = select.POLLHUP | select.POLLNVAL | select.POLLERR
        p.register(external.out_fd, select.POLLIN | eof)
        p.register(external.in_fd, select.POLLOUT | eof)
        MAX_NUM_OUTPUT = external.MAX_NUM_OUTPUT

        tt = 0
        while True:
                for fd, event in p.poll():
                        if event & (select.POLLNVAL | select.POLLERR):
                                raise "Pipe to the external process failed"
                        elif event & select.POLLIN:
                                num = struct.unpack("I",
                                        external.out_fd.read(4))[0]
                                if num > MAX_NUM_OUTPUT:
                                        raise "External output limit "\
                                                "exceeded: %d > %d" %\
                                                (num, MAX_NUM_OUTPUT)
                                for i in range(num):
                                        red_out.add(*external.unpack_kv())
                                        tt += 1
                        elif event & select.POLLOUT:
                                try:
                                        msg = external.pack_kv(*red_in.next())
                                        external.in_fd.write(msg)
                                        external.in_fd.flush()
                                except StopIteration:
                                        p.unregister(external.in_fd)
                                        external.in_fd.close()
                        else:
                                return

def prepare(ext_job, params, path):
        write_files(marshal.loads(ext_job), path)
        open_ext(path + "/op", params)

def open_ext(fname, params):
        global proc, in_fd, out_fd
        proc = Popen([fname], stdin = PIPE, stdout = PIPE)
        in_fd = proc.stdin
        out_fd = proc.stdout
        in_fd.write(params)

def close_ext():
        if proc:
                os.kill(proc.pid, 9)

def write_files(ext_data, path):
        ensure_path(path + "/", False)
        for fname, data in ext_data.iteritems():
                ensure_file(path + "/" + fname, data = data)

def ensure_file(fname, data = None, url = None, timeout = 60, mode = 500):
        while timeout > 0:
                if os.path.exists(fname):
                        return
                try:
                        fd = os.open(fname + ".partial",
                                os.O_CREAT | os.O_EXCL | os.O_WRONLY, 500)
                        if data:
                                os.write(fd, data)
                        else:
                                os.write(fd, comm.download(url))
                        os.close(fd)
                        os.rename(fname + ".partial", fname)
                        return
                except OSError, x:
                        # File exists
                        if x.errno == 17:
                                time.sleep(1)
                                timeout -= 1
                        else:
                                msg("Writing external file %s failed" % fname)
                                raise
        msg("Timeout in writing external file %s" % fname)
        raise Exception("Timeout in writing external file %s" % fname)
        
