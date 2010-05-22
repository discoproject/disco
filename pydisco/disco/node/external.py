import os, os.path, time, struct, marshal
from subprocess import Popen, PIPE
from disco.netstring import decode_netstring_str, encode_netstring_fd
from disco.fileutils import write_files
from disco.util import msg
from disco.error import DiscoError

MAX_ITEM_SIZE = 1024**3
MAX_NUM_OUTPUT = 1000000

proc, in_fd, out_fd = None, None, None

def pack_kv(k, v):
    return struct.pack("I", len(k)) + k +\
           struct.pack("I", len(v)) + v

def unpack_kv():
    le = struct.unpack("I", out_fd.read(4))[0]
    if le > MAX_ITEM_SIZE:
        raise DiscoError("External key size exceeded: %d bytes" % le)
    k = out_fd.read(le)
    le = struct.unpack("I", out_fd.read(4))[0]
    if le > MAX_ITEM_SIZE:
        raise DiscoError("External key size exceeded: %d bytes" % le)
    v = out_fd.read(le)
    return k, v

def ext_map(e, params):
    if isinstance(e, basestring):
        k = ""
        v = e
    else:
        k, v = e
    in_fd.write(pack_kv(k, v))
    in_fd.flush()
    num = struct.unpack("I", out_fd.read(4))[0]
    r = [unpack_kv() for i in range(num)]
    return r

def ext_reduce(red_in, red_out, params):
    import select
    p = select.poll()
    eof = select.POLLHUP | select.POLLNVAL | select.POLLERR
    p.register(out_fd, select.POLLIN | eof)
    p.register(in_fd, select.POLLOUT | eof)
    MAX_NUM_OUTPUT = MAX_NUM_OUTPUT

    tt = 0
    while True:
        for fd, event in p.poll():
            if event & (select.POLLNVAL | select.POLLERR):
                raise DiscoError("Pipe to the external process failed")
            elif event & select.POLLIN:
                num = struct.unpack("I",
                    out_fd.read(4))[0]
                if num > MAX_NUM_OUTPUT:
                    raise DiscoError("External output limit "\
                        "exceeded: %d > %d" %\
                        (num, MAX_NUM_OUTPUT))
                for i in range(num):
                    red_out.add(*unpack_kv())
                    tt += 1
            elif event & select.POLLOUT:
                try:
                    msg = pack_kv(*red_in.next())
                    in_fd.write(msg)
                    in_fd.flush()
                except StopIteration:
                    p.unregister(in_fd)
                    in_fd.close()
            else:
                return

def prepare(ext_task, params, path):
    params = encode_netstring_fd(params)\
        if params and isinstance(params, dict) else "0\n"
    write_files(ext_task, path)
    open_ext(path + "/op", params)

def open_ext(fname, params):
    # XXX! Run external programs in /data/ dir, not /temp/
    global proc, in_fd, out_fd
    proc = Popen([fname], stdin = PIPE, stdout = PIPE)
    in_fd = proc.stdin
    out_fd = proc.stdout
    in_fd.write(params)

def close_ext():
    if proc:
        os.kill(proc.pid, 9)

