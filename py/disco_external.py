import os, os.path, time, struct
from subprocess import *
from disco_worker import msg
from netstring import decode_netstring_str
import disco_external

def pack_kv(k, v):
        return struct.pack("I", len(k)) + k +\
               struct.pack("I", len(v)) + v

def unpack_kv(fd):
        le = struct.unpack("I", fd.read(4))[0]
        k = fd.read(le)
        le = struct.unpack("I", fd.read(4))[0]
        v = fd.read(le)
        return k, v

def ext_map(e, params):
        if type(e) == str:
                k = ""
                v = e
        else:
                k, v = e
        disco_external.in_fd.write(
                disco_external.pack_kv(k, v))
        num = struct.unpack("I", disco_external.out_fd.read(4))[0]
        return [unpack_kv(out_fd) for i in range(num)]

def ext_reduce(red_in, red_out, params):
        for e in red_in:
                for k, v in disco_external.ext_map(e, None):
                        red_out.add(k, v)

def prepare(ext_job, params, path):
        external.write_files(decode_netstring_str(ext_job), path)
        open_ext(path + "/op", params)

def open_ext(fname, params):
        global in_fd, out_fd
        proc = Popen([fname], stdin = PIPE, stdout = PIPE)
        in_fd = proc.stdin
        out_fd = proc.stdout
        in_fd.write(params)

def write_files(ext_data, path):
        ensure_path(path, False)
        for fname, data in ext_data.iteritems():
                disco_util.ensure_file(path + "/" + fname, data)

def ensure_file(fname, data, timeout = 60):
        while timeout > 0:
                if os.path.exists(fname):
                        return
                try:
                        fd = os.open(fname + ".partial",
                                os.O_CREAT | os.O_EXCL | os.O_WRONLY, 500)
                        os.write(fd, data)
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
        
