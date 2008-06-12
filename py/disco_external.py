import os, os.path, time, struct, marshal
from subprocess import *
from netstring import decode_netstring_str
import disco_external, disco_worker

proc = None

def pack_kv(k, v):
        return struct.pack("I", len(k)) + k +\
               struct.pack("I", len(v)) + v

def unpack_kv():
        le = struct.unpack("I", out_fd.read(4))[0]
        k = out_fd.read(le)
        le = struct.unpack("I", out_fd.read(4))[0]
        v = out_fd.read(le)
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
        return [disco_external.unpack_kv() for i in range(num)]

def ext_reduce(red_in, red_out, params):
        for e in red_in:
                for k, v in disco_external.ext_map(e, None):
                        red_out.add(k, v)

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
        disco_worker.ensure_path(path + "/", False)
        for fname, data in ext_data.iteritems():
                ensure_file(path + "/" + fname, data)

def ensure_file(fname, data, timeout = 60, mode = 500):
        while timeout > 0:
                if os.path.exists(fname):
                        return
                try:
                        fd = os.open(fname + ".partial",
                                os.O_CREAT | os.O_EXCL | os.O_WRONLY, 500)
                        if type(data) == str:
                                os.write(fd, data)
                        else:
                                try:
                                        input, stream = data
                                        os.write(fd, stream.read())
                                except:
                                        raise Exception("Couldn't load %s"\
                                                % input)  
                        os.close(fd)
                        os.rename(fname + ".partial", fname)
                        return
                except OSError, x:
                        # File exists
                        if x.errno == 17:
                                time.sleep(1)
                                timeout -= 1
                        else:
                                disco_worker.msg("Writing external "\
                                        "file %s failed" % fname)
                                raise
        disco_worker.msg("Timeout in writing external file %s" % fname)
        raise Exception("Timeout in writing external file %s" % fname)
        
