import os, subprocess, cStringIO, marshal, time, sys
import httplib, re, traceback, tempfile, struct, urllib

import disco_external
from netstring import *

HTTP_PORT = "8989"
LOCAL_PATH = "/var/disco/"
PARAMS_FILE = LOCAL_PATH + "%s/params"
EXT_MAP = LOCAL_PATH + "%s/ext-map"
EXT_REDUCE = LOCAL_PATH + "%s/ext-reduce"
MAP_OUTPUT = LOCAL_PATH + "%s/map-disco-%d-%.9d"
CHUNK_OUTPUT = LOCAL_PATH + "%s/map-chunk-%d"
REDUCE_DL = LOCAL_PATH + "%s/reduce-in-%d.dl"
REDUCE_SORTED = LOCAL_PATH + "%s/reduce-in-%d.sorted"
REDUCE_OUTPUT = LOCAL_PATH + "%s/reduce-disco-%d"

job_name = ""
http_pool = {}

def msg(m, c = 'MSG', job_input = ""):
        t = time.strftime("%y/%m/%d %H:%M:%S")
        print >> sys.stderr, "**<%s>[%s %s (%s)] %s" %\
                (c, t, job_name, job_input, m)

def err(m):
        msg(m, 'MSG')
        raise m 

def data_err(m, job_input):
        if sys.exc_info() == (None, None, None):
                raise m
        else:
                print traceback.print_exc()
                msg(m, 'DAT', job_input)
                raise
            
def this_host():
        return sys.argv[3]

def this_partition():
        return int(sys.argv[5])
        
def this_inputs():
        return sys.argv[6:]

def ensure_path(path, check_exists = True):
        if check_exists and os.path.exists(path):
                err("File exists: %s" % path)
        try:
                os.remove(path)
        except OSError, x:
                if x.errno == 2:
                        # no such file
                        pass
                elif x.errno == 21:
                        # directory
                        pass
                else:
                        raise
        try:
                dir, fname = os.path.split(path)
                os.makedirs(dir)
        except OSError, x:
                if x.errno == 17:
                        # directory already exists
                        pass
                else:
                        raise

def parse_dir(dir_url):
        x, x, host, mode, name = dir_url.split('/')
        html = urllib.urlopen("http://%s:%s/%s" %\
                (host, HTTP_PORT, name)).read()
        inputs = re.findall(">(%s-(.+?)-.*?)</a>" % mode, html)
        return ["%s://%s/%s/%s" % (prefix, host, name, x)\
                        for x, prefix in inputs if "partial" not in x]


def open_local(input, fname, is_chunk):
        try:
                f = file(fname)
                if is_chunk:
                        f.seek(this_partition() * 8)
                        start, end = struct.unpack("QQ", f.read(16))
                        sze = end - start
                        f.seek(start)
                else:
                        sze = os.stat(fname).st_size
                return sze, f
        except:
                data_err("Can't access a local input file: %s"\
                                % input, input)

def open_remote(input, ext_host, ext_file, is_chunk):
        try:
                # We can't open a new HTTP connection for each intermediate
                # result -- this would result to M * R TCP connections where
                # M is the number of maps and R the number of reduces. Instead,
                # we pool connections and reuse them whenever possible. HTTP 
                # 1.1 defaults to keep-alive anyway.
                if ext_host in http_pool:
                        http = http_pool[ext_host]
                        if http._HTTPConnection__response:
                                http._HTTPConnection__response.read()
                else:
                        http = httplib.HTTPConnection(ext_host)
                        http_pool[ext_host] = http

                if is_chunk:
                        pos = this_partition() * 8
                        rge = "bytes=%d-%d" % (pos, pos + 15)
                        #msg("Reading offsets at %s" % rge)
                        http.request("GET", ext_file, None, {"Range": rge})
                        fd = http.getresponse()

                        if fd.status != 206:
                                raise "HTTP error %d" % fd.status
                        start, end = struct.unpack("QQ", fd.read())
                        if start == end:
                                return 0, cStringIO.StringIO()
                        else:
                                rge = "bytes=%d-%d" % (start, end - 1)
                        #msg("Reading data at %s" % rge)
                        http.request("GET", ext_file, None, {"Range": rge})
                        fd = http.getresponse()
                        if fd.status != 206:
                                raise "HTTP error %d" % fd.status
                else:
                        http.request("GET", ext_file, "")
                        fd = http.getresponse()
                        if fd.status != 200:
                                raise "HTTP error %d" % fd.status
                sze = fd.getheader("content-length")
                if sze:
                        sze = int(sze)
                return sze, fd

        except httplib.BadStatusLine:
                # BadStatusLine is caused by a closed connection. Re-open a new
                # connection by deleting this connection from the pool and
                # calling this function again. Note that this might result in
                # endless recursion if something went seriously wrong.
                http.close()
                del http_pool[ext_host]
                return open_remote(input, ext_host, ext_file, is_chunk)
        except:
                data_err("Can't access an external input file (%s/%s): %s"\
                                % (ext_host, ext_file, input), input)

def connect_input(input):
        is_chunk = input.startswith("chunk://")
        if input.startswith("disco://") or is_chunk:
                host, fname = input[8:].split("/", 1)
                local_file = LOCAL_PATH + fname
                ext_host = "%s:%s" % (host, HTTP_PORT)
                ext_file = "/" + fname
        elif input.startswith("http://"):
		ext_host, fname = input[7:].split("/", 1)
		host = ext_host
		ext_file = "/" + fname
		local_file = None
        else:
                host = this_host()
                local_file = input

        if host == this_host() and local_file:
                return open_local(input, local_file, is_chunk)
        else:
                return open_remote(input, ext_host, ext_file, is_chunk)

def encode_kv_pair(fd, key, value):
        skey = str(key)
        sval = str(value)
        fd.write("%d %s %d %s\n" % (len(skey), skey, len(sval), sval))

def netstr_reader(fd, content_len, fname):
        if content_len == None:
                err("Content-length must be defined for netstr_reader")
        def read_netstr(idx, data, tot):
                ldata = len(data)
                i = 0
                lenstr = ""
                if ldata - idx < 11:
                        data = data[idx:] + fd.read(8192)
                        ldata = len(data)
                        idx = 0

                i = data.find(" ", idx, idx + 11)
                if i == -1:
                        err("Corrupted input (%s). Could not "\
                               "parse a value length at %d bytes."\
                                        % (fname, tot))
                else:
                        lenstr = data[idx:i + 1]
                        idx = i + 1

                if ldata < i + 1:
                        data_err("Truncated input (%s). "\
                                "Expected %d bytes, got %d" %\
                                (fname, content_len, tot), fname)
                
                try:
                        llen = int(lenstr)
                except ValueError:
                        err("Corrupted input (%s). Could not "\
                                "parse a value length at %d bytes."\
                                        % (fname, tot))

                tot += len(lenstr)

                if ldata - idx < llen + 1:
                        data = data[idx:] + fd.read(llen + 8193)
                        ldata = len(data)
                        idx = 0

                msg = data[idx:idx + llen]
                
                if idx + llen + 1 > ldata:
                        data_err("Truncated input (%s). "\
                                "Expected a value of %d bytes "\
                                "(offset %u bytes)" %\
                                (fname, llen + 1, tot), fname)

                tot += llen + 1
                idx += llen + 1
                return idx, data, tot, msg
        
        data = fd.read(8192)
        tot = idx = 0
        while tot < content_len:
                idx, data, tot, key = read_netstr(idx, data, tot)
                if not key: break
                idx, data, tot, val = read_netstr(idx, data, tot)
                if not val: break
                yield key, val

def re_reader(item_re_str, fd, content_len, fname, output_tail = False):
        item_re = re.compile(item_re_str)
        buf = ""
        tot = 0
        while True:
                try:
                        if content_len:
                                r = fd.read(min(8192, content_len - tot))
                        else:
                                r = fd.read(8192)
                        tot += len(r)
                        buf += r
                except:
                        data_err("Receiving data failed", fname)

                m = item_re.match(buf)
                while m:
                        yield m.groups()
                        buf = buf[m.end():]
                        m = item_re.match(buf)

                if not len(r) or tot >= content_len:
                        if content_len != None and tot < content_len:
                                data_err("Truncated input (%s). "\
                                         "Expected %d bytes, got %d" %\
                                         (fname, content_len, tot), fname)
                        if len(buf):
                                if output_tail:
                                        yield [buf]
                                else:
                                        msg("Couldn't match the last %d "\
                                            "bytes in %s. Some bytes may be "\
                                            "missing from input." %\
                                                (len(buf), fname))
                        break

class MapOutput:
        def __init__(self, part, combiner = None):
                self.combiner = combiner
                self.comb_buffer = {}
                self.fname = MAP_OUTPUT % (job_name, this_partition(), part)
                ensure_path(self.fname, False)
                self.fd = file(self.fname + ".partial", "w")
                self.part = part
                
        def flush_comb_buffer(self):
                comb = self.combiner
                self.combiner = None
                for key, value in self.comb_buffer.iteritems():
                        self.add(key, value)
                self.combiner = comb
                self.comb_buffer = {}

        def add(self, key, value):
                if self.combiner:
                        comb = self.combiner
                        ret = comb(key, value, self.comb_buffer, 0)
                        if type(ret) == tuple:
                                encode_kv_pair(self.fd, ret[0], ret[1])
                        elif ret == True:
                                self.flush_comb_buffer()
                else:
                        encode_kv_pair(self.fd, key, value)

        def close(self):
                if self.combiner:
                        self.combiner(None, None, self.comb_buffer, 1)
                        self.flush_comb_buffer()
                self.fd.close()
                os.rename(self.fname + ".partial", self.fname)
        
        def disco_address(self):
                return "disco://%s/%s" %\
                        (this_host(), self.fname[len(LOCAL_PATH):])


class ReduceOutput:
        def __init__(self):
                self.fname = REDUCE_OUTPUT % (job_name, this_partition())
                ensure_path(self.fname, False)
                self.fd = file(self.fname + ".partial", "w")

        def add(self, key, value):
                encode_kv_pair(self.fd, key, value)

        def close(self):
                self.fd.close()
                os.rename(self.fname + ".partial", self.fname)
        
        def disco_address(self):
                return "disco://%s/%s" %\
                        (this_host(), self.fname[len(LOCAL_PATH):])


class ReduceReader:
        def __init__(self, input_files, do_sort, mem_sort_limit):
                self.inputs = []
                for input in input_files:
                        if input.startswith("dir://"):
                                self.inputs += parse_dir(input)
                        else:
                                self.inputs.append(input)

                self.line_count = 0
                if do_sort:
                        total_size = 0
                        for input in self.inputs:
                                sze, fd = connect_input(input)
                                total_size += sze

                        msg("Reduce[%d] input is %.2fMB" %\
                                (this_partition(), total_size / 1024**2))

                        if total_size > mem_sort_limit:
                                self.iterator = self.download_and_sort()
                        else: 
                                msg("Sorting in memory")
                                m = list(self.multi_file_iterator(self.inputs, False))
                                m.sort()
                                self.iterator = self.list_iterator(m)
                else:
                        self.iterator = self.multi_file_iterator(self.inputs)
                        
        def iter(self):
                return self.iterator

        def download_and_sort(self):
                dlname = REDUCE_DL % (job_name, this_partition())
                ensure_path(dlname, False)
                msg("Reduce will be downloaded to %s" % dlname)
                out_fd = file(dlname + ".partial", "w")
                for fname in self.inputs:
                        sze, fd = connect_input(fname)
                        for k, v in netstr_reader(fd, sze, fname):
                                if " " in k:
                                        err("Spaces are not allowed in keys "\
                                            "with external sort.")
                                if "\0" in v:
                                        err("Zero bytes are not allowed in "\
                                            "values with external sort. "\
                                            "Consider using base64 encoding.")
                                out_fd.write("%s %s\0" % (k, v))
                out_fd.close()
                os.rename(dlname + ".partial", dlname)
                msg("Reduce input downloaded ok")

                msg("Starting external sort")
                sortname = REDUCE_SORTED % (job_name, this_partition())
                ensure_path(sortname, False)
                cmd = ["sort", "-s", "-k", "1,1", "-z",\
                        "-t", " ", "-o", sortname, dlname]

                proc = subprocess.Popen(cmd)
                ret = proc.wait()
                if ret:
                        err("Sorting %s to %s failed (%d)" %\
                                (dlname, sortname, ret))
                
                msg("External sort done: %s" % sortname)
                return self.multi_file_iterator([sortname], reader =\
                        lambda fd, sze, fname:\
                                re_reader("(.*?) (.*?)\000", fd, sze, fname))

       
        def list_iterator(self, lst):
                i = 0
                for x in lst:
                        yield x
                        i += 1
                        if not i % 100000:
                                msg("%d entries reduced" % i)
                msg("Reduce done: %d entries reduced in total" % i)

        def multi_file_iterator(self, inputs, progress = True,
                                reader = netstr_reader):
                i = 0
                for fname in inputs:
                        sze, fd = connect_input(fname)
                        for x in reader(fd, sze, fname):
                                yield x
                                i += 1
                                if progress and not i % 100000:
                                        msg("%d entries reduced" % i)

                if progress:
                        msg("Reduce done: %d entries reduced in total" % i)

# Function stubs

def fun_map(e, params):
        pass

def fun_map_reader(fd, sze, job_input):
        pass

def fun_partition(key, nr_reduces):
        pass

def fun_combiner(key, value, comb_buffer, flush):
        pass

def fun_reduce(red_in, red_out, params):
        pass

def run_map(job_input, partitions, param):
        i = 0
        sze, fd = connect_input(job_input)
        nr_reduces = len(partitions)
        for entry in fun_map_reader(fd, sze, job_input):
                for key, value in fun_map(entry, param):
                        p = fun_partition(key, nr_reduces)
                        partitions[p].add(key, value)
                i += 1
                if not i % 100000:
                        msg("%d entries mapped" % i)

        msg("Done: %d entries mapped in total" % i)

def merge_chunks(partitions):
        mapout = CHUNK_OUTPUT % (job_name, this_partition())
     
        f = file(mapout + ".partial", "w")
        offset = (len(partitions) + 1) * 8
        for p in partitions:
                f.write(struct.pack("Q", offset))
                offset += os.stat(p.fname).st_size
        f.write(struct.pack("Q", offset))
        f.close()

        if subprocess.call("cat %s >> %s.partial" % 
                        (" ".join([p.fname for p in partitions]),
                                mapout), shell = True):
                data_err("Couldn't create a chunk", mapout)
        os.rename(mapout + ".partial", mapout)
        for p in partitions:
                os.remove(p.fname)

def op_map(job):
        global job_name
        
        job_name = job['name']
        job_input = this_inputs()
        msg("Received a new map job!")
        
        if len(job_input) != 1:
                err("Map can only handle one input. Got: %s" % 
                        " ".join(job_input))

        nr_reduces = int(job['nr_reduces'])
        fun_map_reader.func_code = marshal.loads(job['map_reader'])
        fun_partition.func_code = marshal.loads(job['partition'])
        
        if 'ext_map' in job:
                if 'ext_params' in job:
                        map_params = job['ext_params']
                else:
                        map_params = "0\n"
                disco_external.prepare(job['ext_map'],
                        map_params, EXT_MAP % job_name)
                fun_map.func_code = disco_external.ext_map.func_code
        else:
                map_params = marshal.loads(job['params'])        
                fun_map.func_code = marshal.loads(job['map'])

        if 'combiner' in job:
                fun_combiner.func_code = marshal.loads(job['combiner'])
                partitions = [MapOutput(i, fun_combiner)\
                        for i in range(nr_reduces)]
        else:
                partitions = [MapOutput(i) for i in range(nr_reduces)]
        
        run_map(job_input[0], partitions, map_params)
        for p in partitions:
                p.close()
        if 'chunked' in job:
                merge_chunks(partitions)
                out = "chunk://%s/%s/map-%d" %\
                        (this_host(), job_name, this_partition())
        else:
                out = partitions[0].disco_address()
        
        disco_external.close_ext()
        msg("%d %s" % (this_partition(), out), "OUT")

def op_reduce(job):
        global job_name

        job_name = job['name']
        job_inputs = this_inputs()

        msg("Received a new reduce job!")
        
        do_sort = int(job['sort'])
        mem_sort_limit = int(job['mem_sort_limit'])
        
        if 'ext_reduce' in job:
                if "ext_params" in job:
                        red_params = job['ext_params']
                else:
                        red_params = "0\n"
                disco_external.prepare(job['ext_reduce'],
                        red_params, EXT_REDUCE % job_name)
                fun_reduce.func_code = disco_external.ext_reduce.func_code
        else:
                fun_reduce.func_code = marshal.loads(job['reduce'])
                red_params = marshal.loads(job['params'])

        red_in = ReduceReader(job_inputs, do_sort, mem_sort_limit)
        red_out = ReduceOutput()
        msg("Starting reduce")
        fun_reduce(red_in.iter(), red_out, red_params)
        msg("Reduce done")
        red_out.close()
        disco_external.close_ext()

        msg("%d %s" % (this_partition(), red_out.disco_address()), "OUT")


if __name__ == "__main__":
        if len(sys.argv) < 7:
                err("Invalid command line. "\
                    "Usage: disco_worker.py [op_map|op_reduce] "\
                    "name hostname master_url partid inputs..")

        if "op_" + sys.argv[1] not in globals():
                err("Invalid operation: %s" % sys.argv[1])
      
        # Announce my PID to the master
        print >> sys.stderr, "**<PID>%s" % os.getpid()
        
        name = sys.argv[2]
        master_url = sys.argv[4]
        try:
                ensure_path("%s/%s/" % (LOCAL_PATH, name), False)
                params_file = PARAMS_FILE % name
                url_hdle = urllib.urlopen("%s/%s/params" % (master_url, name))
                disco_external.ensure_file(params_file,\
                        (master_url, url_hdle), mode = 444)
                url_hdle.close()
                m = decode_netstring_fd(file(params_file))
        except:
                data_err("Decoding the job description failed", master_url)

        globals()["op_" + sys.argv[1]](m)
        msg("Worker done", "END")








                

        











        
