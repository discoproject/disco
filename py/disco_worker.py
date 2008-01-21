import os, subprocess, cStringIO, marshal, time, sys, httplib, re, traceback

from netstring import *

HTTP_PORT = "8989"
LOCAL_PATH = "/var/disco/"
MAP_OUTPUT = LOCAL_PATH + "%s/map-%d-%d"
REDUCE_DL = LOCAL_PATH + "%s/reduce-in-%d.dl"
REDUCE_SORTED = LOCAL_PATH + "%s/reduce-in-%d.sorted"
REDUCE_OUTPUT = LOCAL_PATH + "%s/reduce-%d"

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
        return int(sys.argv[4])
        
def this_inputs():
        return sys.argv[5:]

def ensure_path(path, check_exists = True):
        if check_exists and os.path.exists(path):
                err("File exists: %s" % path)
        try:
                os.remove(path)
        except OSError, x:
                if x.errno == 2:
                        # no such file
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

def connect_input(input):
        if input.startswith("disco://"):
                host, fname = input[8:].split("/", 1)
                local_file = LOCAL_PATH + fname
                ext_host = host + ":" + HTTP_PORT
                ext_file = "/" + fname
        elif input.startswith("http://"):
                ext_host, fname = input[7:].split("/", 1)
                ext_file = "/" + fname
                local_file = None
        else:
                host = this_host()
                local_file = input

        if host == this_host() and local_file:
                try:
                        sze = os.stat(local_file).st_size
                        return sze, file(local_file)
                except:
                        data_err("Can't access a local input file: %s"\
                                        % input, input)
        else:
                try:
                        http = httplib.HTTPConnection(ext_host)
                        http.request("GET", ext_file, "")
                        fd = http.getresponse()
                        if fd.status != 200:
                                raise "HTTP error %d" % fd.status
                        sze = int(fd.getheader("content-length"))
                        return sze, fd
                except:
                        data_err("Can't access an external input file: %s"\
                                        % input, input)

def encode_kv_pair(fd, key, value):
        key = str(key)
        value = str(value)
        if " " in key:
                err("Spaces are not allowed in keys: <%s>" % key)
        if "\000" in value:
                err("Zero-bytes not allowed in values: <%s>" % value)

        fd.write("%s %s\000" % (key, value))

def re_reader(item_re_str, fd, content_len, fname):
        item_re = re.compile(item_re_str)
        buf = ""
        tot = 0
        while True:
                try:
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

                if not len(r):
                        if tot < content_len:
                                data_err("Truncated input (%s). "\
                                         "Expected %d bytes, got %d" %\
                                         (fname, content_len, tot), fname)
                        if len(buf):
                                err("Corrupted input (%s). Could not "\
                                        "parse the last %d bytes."\
                                                % (fname, len(buf)))
                        break

class MapOutput:
        def __init__(self, part, combiner = None):
                self.combiner = combiner
                self.comb_buffer = {}
                self.fname = MAP_OUTPUT % (job_name, this_partition(), part)
                ensure_path(self.fname, False)
                self.fd = file(self.fname + ".partial", "w")
                
        def flush_comb_buffer(self):
                self.combiner = None
                for key, value in self.comb_buffer.iteritems():
                        self.add(key, value)
                self.combiner = comb


        def add(self, key, value):
                if self.combiner:
                        comb = self.combiner
                        if comb(key, value, self.comb_buffer, 0):
                                self.flush_comb_buffer()
                        return

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
                self.line_count = 0
                self.inputs = []
                for input in input_files:
                        sze, fd = connect_input(input)
                        self.inputs.append((sze, fd, input))
                
                total_size = sum(sze for sze, fd, fname in self.inputs)
                msg("Reduce[%d] input is %.2fMB" %\
                        (this_partition(), total_size / 1024**2))

                if do_sort:
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
                for sze, fd, fname in self.inputs:
                        msg("Reduce downloading %s" % fname)
                        try:
                                buf = " "
                                tot = 0
                                while len(buf):
                                        buf = fd.read(8192)
                                        tot += len(buf)
                                        out_fd.write(buf)
                                fd.close()
                                if tot < sze:
                                        data_err("Truncated input. "\
                                                "Expected %d bytes, got %d" %\
                                                        (content_len, tot),\
                                                                fname)
                        except:
                                data_err("Could not download input file",\
                                        fname)
                out_fd.close()
                os.rename(dlname + ".partial", dlname)
                msg("Reduce input downloaded ok")

                msg("Starting external sort")
                sortname = REDUCE_SORTED % (job_name, this_partition())
                ensure_path(sortname, False)
                cmd = ["sort", "-n", "-z", "-t", " ", "-o", sortname, dlname]

                proc = subprocess.Popen(cmd)
                ret = proc.wait()
                if ret:
                        err("Sorting %s to %s failed (%d)" %\
                                (dlname, sortname, ret))
                
                msg("External sort done: %s" % sortname)
                tot = sum(sze for sze, fd, fname in self.inputs)
                return self.multi_file_iterator(
                        [(sze, file(sortname), sortname)])
       
        def list_iterator(self, lst):
                i = 0
                for x in lst:
                        yield x
                        i += 1
                        if not i % 10000:
                                msg("%d entries reduced" % i)
                msg("Reduce done: %d entries reduced in total" % i)

        def multi_file_iterator(self, inputs, progress = True):
                i = 0
                for sze, fd, fname in inputs:
                        for x in re_reader("(.*?) (.*?)\000", fd, sze, fname):
                                yield x
                                i += 1
                                if progress and not i % 10000:
                                        msg("%d entries reduced" % i)
                if progress:
                        msg("Reduce done: %d entries reduced in total" % i)


# Function stubs

def fun_map(e):
        pass

def fun_map_reader(fd, sze, job_input):
        pass

def fun_partition(key, nr_reduces):
        pass

def fun_combiner(key, value, comb_buffer, flush):
        pass

def fun_reduce(red_in, red_out, job):
        pass

# Erlay handlers

def run_map(job_input, partitions):
        i = 0
        sze, fd = connect_input(job_input)
        nr_reduces = len(partitions)
        for entry in fun_map_reader(fd, sze, job_input):
                for key, value in fun_map(entry):
                        p = fun_partition(key, nr_reduces)
                        partitions[p].add(key, value)
                i += 1
                if not i % 10000:
                        msg("%d entries mapped" % i)

        msg("Done: %d entries mapped in total" % i)

def op_map(job):
        global job_name
        
        job_name = job['name']
        job_input = this_inputs()
        msg("Received a new map job!")
        
        if len(job_input) != 1:
                err("Map can only handle one input. Got: %s" % 
                        " ".join(job_input))

        nr_reduces = int(job['nr_reduces'])
        fun_map.func_code = marshal.loads(job['map'])
        fun_map_reader.func_code = marshal.loads(job['map_reader'])
        fun_partition.func_code = marshal.loads(job['partition'])
       
        if 'combiner' in job:
                fun_combiner.func_code = marshal.loads(job['combiner'])
                partitions = [MapOutput(i, fun_combiner)\
                        for i in range(nr_reduces)]
        else:
                partitions = [MapOutput(i) for i in range(nr_reduces)]

        run_map(job_input[0], partitions)

        me = this_host()
        for p, part in enumerate(partitions):
                part.close()
                msg("%d %s" % (p, part.disco_address()), "OUT")


def op_reduce(job):
        global job_name

        job_name = job['name']
        job_inputs = this_inputs()

        msg("Received a new reduce job!")
        
        do_sort = int(job['sort'])
        mem_sort_limit = int(job['mem_sort_limit'])
        
        fun_reduce.func_code = marshal.loads(job['reduce'])

        red_in = ReduceReader(job_inputs, do_sort, mem_sort_limit)

        red_out = ReduceOutput()
        msg("Starting reduce")
        fun_reduce(red_in.iter(), red_out, job)
        msg("Reduce done")
        red_out.close()

        msg("%d %s" % (this_partition(), red_out.disco_address()), "OUT")

job_name = ""
if len(sys.argv) < 6:
        err("Invalid command line. "\
            "Usage: disco_worker.py [op_map|op_reduce] name hostname partid inputs..")

if "op_" + sys.argv[1] not in globals():
        err("Invalid operation: %s" % sys.argv[1])

try:
        m = decode_netstring_fd(sys.stdin)
except:
        msg("Decoding the job description failed", "ERR")
        raise

globals()["op_" + sys.argv[1]](m)
msg("Worker done", "END")








                

        











        
