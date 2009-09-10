import os, subprocess, cStringIO, marshal, time, sys, cPickle
import re, traceback, tempfile, struct, random

from disco.util import\
    parse_dir, load_conf, err, data_err, msg, resultfs_enabled, load_oob
from disco.func import re_reader, netstr_reader
from disconode.util import ensure_path, safe_append, write_files
from disconode import external
from disco import comm

try:
        import hashlib as md5
except ImportError:
        # Hashlib is not available in Python2.4
        import md5

oob_chars = re.compile("[^a-zA-Z_\-:0-9]")

status_interval = 0

# Function stubs

def fun_map(e, params):
        pass

def fun_map_reader(fd, sze, job_input):
        pass

def fun_map_writer(fd, key, value, params):
        pass

def fun_partition(key, nr_reduces, params):
        pass

def fun_combiner(key, value, comb_buffer, flush, params):
        pass

def fun_reduce(red_in, red_out, params):
        pass

def fun_reduce_reader(fd, sze, job_input):
        pass

def fun_reduce_writer(fd, key, value, params):
        pass

def fun_init(reader, params):
        pass

def this_name():
        return sys.argv[2]

def this_master():
        return sys.argv[4].split("/")[2]

def this_host():
        return sys.argv[3]

def this_partition():
        return int(sys.argv[5])
        
def this_inputs():
        return sys.argv[6:]

def init():
        global HTTP_PORT, PARAMS_FILE, EXT_MAP, EXT_REDUCE,\
               PART_SUFFIX, MAP_OUTPUT, REDUCE_DL,\
               REDUCE_SORTED, REDUCE_OUTPUT, OOB_FILE,\
               JOB_HOME, DISCO_ROOT, JOB_ROOT, PART_OUTPUT,\
               MAP_INDEX, REDUCE_INDEX, REQ_FILES, CHDIR_PATH

        tmp, HTTP_PORT, DISCO_ROOT = load_conf()
        job_name = this_name()

        JOB_HOME = "%s/%s/%s/" %\
                (this_host(), md5.md5(job_name).hexdigest()[:2], job_name)
        JOB_ROOT = "%s/data/%s" % (DISCO_ROOT, JOB_HOME)

        if resultfs_enabled:
                pp = "%s/temp/%s" % (DISCO_ROOT, JOB_HOME)
        else:
                pp = JOB_ROOT

        CHDIR_PATH = pp
        PARAMS_FILE = pp + "params.dl"
        REQ_FILES = pp + "lib"
        EXT_MAP = pp + "ext.map"
        EXT_REDUCE = pp + "ext.reduce"
        PART_SUFFIX = "-%.9d"
        MAP_OUTPUT = pp + "map-disco-%d" + PART_SUFFIX
        PART_OUTPUT = pp + "part-disco-%.9d"
        REDUCE_DL = pp + "reduce-in-%d.dl"
        REDUCE_SORTED = pp + "reduce-in-%d.sorted"
        REDUCE_OUTPUT = pp + "reduce-disco-%d"
        OOB_FILE = pp + "oob/%s"
        MAP_INDEX = pp + "map-index.txt"
        REDUCE_INDEX = pp + "reduce-index.txt"

def put(key, value):
        if oob_chars.match(key):
                raise "OOB key contains invalid characters (%s)" % key
        if value != None:
                f = file(OOB_FILE % key, "w")
                f.write(value)
                f.close()
        print >> sys.stderr, "**<OOB>%s %s/oob/%s" % (key, JOB_HOME, key)

def get(key, job = None):
        try:
                job = job or this_name()
                return load_oob("http://" + this_master(), job, key)
        except comm.CommException, x:
                data_err("OOB key (%s) not found at %s: HTTP status '%s'" %\
                        (key, url, x.http_code), key)

def open_local(input, fname):
        try:
                f = file(fname)
                sze = os.stat(fname).st_size
                return sze, f
        except:
                data_err("Can't access a local input file (%s): %s"\
                                % (input, fname), input)

def open_remote(input, ext_host, ext_file):
        try:
                return comm.open_remote("http://%s%s" % (ext_host, ext_file))
        except Exception, x:
                data_err("Can't access an external input file (%s%s): %s"\
                         % (ext_host, ext_file, x), x)

def connect_input(input):
        
        if input.startswith("disco://"):
                host, fname = input[8:].split("/", 1)
                local_file = "%s/data/%s" % (DISCO_ROOT, fname)
                ext_host = "%s:%s" % (host, HTTP_PORT)
                ext_file = "/" + fname

        elif input.startswith("http://"):
                ext_host, fname = input[7:].split("/", 1)
                host = ext_host
                ext_file = "/" + fname
                local_file = None

        elif input.startswith("raw://"):
                return len(input) - 6, cStringIO.StringIO(input[6:])
        else:
                host = this_host()
                if input.startswith("dfs://"):
                        t, path = input[6:].split("/", 1)
                        local_file = "%s/input/%s" % (DISCO_ROOT, path)
                elif input.startswith("file://"):
                        local_file = input[7:]
                else:
                        local_file = input

        if local_file and (resultfs_enabled or host == this_host()):
                return open_local(input, local_file)
        else:
                return open_remote(input, ext_host, ext_file)

class MapOutput(object):
        def __init__(self, part, params, combiner = None):
                self.combiner = combiner
                self.params = params
                self.comb_buffer = {}
                self.fname = MAP_OUTPUT % (this_partition(), part)
                ensure_path(self.fname, False)
                self.fd = file(self.fname + ".partial", "w")
                self.part = part
                
        def add(self, key, value):
                if self.combiner:
                        ret = self.combiner(key, value, self.comb_buffer,\
                                   0, self.params)
                        if ret:
                                for key, value in ret:
                                        fun_map_writer(self.fd, key, value,
                                                self.params)
                else:
                        fun_map_writer(self.fd, key, value, self.params)

        def close(self):
                if self.combiner:
                        ret = self.combiner(None, None, self.comb_buffer,\
                                1, self.params)
                        if ret:
                                for key, value in ret:
                                        fun_map_writer(self.fd, key, value,\
                                                self.params)
                self.fd.close()
                os.rename(self.fname + ".partial", self.fname)
        

class ReduceOutput(object):
        def __init__(self, params):
                self.fname = REDUCE_OUTPUT % this_partition()
                self.params = params
                ensure_path(self.fname, False)
                self.fd = file(self.fname + ".partial", "w")

        def add(self, key, value):
                fun_reduce_writer(self.fd, key, value, self.params)

        def close(self):
                self.fd.close()
                os.rename(self.fname + ".partial", self.fname)


def num_cmp(x, y):
        try:
                x = (int(x[0]), x[1])
                y = (int(y[0]), y[1])
        except ValueError:
                pass
        return cmp(x, y)

class ReduceReader(object):
        def __init__(self, input_files, do_sort, mem_sort_limit):
                self.inputs = []
                part = PART_SUFFIX % this_partition()
                for input in input_files:
                        if input.startswith("dir://"):
                                try:
                                        self.inputs += parse_dir(input,
                                                part_id = this_partition())
                                except:
                                        data_err("Couldn't resolve address %s"\
                                                % input, input)
                        else:
                                self.inputs.append(input)

                self.line_count = 0
                if do_sort:
                        total_size = 0
                        for input in self.inputs:
                                sze, fd = connect_input(input)
                                total_size += sze

                        msg("Reduce[%d] input is %.2fMB" %\
                                (this_partition(), total_size / 1024.0**2))

                        if total_size > mem_sort_limit:
                                self.iterator = self.download_and_sort()
                        else: 
                                msg("Sorting in memory")
                                m = list(self.multi_file_iterator(self.inputs, False))
                                m.sort(num_cmp)
                                self.iterator = self.list_iterator(m)
                else:
                        self.iterator = self.multi_file_iterator(self.inputs)
                        
        def iter(self):
                return self.iterator

        def download_and_sort(self):
                dlname = REDUCE_DL % this_partition()
                ensure_path(dlname, False)
                msg("Reduce will be downloaded to %s" % dlname)
                out_fd = file(dlname + ".partial", "w")
                for fname in self.inputs:
                        sze, fd = connect_input(fname)
                        for k, v in fun_reduce_reader(fd, sze, fname):
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
                sortname = REDUCE_SORTED % this_partition()
                ensure_path(sortname, False)
                cmd = ["sort", "-n", "-k", "1,1", "-z",\
                        "-t", " ", "-o", sortname, dlname]

                proc = subprocess.Popen(cmd)
                ret = proc.wait()
                if ret:
                        err("Sorting %s to %s failed (%d)" %\
                                (dlname, sortname, ret))
                
                msg("External sort done: %s" % sortname)
                return self.multi_file_iterator([sortname], reader =\
                        lambda fd, sze, fname:\
                                re_reader("(?s)(.*?) (.*?)\000", fd, sze, fname))

       
        def list_iterator(self, lst):
                i = 0
                for x in lst:
                        yield x
                        i += 1
                        if status_interval and not i % status_interval:
                                msg("%d entries reduced" % i)
                msg("Reduce done: %d entries reduced in total" % i)

        def multi_file_iterator(self, inputs, progress = True,
                                reader = fun_reduce_reader):
                i = 0
                for fname in inputs:
                        sze, fd = connect_input(fname)
                        for x in reader(fd, sze, fname):
                                yield x
                                i += 1
                                if progress and status_interval and\
                                        not i % status_interval:
                                        msg("%d entries reduced" % i)

                if progress:
                        msg("Reduce done: %d entries reduced in total" % i)


def run_map(job_input, partitions, param):
        i = 0
        sze, fd = connect_input(job_input)
        nr_reduces = len(partitions)
        reader = fun_map_reader(fd, sze, job_input)
        fun_init(reader, param)
        
        for entry in reader:
                for key, value in fun_map(entry, param):
                        p = fun_partition(key, nr_reduces, param)
                        partitions[p].add(key, value)
                i += 1
                if status_interval and not i % status_interval:
                        msg("%d entries mapped" % i)

        msg("Done: %d entries mapped in total" % i)

def merge_partitions(partitions):
        for i, p in enumerate(partitions):
                safe_append(file(p.fname), PART_OUTPUT % i)
                os.remove(p.fname)

def import_modules(modules, funcs):
    for m in modules:
        mod = __import__(m, fromlist = [m])
        for fun in funcs:
            fun.func_globals.setdefault(m.split(".")[-1], mod)

def op_map(job):
        job_input = this_inputs()
        msg("Received a new map job!")
        
        if len(job_input) != 1:
                err("Map can only handle one input. Got: %s" % 
                        " ".join(job_input))

        nr_reduces = int(job['nr_reduces'])
        nr_part = max(1, nr_reduces)
        fun_map_reader.func_code = marshal.loads(job['map_reader'])
        fun_map_writer.func_code = marshal.loads(job['map_writer'])
        fun_partition.func_code = marshal.loads(job['partition'])

        if 'map_init' in job:
                fun_init.func_code = marshal.loads(job['map_init'])
        
        if 'required_files' in job:
                write_files(marshal.loads(job['required_files']), REQ_FILES)
                sys.path.insert(0, REQ_FILES)

        req_mod = job['required_modules'].split()
        import_modules(req_mod, [fun_map_reader, fun_map_writer,
            fun_partition, fun_map, fun_combiner, fun_init])

        if 'ext_map' in job:
                if 'ext_params' in job:
                        map_params = job['ext_params']
                else:
                        map_params = "0\n"
                external.prepare(job['ext_map'], map_params, EXT_MAP)
                fun_map.func_code = external.ext_map.func_code
        else:
                map_params = cPickle.loads(job['params'])        
                fun_map.func_code = marshal.loads(job['map'])
        

        if 'combiner' in job:
                fun_combiner.func_code = marshal.loads(job['combiner'])
                partitions = [MapOutput(i, map_params, fun_combiner)\
                        for i in range(nr_part)]
        else:
                partitions = [MapOutput(i, map_params) for i in range(nr_part)]
        
        run_map(job_input[0], partitions, map_params)
        external.close_ext()
        
        for p in partitions:
                p.close()

        if nr_reduces:
                merge_partitions(partitions)
                n = os.path.basename(PART_OUTPUT % 0)
                msg("dir://%s/%s%s:%d" % (this_host(), JOB_HOME, n,
                        len(partitions) - 1), "OUT")
        else:
                res = [os.path.basename(p.fname) for p in partitions]
                index = cStringIO.StringIO("\n".join(res) + "\n")
                safe_append(index, MAP_INDEX)
                msg("dir://%s/%smap-index.txt" %\
                        (this_host(), JOB_HOME), "OUT")

def op_reduce(job):
        job_inputs = this_inputs()

        msg("Received a new reduce job!")
        
        do_sort = int(job['sort'])
        mem_sort_limit = int(job['mem_sort_limit'])
        req_mod = job['required_modules'].split()
        
        if 'reduce_init' in job:
                fun_init.func_code = marshal.loads(job['reduce_init'])

        fun_reduce_reader.func_code = marshal.loads(job['reduce_reader'])
        fun_reduce_writer.func_code = marshal.loads(job['reduce_writer'])
        
        if 'required_files' in job:
                write_files(marshal.loads(job['required_files']), REQ_FILES)
                sys.path.insert(0, REQ_FILES)
        
        import_modules(req_mod, [fun_reduce_reader, fun_reduce_writer,\
            fun_reduce, fun_init])
         
        if 'ext_reduce' in job:
                if "ext_params" in job:
                        red_params = job['ext_params']
                else:
                        red_params = "0\n"
                external.prepare(job['ext_reduce'], red_params, EXT_REDUCE)
                fun_reduce.func_code = external.ext_reduce.func_code
        else:
                fun_reduce.func_code = marshal.loads(job['reduce'])
                red_params = cPickle.loads(job['params'])

        red_in = ReduceReader(job_inputs, do_sort, mem_sort_limit).iter()
        red_out = ReduceOutput(red_params)
        
        msg("Starting reduce")
        fun_init(red_in, red_params)
        fun_reduce(red_in, red_out, red_params)
        msg("Reduce done")
        
        red_out.close()
        external.close_ext()
        
        index = cStringIO.StringIO(os.path.basename(red_out.fname) + "\n")
        safe_append(index, REDUCE_INDEX)
        msg("dir://%s/%sreduce-index.txt" % (this_host(), JOB_HOME), "OUT")

init()
