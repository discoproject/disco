import os, subprocess, cStringIO, marshal, time, sys, cPickle
import re, traceback, tempfile, struct, random
from disco.util import parse_dir, err, data_err, msg, load_oob
from disco.func import re_reader, netstr_reader
from disco.netstring import decode_netstring_str
from disco.fileutils import safe_update, write_files, ensure_path, AtomicFile
from disconode import external
import disco.comm, disco.settings, disco.func

Task = None
oob_chars = re.compile("[^a-zA-Z_\-:0-9]")

status_interval = 0
input_stream_stack = []
output_stream_stack = []

def init(mode, host, master, job_name, id, inputs):
        global Task
        Task = disco.settings.TaskEnvironment(\
                mode, host, master, job_name, id, inputs)

# Function stubs

def fun_input_stream(stream, size, url, params):
        pass

def fun_output_stream(stream, partition, url, params):
        pass

def fun_map(e, params):
        pass

def fun_reader(stream, sze, job_input):
        pass

def fun_writer(stream, key, value, params):
        pass

def fun_partition(key, nr_reduces, params):
        pass

def fun_combiner(key, value, comb_buffer, flush, params):
        pass

def fun_reduce(red_in, red_out, params):
        pass

def fun_init(reader, params):
        pass

def this_name():
        return Task.name

def this_master():
        return Task.master

def this_host():
        return Task.host

def this_partition():
        return Task.id

def this_inputs():
        return Task.inputs

def put(key, value):
        if oob_chars.match(key):
                err("OOB key contains invalid characters (%s)" % key)
        if value != None:
                f = file(Task.path("OOB_FILE", key)[0], "w")
                f.write(value)
                f.close()
        print >> sys.stderr, "**<OOB>%s %s/oob/%s" % (key, Task.home, key)

def get(key, job = None):
        try:
                job = job or Task.name
                return load_oob("http://" + Task.master, job, key)
        except disco.comm.CommException, x:
                data_err("OOB key (%s) not found at %s: HTTP status '%s'" %\
                        (key, url, x.http_code), key)

def connect_input(url, params):
        fd = sze = None
        for name, fun_code in input_stream_stack:
                try:
                        fun_input_stream.func_code = fun_code
                        fd, sze, url = fun_input_stream(fd, sze, url, params)
                except Exception, x:
                        suffix = ""
                        if url:
                                suffix = " when opening %s" % url
                        data_err("Input stream %s failed%s" % (name, suffix), url)
        return fd, sze, url

def connect_output(params, part = 0):
        fd = url = None
        fd_list = []
        for name, fun_code in output_stream_stack:
                try:
                        fun_output_stream.func_code = fun_code
                        fd, url = fun_output_stream(fd, part, url, params)
                        fd_list.append(fd)
                except Exception, x:
                        suffix = ""
                        if url:
                                suffix = " when opening %s " % url
                        data_err("Output stream %s failed%s" % (name, suffix), url)
        return fd, url, fd_list

def close_output(fd_list):
        for fd in reversed(fd_list):
                if hasattr(fd, "close"):
                        fd.close()

class MapOutput(object):
        def __init__(self, part, params, combiner = None):
                self.combiner = combiner
                self.params = params
                self.comb_buffer = {}
                self.fd, self._url, self.fd_list = connect_output(params, part)
                self.part = part
        
        def url(self):
                return self._url
                
        def add(self, key, value):
                if self.combiner:
                        ret = self.combiner(key, value, self.comb_buffer,\
                                   0, self.params)
                        if ret:
                                for key, value in ret:
                                        fun_writer(self.fd, key, value, self.params)
                else:
                        fun_writer(self.fd, key, value, self.params)

        def close(self):
                if self.combiner:
                        ret = self.combiner(None, None, self.comb_buffer,\
                                1, self.params)
                        if ret:
                                for key, value in ret:
                                        fun_writer(self.fd, key, value,\
                                                self.params)
                close_output(self.fd_list)

class ReduceOutput(object):
        def __init__(self, params):
                self.params = params
                self.fd, self._url, self.fd_list = connect_output(params)
        
        def url(self):
                return self._url

        def add(self, key, value):
                fun_writer(self.fd, key, value, self.params)
        
        def close(self):
                close_output(self.fd_list)


def num_cmp(x, y):
        try:
                x = (int(x[0]), x[1])
                y = (int(y[0]), y[1])
        except ValueError:
                pass
        return cmp(x, y)

class ReduceReader(object):
        def __init__(self, input_files, do_sort, mem_sort_limit, params):
                self.inputs = []
                for input in input_files:
                        if input.startswith("dir://"):
                                try:
                                        self.inputs += parse_dir(input, partid = Task.id)
                                except:
                                        data_err("Couldn't resolve address %s"\
                                                % input, input)
                        else:
                                self.inputs.append(input)
                
                random.shuffle(self.inputs)

                self.line_count = 0
                if do_sort:
                        total_size = 0
                        for input in self.inputs:
                                fd, sze, url = connect_input(input, params)
                                total_size += sze

                        msg("Reduce[%d] input is %.2fMB" %\
                                (Task.id, total_size / 1024.0**2))

                        if total_size > mem_sort_limit:
                                self.iterator = self.download_and_sort(params)
                        else: 
                                msg("Sorting in memory")
                                m = list(self.multi_file_iterator(self.inputs, False))
                                m.sort(num_cmp)
                                self.iterator = self.list_iterator(m)
                else:
                        self.iterator = self.multi_file_iterator(self.inputs, params)
                        
        def iter(self):
                return self.iterator

        def download_and_sort(self, params):
                dlname = Task.path("REDUCE_DL", Task.id)[0]
                msg("Reduce will be downloaded to %s" % dlname)
                out_fd = AtomicFile(dlname, "w")
                for url in self.inputs:
                        fd, sze, url = connect_input(url, params)
                        for k, v in fun_reader(fd, sze, url):
                                if " " in k:
                                        err("Spaces are not allowed in keys "\
                                            "with external sort.")
                                if "\0" in v:
                                        err("Zero bytes are not allowed in "\
                                            "values with external sort. "\
                                            "Consider using base64 encoding.")
                                out_fd.write("%s %s\0" % (k, v))
                out_fd.close()
                msg("Reduce input downloaded ok")

                msg("Starting external sort")
                sortname = Task.path("REDUCE_SORTED", Task.id)[0]
                ensure_path(sortname, False)
                cmd = ["sort", "-n", "-k", "1,1", "-z",\
                        "-t", " ", "-o", sortname, dlname]

                proc = subprocess.Popen(cmd)
                ret = proc.wait()
                if ret:
                        err("Sorting %s to %s failed (%d)" %\
                                (dlname, sortname, ret))

                msg("External sort done: %s" % sortname)
                return self.multi_file_iterator([sortname], params, reader =\
                        lambda fd, sze, url:\
                                re_reader("(?s)(.*?) (.*?)\000", fd, sze, url))


        def list_iterator(self, lst):
                i = 0
                for x in lst:
                        yield x
                        i += 1
                        if status_interval and not i % status_interval:
                                msg("%d entries reduced" % i)
                msg("Reduce done: %d entries reduced in total" % i)

        def multi_file_iterator(self, inputs, params, progress = True,
                                reader = fun_reader):
                i = 0
                for url in inputs:
                        fd, sze, url = connect_input(url, params)
                        for x in reader(fd, sze, url):
                                yield x
                                i += 1
                                if progress and status_interval and\
                                        not i % status_interval:
                                        msg("%d entries reduced" % i)

                if progress:
                        msg("Reduce done: %d entries reduced in total" % i)


def run_map(job_input, partitions, param):
        i = 0
        fd, sze, url = connect_input(job_input, param)
        nr_reduces = max(1, Task.num_partitions)
        reader = fun_reader(fd, sze, url)
        fun_init(reader, param)

        for entry in reader:
                for key, value in fun_map(entry, param):
                        p = fun_partition(key, nr_reduces, param)
                        partitions[p].add(key, value)
                i += 1
                if status_interval and not i % status_interval:
                        msg("%d entries mapped" % i)

        msg("Done: %d entries mapped in total" % i)

def import_modules(modules):
        funcs = [f for n, f in globals().items() if n.startswith("fun_")]
        mod = [(m, __import__(m, fromlist = [m])) for m in modules]
        for n, m in mod: 
                for fun in funcs:
                        fun.func_globals.setdefault(n.split(".")[-1], m)

def load_stack(job, mode, inout):
        key = "%s_%s_stream" % (mode, inout)
        if key in job:
                s = [(k, marshal.loads(v))
                        for k, v in decode_netstring_str(job[key])]
        else:
                s = [("disco.func.%s" % key, getattr(disco.func, key).func_code)]
        return s

def init_common(job):
        global status_interval, input_stream_stack, output_stream_stack
        if 'required_files' in job:
                path = Task.path("REQ_FILES")[0]
                write_files(marshal.loads(job['required_files']), path)
                sys.path.insert(0, path)
        
        Task.num_partitions = int(job['nr_reduces'])
        status_interval = int(job['status_interval']) 
        
        input_stream_stack = load_stack(job, Task.mode, "input")
        output_stream_stack = load_stack(job, Task.mode, "output")
        
        req_mod = job['required_modules'].split()
        import_modules(req_mod)

def op_map(job):
        msg("Received a new map job!")
        
        if len(Task.inputs) != 1:
                err("Map can only handle one input. Got: %s" % 
                        " ".join(Task.inputs))

        fun_reader.func_code = marshal.loads(job['map_reader'])
        fun_writer.func_code = marshal.loads(job['map_writer'])
        fun_partition.func_code = marshal.loads(job['partition'])

        if 'map_init' in job:
                fun_init.func_code = marshal.loads(job['map_init'])
        
        if 'ext_map' in job:
                if 'ext_params' in job:
                        map_params = job['ext_params']
                else:
                        map_params = "0\n"
                
                path = Task.path("EXT_MAP")[0]
                external.prepare(job['ext_map'], map_params, path)
                fun_map.func_code = external.ext_map.func_code
        else:
                map_params = cPickle.loads(job['params'])
                fun_map.func_code = marshal.loads(job['map'])
        
        init_common(job)
        
        nr_part = max(1, Task.num_partitions)

        if 'combiner' in job:
                fun_combiner.func_code = marshal.loads(job['combiner'])
                partitions = [MapOutput(i, map_params, fun_combiner)\
                        for i in range(nr_part)]
        else:
                partitions = [MapOutput(i, map_params) for i in range(nr_part)]
        
        run_map(Task.inputs[0], partitions, map_params)
        external.close_ext()
        
        urls = {}
        for i, p in enumerate(partitions):
                p.close()
                urls["%d %s" % (i, p.url())] = True

        index, index_url = Task.path("MAP_INDEX", scheme = "dir")
        safe_update(index, urls)
        msg(index_url, "OUT")

def op_reduce(job):
        msg("Received a new reduce job!")

        do_sort = int(job['sort'])
        mem_sort_limit = int(job['mem_sort_limit'])
        
        if 'reduce_init' in job:
                fun_init.func_code = marshal.loads(job['reduce_init'])

        fun_reader.func_code = marshal.loads(job['reduce_reader'])
        fun_writer.func_code = marshal.loads(job['reduce_writer'])

        if 'ext_reduce' in job:
                if "ext_params" in job:
                        red_params = job['ext_params']
                else:
                        red_params = "0\n"

                path = Task.path("EXT_MAP")[0]
                external.prepare(job['ext_reduce'], red_params, path)
                fun_reduce.func_code = external.ext_reduce.func_code
        else:
                fun_reduce.func_code = marshal.loads(job['reduce'])
                red_params = cPickle.loads(job['params'])
        
        init_common(job)

        red_in = ReduceReader(Task.inputs, do_sort,
                        mem_sort_limit, red_params).iter()
        red_out = ReduceOutput(red_params)

        msg("Starting reduce")
        fun_init(red_in, red_params)
        fun_reduce(red_in, red_out, red_params)
        msg("Reduce done")

        red_out.close()
        external.close_ext()
        
        index, index_url = Task.path("REDUCE_INDEX", scheme = "dir")
        safe_update(index, {"%d %s" % (Task.id, red_out.url()): True})
        msg(index_url, "OUT")
