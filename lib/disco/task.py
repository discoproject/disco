import hashlib, os, random, re, subprocess, sys, cPickle, time

from functools import partial
from itertools import chain
from types import FunctionType

from disco import func, comm, util
from disco.ddfs import DDFS
from disco.core import Disco, JobDict
from disco.error import DiscoError, DataError
from disco.events import Status, OutputURL, TaskFailed
from disco.fileutils import AtomicFile, ensure_file, ensure_path, write_files
from disco.node import external, worker
from disco.settings import DiscoSettings

oob_chars = re.compile(r'[^a-zA-Z_\-:0-9]')

class Task(object):
    def __init__(self,
                 netlocstr='',
                 id=-1,
                 inputs=None,
                 jobdict=None,
                 jobname='',
                 settings=DiscoSettings()):
        self.netloc   = util.netloc.parse(netlocstr)
        self.id       = int(id)
        self.inputs   = inputs
        self.jobdict  = jobdict
        self.jobname  = jobname
        self.settings = settings
        self.blobs    = []
        self.mode     = self.__class__.__name__.lower()
        self.run_id   = "%s:%d-%x-%x" % (self.mode,
                                         self.id,
                                         int(time.time() * 1000),
                                         os.getpid())

        if not jobdict:
            self.jobdict = JobDict.unpack(open(self.jobpack),
                                          globals=worker.__dict__)
        self.insert_globals(self.functions)

    def __getattr__(self, key):
        if key in self.jobdict:
            return self.jobdict[key]
        task_key = '%s_%s' % (self.mode, key)
        if task_key in self.jobdict:
            return self.jobdict[task_key]
        raise AttributeError("%s has no attribute %s" % (self, key))

    def __iter__(self):
        return self.entries

    def hex_key(self, name):
        return hashlib.md5(name).hexdigest()[:2]

    @property
    def host(self):
        return self.netloc[0]

    @property
    def ispartitioned(self):
        return bool(self.jobdict['partitions'])

    @property
    def jobpack(self):
        jobpack = os.path.join(self.jobroot, 'jobpack.dl')
        def data():
            return Disco(self.master).jobpack(self.jobname)
        ensure_path(self.jobroot)
        ensure_file(jobpack, data=data, mode=444)
        return jobpack

    @property
    def jobpath(self):
        return os.path.join(self.host,
                            self.hex_key(self.jobname),
                            self.jobname)

    @property
    def taskpath(self):
        return os.path.join(self.jobpath,
                            self.hex_key(self.run_id),
                            self.run_id)

    @property
    def jobroot(self):
        return os.path.join(self.settings['DISCO_DATA'], self.jobpath)

    @property
    def taskroot(self):
        return os.path.join(self.settings['DISCO_DATA'], self.taskpath)

    @property
    def lib(self):
        return os.path.join(self.jobroot, 'lib')

    @property
    def master(self):
        return self.settings['DISCO_MASTER']

    @property
    def partid(self):
        return None

    @property
    def port(self):
        return self.settings['DISCO_PORT']

    @property
    def root(self):
        return self.settings['DISCO_ROOT']

    @property
    def sort_buffer_size(self):
        return self.settings['DISCO_SORT_BUFFER_SIZE']

    @property
    def map_index(self):
        filename = 'map-index.txt'
        return self.path(filename), self.url(filename, scheme='dir')

    @property
    def reduce_index(self):
        filename = 'reduce-index.txt'
        return self.path(filename), self.url(filename, scheme='dir')

    @property
    def reduce_output(self):
        filename = 'reduce-disco-%d' % self.id
        return self.path(filename), self.url(filename)

    def map_output(self, partition):
        filename = 'map-disco-%d-%.9d' % (self.id, partition)
        return self.path(filename), self.url(filename)

    def partition_output(self, partition):
        filename = 'part-disco-%.9d' % partition
        return self.path(filename), self.url(filename, scheme='part')

    def path(self, filename):
        return os.path.join(self.taskroot, filename)

    def url(self, filename, scheme='disco'):
        return '%s://%s/disco/%s/%s' % (scheme, self.host, self.taskpath, filename)

    def open_url(self, url):
        scheme, netloc, rest = util.urlsplit(url, localhost=self.host)
        if scheme == 'file':
            return comm.open_local(rest)
        return comm.open_remote('%s://%s/%s' % (scheme, netloc, rest))

    def track_status(self, iterator, message_template):
        status_interval = self.status_interval
        n = -1
        for n, item in enumerate(iterator):
            if status_interval and (n + 1) % status_interval == 0:
                Status(message_template % (n + 1))
            yield item
        Status("Done: %s" % (message_template % (n + 1)))

    def connect_input(self, url, fd=None, size=None):
        def fd_tuple(object, *args):
            if isinstance(object, tuple):
                return object
            return (object,) + args

        for input_stream in self.input_stream:
            fd, size, url = fd_tuple(input_stream(fd, size, url, self.params),
                                     size, url)

        # backwards compatibility for readers
        if self.reader:
            if util.argcount(self.reader) == 3:
                return fd_tuple(self.reader(fd, size, url), size, url)
            return fd_tuple(self.reader(fd, size, url, self.params), size, url)
        return fd, size, url

    def connect_output(self, part=0, fd=None, url=None):
        fd_list = []
        for output_stream in self.output_stream:
            fd, url = output_stream(fd, part, url, self.params)
            fd_list.append(fd)

        # backwards compatibility for writers
        if self.writer:
            fd.add = lambda k, v: self.writer(fd, k, v, self.params)
        return fd, url, fd_list

    def close_output(self, fd_list):
        for fd in reversed(fd_list):
            if hasattr(fd, 'close'):
                fd.close()

    @property
    def connected_inputs(self):
        shuffled = list(self.inputs)
        random.shuffle(shuffled)
        inputs = [url for input in shuffled
                  for url in util.urllist(input, partid=self.partid)]
        for input in inputs:
            yield self.connect_input(input)

    @property
    def entries(self):
        for fd, size, url in self.connected_inputs:
            for entry in fd:
                yield entry

    @property
    def functions(self):
        for fn in chain((getattr(self, name) for name in self.jobdict.functions),
                        *(getattr(self, stack) for stack in self.jobdict.stacks)):
            if fn:
                yield fn

    def insert_globals(self, functions):
        write_files(self.required_files, self.lib)
        sys.path.insert(0, self.lib)
        for fn in functions:
            if isinstance(fn, partial):
                fn = fn.func
            if isinstance(fn, FunctionType):
                fn.func_globals.setdefault('Task', self)
                for module in self.required_modules:
                    mod_name = module[0] if util.iskv(module) else module
                    mod = __import__(mod_name, fromlist=[mod_name])
                    fn.func_globals.setdefault(mod_name.split('.')[-1], mod)

    def run(self):
        assert self.version == '%s.%s' % sys.version_info[:2], "Python version mismatch"
        ensure_path(self.taskroot)
        os.chdir(self.taskroot)
        os.symlink(self.lib, 'lib')
        self._run_profile() if self.profile else self._run()

    def _run_profile(self):
        from cProfile import runctx
        name = 'profile-%s-%s' % (self.mode, self.id)
        path = os.path.join(self.taskroot, name)
        runctx('self._run()', globals(), locals(), path)
        self.put(name, file(path).read())

    def get(self, key, job=None):
        """
        Gets an out-of-band result assigned with the key *key*. The job name *job*
        defaults to the current job.

        Given the semantics of OOB results (see above), this means that the default
        value is only good for the reduce phase which can access results produced
        in the preceding map phase.
        """
        return util.load_oob(self.master, job or self.jobname, key)

    def put(self, key, value):
        """
        Stores an out-of-band result *value* with the key *key*. Key must be unique in
        this job. Maximum key length is 256 characters. Only characters in the set
        ``[a-zA-Z_\-:0-9@]`` are allowed in the key.
        """
        if DDFS.safe_name(key) != key:
            raise DiscoError("OOB key contains invalid characters (%s)" % key)
        util.save_oob(self.master, self.jobname, key, value)

class Map(Task):
    def _run(self):
        if len(self.inputs) != 1:
            TaskFailed("Map takes 1 input, got: %s" % ' '.join(self.inputs))

        if self.save and not self.reduce and self.ispartitioned:
            TaskFailed("Storing partitioned outputs in DDFS is not yet supported")

        if self.ext_map:
            external.prepare(self.map, self.ext_params, self.path('ext.map'))
            self.map = FunctionType(external.ext_map.func_code,
                                    globals=external.__dict__)
            self.insert_globals([self.map])

        entries = self.track_status(self, "%s entries mapped")
        params  = self.params
        outputs = [MapOutput(self, i)
                   for i in xrange(max(1, int(self.jobdict['partitions'])))]

        self.init(entries, params)
        for entry in entries:
            for k, v in self.map(entry, params):
                outputs[self.partition(k, len(outputs), params)].add(k, v)

        external.close_ext()

        index, index_url = self.map_index

        f = file(index, 'w')
        for i, output in enumerate(outputs):
            print >> f, '%d %s' % (i, output.url)
            output.close()
        f.close()

        if self.save and not self.reduce:
            OutputURL(util.ddfs_save(self.blobs, self.jobname, self.master))
            Status("Results pushed to DDFS")
        else:
            OutputURL(index_url)

class MapOutput(object):
    def __init__(self, task, id):
        self.task = task
        self.comb_buffer = {}
        self.fd, self.url, self.fd_list = task.connect_output(id)

    def add(self, key, value):
        if self.task.combiner:
            ret = self.task.combiner(key, value,
                                     self.comb_buffer,
                                     0,
                                     self.task.params)
            if ret:
                for key, value in ret:
                    self.fd.add(key, value)
        else:
            self.fd.add(key, value)

    def close(self):
        if self.task.combiner:
            ret = self.task.combiner(None, None,
                                     self.comb_buffer,
                                     1,
                                     self.task.params)
            if ret:
                for key, value in ret:
                    self.fd.add(key, value)
        self.task.close_output(self.fd_list)

class Reduce(Task):
    def _run(self):
        entries = self.track_status(self, "%s entries reduced")
        red_out, out_url, fd_list = self.connect_output()
        params = self.params

        if self.ext_reduce:
            external.prepare(self.reduce, self.ext_params, self.path('ext.reduce'))
            self.reduce = FunctionType(external.ext_reduce.func_code,
                                       globals=external.__dict__)
            self.insert_globals([self.reduce])

        total_size = sum(size for fd, size, url in self.connected_inputs)
        Status("Input is %s" % (util.format_size(total_size)))

        self.init(entries, params)
        if util.argcount(self.reduce) < 3:
            for k, v in self.reduce(entries, *(params, )):
                red_out.add(k, v)
        else:
            self.reduce(entries, red_out, params)

        self.close_output(fd_list)
        external.close_ext()

        if self.save:
            OutputURL(util.ddfs_save(self.blobs, self.jobname, self.master))
            Status("Results pushed to DDFS")
        else:
            index, index_url = self.reduce_index
            f = file(index, 'w')
            print >> f, '%d %s' % (self.id, out_url)
            f.close()
            OutputURL(index_url)

    def __iter__(self):
        if self.sort == 'merge':
            return self.merge_sorted_entries
        elif self.sort:
            return self.sorted_entries
        return self.entries

    def disk_sort(self, filename):
        Status("Sorting %s..." % filename)
        try:
            subprocess.check_call(['sort',
                                   '-z',
                                   '-t', '\xff',
                                   '-k', '1,1',
                                   '-T', '.',
                                   '-S', self.sort_buffer_size,
                                   '-o', filename,
                                   filename])
        except subprocess.CalledProcessError, e:
            raise DataError("Sorting %s failed: %s" % (filename, e))
        Status("Finished sorting")

    @property
    def merge_sorted_entries(self):
        from disco.future import merge
        return merge(*(fd for fd, size, url in self.connected_inputs))

    @property
    def sorted_entries(self):
        dlname = self.path('reduce-in-%d.dl' % self.id)
        Status("Downloading %s" % dlname)
        out_fd = AtomicFile(dlname, 'w')
        for key, value in self.entries:
            if not isinstance(key, str):
                raise ValueError("Keys must be strings for external sort", key)
            if '\xff' in key or '\x00' in key:
                raise ValueError("Cannot sort key with 0xFF or 0x00 bytes", key)
            else:
                # value pickled using protocol 0 will always be printable ASCII
                out_fd.write('%s\xff%s\x00' % (key, cPickle.dumps(value, 0)))
        out_fd.close()
        Status("Downloaded OK")

        self.disk_sort(dlname)
        fd, size, url = comm.open_local(dlname)
        for k, v in func.re_reader("(?s)(.*?)\xff(.*?)\x00", fd, size, url):
            yield k, cPickle.loads(v)

    @property
    def params(self):
        if self.ext_reduce:
            return self.ext_params or '0\n'
        return self.jobdict['params']

    @property
    def partid(self):
        if self.ispartitioned or self.jobdict.input_is_partitioned:
            if not self.jobdict['merge_partitions']:
                return self.id
