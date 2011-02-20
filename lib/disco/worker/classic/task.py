import os, re, sys, cPickle, time

from functools import partial
from types import FunctionType

from disco import comm, job, util
from disco.ddfs import DDFS
from disco.core import Disco
from disco.error import DiscoError
from disco.events import Output, Status
from disco.fileutils import ensure_file, ensure_path, AtomicFile

from disco.worker.classic import external, func

def putout(ofile, otype, olabel=None):
    output = [ofile, otype]
    if olabel:
        output.append(str(olabel))
    return Output(output).send()

def status(message):
    return Status(message).send()

def task(worker, jobdict, mode='map', **taskinfo):
    return globals()[mode.capitalize()](worker, jobdict, mode=mode, **taskinfo)

class Task(job.Task):
    def __init__(self, worker, jobdict, **taskinfo):
        super(Task, self).__init__(worker, jobdict, **taskinfo)
        for value in util.flatten(worker.values()):
            self.insert_task(value)

    def open_url(self, url):
        file, size, url = self.connect_input(url)
        self.open_hook(file, size, url)
        return file

    def open_hook(self, fd, size, url):
        pass

    def open(self, input):
        self.notifier(input)
        return super(Task, self).open(input)

    def run(self, *inputs):
        assert self.version == '%s.%s' % sys.version_info[:2], "Python version mismatch"
        ensure_path(self.taskpath)

        def open_hook(file, size, url):
            status("Input is %s" % (util.format_size(size)))
        self.open_hook = open_hook

        if self.isexternal:
            setattr(self, *external.prepare(self.params, self.mode))
        output = self._run(self.params, *inputs)
        output.close()
        external.close()

        if self.should_save:
            putout(DDFS(self.master).save(self.jobname, output.paths), 'tag')
            status("Results pushed to DDFS")
        else:
            for path, type, id in output.index:
                putout(path, type, id)
            status("Wrote index file")

    def __getattr__(self, key):
        if key in self.worker:
            return self.worker[key]
        task_key = '%s_%s' % (self.mode, key)
        if task_key in self.worker:
            return self.worker[task_key]
        raise AttributeError("%s has no attribute %s" % (self, key))

    @property
    def isexternal(self):
        if isinstance(getattr(self, self.mode), dict):
            return True

    @property
    def params(self):
        if self.isexternal:
            return self.ext_params
        return self.worker['params']

    @property
    def should_save(self):
        return self.save

    def connect_input(self, url, fd=None, size=None):
        def fd_tuple(object, *args):
            if isinstance(object, tuple):
                return object
            return (object,) + args

        for input_stream in self.input_stream:
            fd, size, url = fd_tuple(input_stream(fd, size, url, self.params), size, url)

        if self.reader:
            if util.argcount(self.reader) == 3:
                return fd_tuple(self.reader(fd, size, url), size, url)
            return fd_tuple(self.reader(fd, size, url, self.params), size, url)
        return fd, size, url

    def index(self, name, output):
        index = AtomicFile(self.path(name), 'w')
        index.write(output.index)
        index.close()
        return self.url(name, scheme='dir')

    def insert_task(self, object):
        if isinstance(object, partial):
            object = object.func
        if isinstance(object, FunctionType):
            object.func_globals.setdefault('Task', self)

    def outname(self, id):
        return '%s-disco-%s' % (self.mode, id)

    def outurls(self, partition):
        name = self.outname('%s-%s' % (self.taskid, partition))
        return self.path(name), self.url(name)

    def status_iter(self, iterator, message_template):
        status_interval = self.status_interval
        n = -1
        for n, item in enumerate(iterator):
            if status_interval and (n + 1) % status_interval == 0:
                status(message_template % (n + 1))
            yield item
        status("Done: %s" % (message_template % (n + 1)))

class Map(Task):
    def read(self, *inputs):
        return super(Map, self).read(*util.inputlist(*inputs))

    @property
    def should_save(self):
        return self.save and not self.reduce

    def _run(self, params, *inputs):
        if self.save and self.partitions and not self.reduce:
            raise NotImplementedError("Storing partitioned outputs in DDFS is not yet supported")
        output = OutputStreamDict(self)
        entries = self.status_iter(self.read(*inputs), "%s entries mapped")
        combiners = {}
        nr_partitions = max(1, self.partitions)
        self.init(entries, params)
        for entry in entries:
            for key, val in self.map(entry, params):
                partition = self.partition(key, nr_partitions, params)
                if self.combiner:
                    if partition not in combiners:
                        combiners[partition] = Combiner(self.combiner, params)
                    for record in combiners[partition].combine(key, val):
                        output.add(partition, *record)
                else:
                    output.add(partition, key, val)
        for partition, combiner in combiners.items():
            for record in combiner.combine(None, None, done=True):
                output.add(partition, *record)
        return output

    def outurls(self, partition):
        if self.partitions:
            name = self.outname(partition)
            return self.path(name), self.url(name, scheme='part')
        return super(Map, self).outurls(partition)

class Reduce(Task):
    def read(self, *inputs):
        from disco.util import inputlist, ispartitioned, shuffled
        if isinstance(self.sort, FunctionType):
            sort = self.sort
        elif self.sort:
            sort = func.disk_sort
        else:
            sort = func.nop_sort
        partition = None
        if ispartitioned(inputs) and not self.merge_partitions:
            partition = self.taskid
        return sort(self, (self.open(i)
                           for i in shuffled(inputlist(*inputs, partition=partition))))

    def _run(self, params, *inputs):
        entries = self.status_iter(self.read(*inputs), "%s entries reduced")
        output = OutputStream(self, self.taskid)
        self.init(entries, params)
        if util.argcount(self.reduce) < 3:
            for record in self.reduce(entries, *(params, )):
                output.add(*record)
        else:
            self.reduce(entries, output.fd, params)
        return output

class Combiner(object):
    def __init__(self, combiner, params):
        self.combiner = combiner
        self.buffer = {}
        self.params = params

    def combine(self, key, val, done=False):
        return self.combiner(key, val, self.buffer, done, self.params) or ()

class OutputStream(object):
    def __init__(self, task, id, type='disco', fd=None, url=None):
        self.fds = []
        for output_stream in task.output_stream:
            fd, url = output_stream(fd, id, url, task.params)
            self.fds.append(fd)
        self.type, self.id, self.fd, self.url = type, id, fd, url

    def add(self, *record):
        self.fd.add(*record)

    def close(self):
        for fd in reversed(self.fds):
            if hasattr(fd, 'close'):
                fd.close()

    @property
    def index(self):
        return [(self.fd.path, self.type, self.id)]

    @property
    def path(self):
        return self.fd.path

    @property
    def paths(self):
        yield self.fd.path

class OutputStreamDict(dict):
    def __init__(self, task):
        self.task = task

    def add(self, id, *record):
        if id not in self:
            self[id] = OutputStream(self.task, id, type='part')
        self[id].add(*record)

    def close(self):
        for output in self.values():
            output.close()

    @property
    def index(self):
        for output in self.values():
            yield (output.path, output.type, output.id)

    @property
    def paths(self):
        return util.flatten(output.paths for output in self.values())
