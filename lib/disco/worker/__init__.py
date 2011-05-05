#!/usr/bin/env python
"""
:mod:`disco.worker` -- Python Worker Interface
==============================================

In Disco, :term:`workers <worker>` do the brunt of the data processing work.
When a :class:`disco.job.Job` is created, it gets passed a :class:`Worker` instance,
which is responsible for defining the fields used by the :class:`disco.job.JobPack`.
In most cases, you don't need to define your own Worker subclass in order to run a job.
The Worker classes defined in :mod:`disco` will take care of the details
of creating the fields necessary for the :class:`disco.job.JobPack`,
and when executed on the nodes,
will handle the implementation of the :ref:`worker_protocol`.

There is perhaps a subtle, but important, distinction between
a :term:`worker` and a :class:`Worker`.
The former refers to any binary that gets executed on the nodes,
specified by :attr:`jobdict.worker`.
The latter is a Python class,
which handles details of submitting the job on the client side,
as well as controlling the execution of user-defined code on the nodes.
A :class:`Worker` can be subclassed trivially to create a new :term:`worker`,
without having to worry about fulfilling many of the requirements
for a well-behaving worker.
In short,
a :class:`Worker` provides Python library support for a Disco :term:`worker`.
Those wishing to write a worker in a language besides Python may make use of
the Worker class for submitting jobs to the master,
but generally need to handle the :ref:`worker_protocol`
in the language used for the worker executable.

The :class:`Classic Worker <disco.worker.classic.worker.Worker>`
is a subclass of :class:`Worker`,
which implements the classic Disco :term:`mapreduce` interface.

The following steps illustrate the sequence of events for running a :term:`job`
using a standard :class:`Worker`:

#. (client) instantiate a :class:`disco.job.Job`
        #. if a worker is supplied, use that worker
        #. otherwise, create a worker using :attr:`disco.job.Job.Worker`
           (the default is :class:`disco.worker.classic.worker.Worker`)
#. (client) call :meth:`disco.job.Job.run`
        #. create a :class:`disco.job.JobPack` using:
           :meth:`Worker.jobdict`,
           :meth:`Worker.jobenvs`,
           :meth:`Worker.jobhome`,
           :meth:`disco.task.jobdata`
        #. submit the :class:`disco.job.JobPack` to the master
#. (node) master unpacks the :term:`job home`
#. (node) master executes the :attr:`jobdict.worker` with
   current working directory set to the :term:`job home` and
   environment variables set from :ref:`jobenvs`
#. (node) worker requests the :class:`disco.task.Task` from the master
#. (node) worker runs the :term:`task` and reports the output to the master
"""
import os, sys, time, traceback

from disco.error import DataError
from disco.fileutils import DiscoOutput, NonBlockingInput, Wait

class MessageWriter(object):
    def __init__(self, worker):
        self.worker = worker

    @classmethod
    def force_utf8(cls, string):
        if isinstance(string, unicode):
            return string.encode('utf-8', 'replace')
        return string.decode('utf-8', 'replace').encode('utf-8')

    def write(self, string):
        string = string.strip()
        if string:
            self.worker.send('MSG', self.force_utf8(string))

class Worker(dict):
    """
    A :class:`Worker` is a :class:`dict` subclass,
    with special methods defined for serializing itself,
    and possibly reinstantiating itself on the nodes where :term:`tasks <task>` are run.

    The :class:`Worker` base class defines the following parameters:

    :type  map: function or None
    :param map: called when the :class:`Worker` is :meth:`run` with a
                :class:`disco.task.Task` in mode *map*.
                Also used by :meth:`jobdict` to set :attr:`jobdict.map?`.

    :type  reduce: function or None
    :param reduce: called when the :class:`Worker` is :meth:`run` with a
                   :class:`disco.task.Task` in mode *reduce*.
                   Also used by :meth:`jobdict` to set :attr:`jobdict.reduce?`.

    :type  save: bool
    :param save: whether or not to save the output to :ref:`DDFS`.

    :type  profile: bool
    :param profile: determines whether :meth:`run` will be profiled.
    """
    def __init__(self, **kwargs):
        super(Worker, self).__init__(self.defaults())
        self.update(kwargs)
        self.outputs = {}

    @property
    def bin(self):
        """
        The path to the :term:`worker` binary, relative to the :term:`job home`.
        Used to set :attr:`jobdict.worker` in :meth:`jobdict`.
        """
        from inspect import getsourcefile, getmodule
        return getsourcefile(getmodule(self)).strip('/')

    def defaults(self):
        """
        :return: dict of default values for the :class:`Worker`.
        """
        return {'map': None,
                'merge_partitions': False, # XXX: maybe deprecated
                'reduce': None,
                'save': False,
                'partitions': 1,  # move to classic once partitions are dynamic
                'profile': False}

    def getitem(self, key, job, jobargs, default=None):
        """
        Resolves ``key`` in the following order:
                #. ``jobargs`` (parameters passed in during :meth:`disco.job.Job.run`)
                #. ``job`` (attributes of the :class:`disco.job.Job`)
                #. ``self`` (items in the :class:`Worker` dict itself)
                #. ``default``
        """
        if key in jobargs:
            return jobargs[key]
        elif hasattr(job, key):
            return getattr(job, key)
        return self.get(key, default)

    def jobdict(self, job, **jobargs):
        """
        Creates :ref:`jobdict` for the :class:`Worker`.

        Makes use of the following parameters,
        in addition to those defined by the :class:`Worker` itself:

        :type  input: list of urls or list of list of urls
        :param input: used to set :attr:`jobdict.input`.
                Disco natively handles the following url schemes:

                * ``http://...`` - any HTTP address
                * ``file://...`` or no scheme - a local file.
                    The file must exist on all nodes where the tasks are run.
                    Due to these restrictions, this form has only limited use.
                * ``tag://...`` - a tag stored in :ref:`DDFS`
                * ``raw://...`` - pseudo-address: use the address itself as data.
                * ``dir://...`` - used by Disco internally.
                * ``disco://...`` - used by Disco internally.

                .. seealso:: :mod:`disco.schemes`.

        :type  name: string
        :param name: directly sets :attr:`jobdict.prefix`.

        :type  owner: string
        :param owner: directly sets :attr:`jobdict.owner`.
                      If not specified, uses :envvar:`DISCO_JOB_OWNER`.

        :type  scheduler: dict
        :param scheduler: directly sets :attr:`jobdict.scheduler`.

        Uses :meth:`getitem` to resolve the values of parameters.

        :return: the :term:`job dict`.
        """
        from disco.util import inputlist, ispartitioned, read_index
        def get(key, default=None):
            return self.getitem(key, job, jobargs, default)
        has_map = bool(get('map'))
        has_reduce = bool(get('reduce'))
        input = inputlist(get('input', []),
                          partition=None if has_map else False,
                          settings=job.settings)

        # -- nr_reduces --
        # ignored if there is not actually a reduce specified
        # XXX: master should always handle this
        if has_map:
            # partitioned map has N reduces; non-partitioned map has 1 reduce
            nr_reduces = get('partitions') or 1
        elif ispartitioned(input):
            # no map, with partitions: len(dir://) specifies nr_reduces
            nr_reduces = 1 + max(int(id)
                                 for dir in input
                                 for id, url in read_index(dir))
        else:
            # no map, without partitions can only have 1 reduce
            nr_reduces = 1

        if get('merge_partitions'):
            nr_reduces = 1

        return {'input': input,
                'worker': self.bin,
                'map?': has_map,
                'reduce?': has_reduce,
                'nr_reduces': nr_reduces,
                'prefix': get('name'),
                'scheduler': get('scheduler', {}),
                'owner': get('owner', job.settings['DISCO_JOB_OWNER'])}

    def jobenvs(self, job, **jobargs):
        """
        :return: :ref:`jobenvs` dict.
        """
        return {'PYTHONPATH': ':'.join([path.strip('/') for path in sys.path])}

    def jobhome(self, job, **jobargs):
        """
        :return: the :term:`job home` (serialized).

        Calls :meth:`jobzip` to create the :class:`disco.fileutils.DiscoZipFile`.
        """
        jobzip = self.jobzip(job, **jobargs)
        jobzip.close()
        return jobzip.dumps()

    def jobzip(self, job, **jobargs):
        """
        A hook provided by the :class:`Worker` for creating the :term:`job home` zip.

        :return: a :class:`disco.fileutils.DiscoZipFile`.
        """
        from clx import __file__ as clxpath
        from disco import __file__ as discopath
        from disco.fileutils import DiscoZipFile
        jobzip = DiscoZipFile()
        jobzip.writepath(os.path.dirname(clxpath), exclude=('.pyc',))
        jobzip.writepath(os.path.dirname(discopath), exclude=('.pyc',))
        jobzip.writesource(job)
        jobzip.writesource(self)
        return jobzip

    def input(self, task, merged=False, **kwds):
        """
        :type  task: :class:`disco.task.Task`
        :param task: the task for which to retrieve input.

        :type  merged: bool
        :param merged: if specified, returns a :class:`MergedInput`.

        :type  kwds: dict
        :param kwds: additional keyword arguments for the :class:`Input`.

        :return: an :class:`Input` to iterate over the inputs from the master.
        """
        if merged:
            return MergedInput(self.get_inputs(), task=task, **kwds)
        return SerialInput(self.get_inputs(), task=task, **kwds)

    def output(self, task, partition=None, **kwds):
        """
        :type  task: :class:`disco.task.Task`
        :param task: the task for which to create output.

        :type  partition: string or None
        :param partition: the label of the output partition to get.

        :type  kwds: dict
        :param kwds: additional keyword arguments for the :class:`Output`.

        :return: the previously opened :class:`Output` for *partition*,
                 or if necessary, a newly opened one.
        """
        if partition not in self.outputs:
            self.outputs[partition] = Output(task.output(partition=partition), **kwds)
        return self.outputs[partition]

    def start(self, task, job, **jobargs):
        from disco.sysutil import set_mem_limit
        set_mem_limit(job.settings['DISCO_WORKER_MAX_MEM'])
        task.makedirs()
        if self.getitem('profile', job, jobargs):
            from cProfile import runctx
            name = 'profile-%s' % task.uid
            path = task.path(name)
            runctx('self.run(task, job, **jobargs)', globals(), locals(), path)
            task.put(name, open(path).read())
        else:
            self.run(task, job, **jobargs)
        self.end(task, job, **jobargs)

    def run(self, task, job, **jobargs):
        """
        Called to do the actual work of processing the :class:`disco.task.Task`.
        """
        self.getitem(task.mode, job, jobargs)(task, job, **jobargs)

    def end(self, task, job, **jobargs):
        def get(key):
            return self.getitem(key, job, jobargs)
        if not get('save') or (task.mode == 'map' and get('reduce')):
            self.send_outputs()
            self.send('MSG', "Results sent to master")
        else:
            self.save_outputs(task.jobname, master=task.master)
            self.send('MSG', "Results saved to DDFS")

    @classmethod
    def main(cls):
        """
        The main method used to bootstrap the :class:`Worker` when it is being executed.

        It is enough for the module to define::

                if __name__ == '__main__':
                    Worker.main()

        .. note:: It is critical that subclasses check if they are executing
                  in the ``__main__`` module, before running :meth:`main`,
                  as the worker module is also generally imported on the client side.
        """
        try:
            sys.stdin = NonBlockingInput(sys.stdin, timeout=600)
            sys.stdout = MessageWriter(cls)
            cls.send('WORKER', {'pid': os.getpid(), 'version': "1.0"})
            task = cls.get_task()
            job, jobargs = task.jobobjs
            job.worker.start(task, job, **jobargs)
            cls.send('DONE')
        except (DataError, EnvironmentError, MemoryError), e:
            # check the number of open file descriptors (under proc), warn if close to max
            # http://stackoverflow.com/questions/899038/getting-the-highest-allocated-file-descriptor
            # also check for other known reasons for error, such as if disk is full
            cls.send('ERROR', traceback.format_exc())
            raise
        except Exception, e:
            cls.send('FATAL', MessageWriter.force_utf8(traceback.format_exc()))
            raise

    @classmethod
    def send(cls, type, payload=''):
        from disco.json import dumps, loads
        body = dumps(payload)
        sys.stderr.write('%s %d %s\n' % (type, len(body), body))
        spent, rtype = sys.stdin.t_read_until(' ')
        spent, rsize = sys.stdin.t_read_until(' ', spent=spent)
        spent, rbody = sys.stdin.t_read(int(rsize) + 1, spent=spent)
        if type == 'ERROR':
            raise ValueError(loads(rbody[:-1]))
        return loads(rbody[:-1])

    @classmethod
    def get_input(cls, id):
        done, inputs = cls.send('INPUT', ['include', [id]])
        _id, status, replicas = inputs[0]

        if status == 'busy':
            raise Wait
        if status == 'failed':
            raise DataError("Can't handle broken input", id)
        return [(id, str(url)) for id, url in replicas]

    @classmethod
    def get_inputs(cls, done=False, exclude=()):
        while not done:
            done, inputs = cls.send('INPUT')
            for id, _status, _replicas in inputs:
                if id not in exclude:
                    yield IDedInput((cls, id))
                    exclude += (id, )

    @classmethod
    def get_task(cls):
        from disco.task import Task
        return Task(**dict((str(k), v) for k, v in cls.send('TASK').items()))

    def save_outputs(self, jobname, master=None):
        from disco.ddfs import DDFS
        def paths():
            for output in self.outputs.values():
                output.file.close()
                yield output.path
        self.send('OUTPUT', [DDFS(master).save(jobname, paths()), 'tag'])

    def send_outputs(self):
        for output in self.outputs.values():
            output.file.close()
            self.send('OUTPUT', [output.path, output.type, output.partition])

class IDedInput(tuple):
    @property
    def worker(self):
        return self[0]

    @property
    def id(self):
        return self[1]

    @property
    def replicas(self):
        return self.worker.get_input(self.id)

    def unavailable(self, tried):
        return self.worker.send('INPUT_ERR', [self.id, list(tried)])

    def __str__(self):
        return '%s' % [url for rid, url in self.replicas]

class ReplicaIter(object):
    def __init__(self, input):
        self.input, self.used = input, set()

    def __iter__(self):
        return self

    def next(self):
        replicas = dict(self.input.replicas)
        repl_ids = set(replicas) - self.used
        for repl_id in repl_ids:
            self.used.add(repl_id)
            return replicas[repl_id]
        self.input.unavailable(self.used)
        raise StopIteration

class InputIter(object):
    def __init__(self, input, task=None, open=None, start=0):
        self.input = input
        if isinstance(input, IDedInput):
            self.urls = ReplicaIter(input)
        elif isinstance(input, basestring):
            self.urls = iter([input])
        else:
            self.urls = iter(input)
        self.last = start - 1
        self.open = open if open else Input.default_opener(task=task)
        self.swap()

    def __iter__(self):
        return self

    def next(self):
        try:
            self.last, item = self.iter.next()
            return item
        except DataError:
            self.swap(traceback.format_exc())
            raise Wait(0)

    def swap(self, error=None):
        try:
            def skip(iter, N):
                from itertools import dropwhile
                return dropwhile(lambda (n, rec): n < N, enumerate(iter))
            self.iter = skip(self.open(self.urls.next()), self.last + 1)
        except DataError:
            self.swap(traceback.format_exc())
        except StopIteration:
            if error:
                raise DataError("Exhausted all available replicas, "
                                "last error was:\n\n%s" % error, self.input)
            raise DataError("Exhausted all available replicas", self.input)

class Input(object):
    """
    An iterable over one or more :class:`Worker` inputs,
    which can gracefully handle corrupted replicas or otherwise failed inputs.

    :type  open: function
    :param open: a function with the following signature::

                        def open(url):
                            ...
                            return file

                used to open input files.
    """
    def __init__(self, input, task=None, **kwds):
        self.input, self.task, self.kwds = input, task, kwds

    def __iter__(self):
        iter = self.input_iter(self.input)
        while iter:
            try:
                for item in iter:
                    yield item
                iter = None
            except Wait, w:
                time.sleep(w.retry_after)

    def input_iter(self, input):
        return InputIter(self.input, task=self.task, **self.kwds)

    @classmethod
    def default_opener(cls, task):
        from disco import schemes
        def open(url):
            return schemes.open(url, task=task)
        return open

class Output(object):
    """
    A container for outputs from :class:`workers <Worker>`.

    :type  open: function
    :param open: a function with the following signature::

                        def open(url):
                            ...
                            return file

                used to open new output files.

    .. attribute:: path

        The path to the underlying output file.

    .. attribute:: type

        The type of output.

    .. attribute:: partition

        The partition label for the output (or None).

    .. attribute:: file

        The underlying output file handle.
    """
    def __init__(self, (path, type, partition), open=None):
        self.path, self.type, self.partition = path, type, partition
        self.open = open or DiscoOutput
        self.file = self.open(self.path)

class SerialInput(Input):
    """
    Produces an iterator over the records in a list of sequential inputs.
    """
    def __iter__(self):
        for input in self.input:
            for record in Input(input, task=self.task, **self.kwds):
                yield record

class ParallelInput(Input):
    """
    Produces an iterator over the unordered records in a set of inputs.

    Usually require the full set of inputs (i.e. will block with streaming).
    """
    BUSY_TIMEOUT = 1

    def __iter__(self):
        iters = [self.input_iter(input) for input in self.input]
        while iters:
            iter = iters.pop()
            try:
                for item in iter:
                    yield item
            except Wait, w:
                if not iters:
                    time.sleep(w.retry_after)
                iters.insert(0, iter)

    def couple(self, iters, heads, n):
        while True:
            if heads[n] is Wait:
                self.fill(iters, heads, n=n)
            head = heads[n]
            heads[n] = Wait
            yield head

    def fetch(self, iters, heads, stop=all):
        busy = 0
        for n, head in enumerate(heads):
            if head is Wait:
                try:
                    heads[n] = next(iters[n])
                except Wait:
                    if stop in (all, n):
                        busy += 1
                except StopIteration:
                    if stop in (all, n):
                        raise
        return busy

    def fill(self, iters, heads, n=all, busy=True):
        while busy:
            busy = self.fetch(iters, heads, stop=n)
            if busy:
                time.sleep(self.BUSY_TIMEOUT)
        return heads

class MergedInput(ParallelInput):
    """
    Produces an iterator over the minimal head elements of the inputs.
    """
    def __iter__(self):
        from disco.future import merge
        iters = [self.input_iter(input) for input in self.input]
        heads = [Wait] * len(iters)
        return merge(*(self.couple(iters, heads, n) for n in xrange(len(iters))))

if __name__ == '__main__':
    Worker.main()
