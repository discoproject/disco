#!/usr/bin/env python
"""
:mod:`disco.worker` -- Python Worker Interface
==============================================

In Disco, :term:`workers <worker>` do the brunt of the data processing
work.  When a :class:`disco.job.Job` is created, it gets passed
a :class:`Worker` instance, which is responsible for defining the fields
used by the :class:`disco.job.JobPack`.  In most cases, you don't need
to define your own Worker subclass in order to run a job.  The Worker
classes defined in :mod:`disco` will take care of the details of
creating the fields necessary for the :class:`disco.job.JobPack`, and
when executed on the nodes, will handle the implementation of
the :ref:`worker_protocol`.

There is perhaps a subtle, but important, distinction between
a :term:`worker` and a :class:`Worker`.  The former refers to any binary
that gets executed on the nodes, specified by :attr:`jobdict.worker`.
The latter is a Python class, which handles details of submitting the
job on the client side, as well as controlling the execution of
user-defined code on the nodes.  A :class:`Worker` can be subclassed
trivially to create a new :term:`worker`, without having to worry
about fulfilling many of the requirements for a well-behaving worker.
In short, a :class:`Worker` provides Python library support for a
Disco :term:`worker`.  Those wishing to write a worker in a language
besides Python may make use of the Worker class for submitting jobs to
the master, but generally need to handle the :ref:`worker_protocol` in
the language used for the worker executable.

The :class:`Classic Worker <disco.worker.classic.worker.Worker>` is
a subclass of :class:`Worker`, which implements the classic
Disco :term:`mapreduce` interface.

The following steps illustrate the sequence of events for running
a :term:`job` using a standard :class:`Worker`:

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

from disco.compat import basestring, force_utf8
from disco.error import DataError
from disco.fileutils import DiscoOutput, NonBlockingInput, Wait, AtomicFile
from disco.comm import open_url


# Maximum amount of time a task might take to finish.
DISCO_WORKER_MAX_TIME = 24 * 60 * 60

class MessageWriter(object):
    def __init__(self, worker):
        self.worker = worker

    def write(self, string):
        string = string.strip()
        if string:
            self.worker.send('MSG', force_utf8(string))

    def isatty(self):
        return False

    def flush(self):
        pass

class Worker(dict):
    """
    A :class:`Worker` is a :class:`dict` subclass, with special
    methods defined for serializing itself, and possibly
    reinstantiating itself on the nodes where :term:`tasks <task>` are
    run.

    .. note:: The base worker tries to guess which modules are needed
              automatically, for all of the :term:`job functions`
              specified below, if the *required_modules* parameter is
              not specified.  It sends any local dependencies
              (i.e. modules not included in the Python standard
              library) to nodes by default.

              If guessing fails, or you have other requirements, see
              :mod:`disco.worker.modutil` for options.



    The :class:`Worker` base class defines the following parameters:

    :type  save_results: bool
    :param save_results: whether or not to save the output to :ref:`DDFS`.

    :type save_info: string
    :param save_info: the information about saving into a DFS.

    :type  profile: bool
    :param profile: determines whether :meth:`run` will be profiled.

    :type  required_files: list of paths or dict
    :param required_files: additional files that are required by the worker.
                           Either a list of paths to files to include,
                           or a dictionary which contains items of the form
                           ``(filename, filecontents)``.

                           .. versionchanged:: 0.4
                              The worker includes *required_files* in :meth:`jobzip`,
                              so they are available relative to the working directory
                              of the worker.

    :type  required_modules: see :ref:`modspec`
    :param required_modules: required modules to send with the worker.
    """
    stderr = sys.stderr

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
        return {'save_results': False,
                'profile': False,
                'required_files': {},
                'required_modules': None}

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

    def get_modules(self, job, **jobargs):
        from disco.worker.modutil import find_modules
        from disco.util import iterify
        def get(key):
            return self.getitem(key, job, jobargs)
        from inspect import getsourcefile, getmodule
        job_path = getsourcefile(getmodule(job))

        return find_modules([obj
                             for key in self
                             for obj in iterify(get(key))
                             if callable(obj)],
                             job_path=job_path,
                            exclude=['Task'])


    def jobdict(self, job, **jobargs):
        """
        Creates a basic :ref:`jobdict` for the :class:`Worker`.

        Makes use of the following parameters:

        :type  name: string
        :param name: directly sets :attr:`jobdict.prefix`.

        :type  owner: string
        :param owner: directly sets :attr:`jobdict.owner`.
                      If not specified, uses :envvar:`DISCO_JOB_OWNER`.

        :return: :ref:`jobdict` dict.
        """
        return {'prefix': self.getitem('name', job, jobargs),
                'save_results': self.getitem('save_results', job, jobargs, False),
                'save_info': self.getitem('save_info', job, jobargs, "ddfs"),
                'scheduler': self.getitem('scheduler', job, jobargs, {}),
                'owner': self.getitem('owner', job, jobargs,
                                      job.settings['DISCO_JOB_OWNER'])}

    def jobenvs(self, job, **jobargs):
        """
        :return: :ref:`jobenvs` dict.
        """
        envs = {'PYTHONPATH': ':'.join([path.strip('/') for path in sys.path])}
        envs['LD_LIBRARY_PATH'] = 'lib'
        envs['PYTHONPATH'] = ':'.join(('lib', envs.get('PYTHONPATH', '')))
        return envs

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
        A hook provided by the :class:`Worker` for creating the
        :term:`job home` zip.  The base implementation creates a
        minimal zip file containing the Disco standard library, and
        any user-specified required files and modules.

        :return: a :class:`disco.fileutils.DiscoZipFile`.
        """
        # First, add the disco standard library.
        from clx import __file__ as clxpath
        from disco import __file__ as discopath
        from disco.fileutils import DiscoZipFile
        jobzip = DiscoZipFile()
        jobzip.writepath(os.path.dirname(clxpath), exclude=('.pyc', '__pycache__'))
        jobzip.writepath(os.path.dirname(discopath), exclude=('.pyc', '__pycache__'))
        jobzip.writesource(job)
        jobzip.writesource(self)
        # Then, add any user-specified required files.
        from disco.util import iskv
        def get(key):
            return self.getitem(key, job, jobargs)
        if isinstance(get('required_files'), dict):
            for path, bytes in get('required_files').items():
                jobzip.writestr(path, bytes)
        else:
            for path in get('required_files'):
                jobzip.write(path, os.path.join('lib', os.path.basename(path)))
        if get('required_modules') is None:
            self['required_modules'] = self.get_modules(job, **jobargs)
        for mod in get('required_modules'):
            if iskv(mod):
                jobzip.writepath(mod[1])
        # Done with basic minimal zip.
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

    def output(self, task, label=None, **kwds):
        """
        :type  task: :class:`disco.task.Task`
        :param task: the task for which to create output.

        :type  label: int or None
        :param label: the label of the output partition to get.

        :type  kwds: dict
        :param kwds: additional keyword arguments for the :class:`Output`.

        :return: the previously opened :class:`Output` for *label*,
                 or if necessary, a newly opened one.
        """
        if label not in self.outputs:
            self.outputs[label] = Output(task.output(label=label), **kwds)
        return self.outputs[label]

    def start(self, task, job, **jobargs):
        from disco.sysutil import set_mem_limit
        set_mem_limit(job.settings['DISCO_WORKER_MAX_MEM'])
        task.makedirs()
        if self.getitem('profile', job, jobargs):
            from cProfile import runctx
            name = 'profile-{0}'.format(task.uid)
            path = task.path(name)
            runctx('self.run(task, job, **jobargs)', globals(), locals(), path)
            task.put(name, open(path, 'rb').read())
        else:
            self.run(task, job, **jobargs)
        self.end(task, job, **jobargs)

    def run(self, task, job, **jobargs):
        """
        Called to do the actual work of processing the
        :class:`disco.task.Task`.  This method runs in the Disco
        cluster, on a server that is executing one of the tasks in a
        job submitted by a client.
        """
        self.getitem(task.stage, job, jobargs)(task, job, **jobargs)

    def end(self, task, job, **jobargs):
        self.send_outputs()
        self.send('MSG', "Results sent to master")

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
            sys.stdin = NonBlockingInput(sys.stdin,
                                         timeout=3 * DISCO_WORKER_MAX_TIME)
            sys.stdout = sys.stderr = MessageWriter(cls)
            cls.send('WORKER', {'pid': os.getpid(), 'version': "1.1"})
            task = cls.get_task()
            job, jobargs = task.jobobjs
            job.worker.start(task, job, **jobargs)
            cls.send('DONE')
        except (DataError, EnvironmentError, MemoryError) as e:
            # check the number of open file descriptors (under proc), warn if close to max
            # http://stackoverflow.com/questions/899038/getting-the-highest-allocated-file-descriptor
            # also check for other known reasons for error, such as if disk is full
            cls.send('ERROR', traceback.format_exc())
            raise
        except Exception as e:
            cls.send('FATAL', force_utf8(traceback.format_exc()))
            raise

    @classmethod
    def send(cls, type, payload=''):
        from json import dumps, loads
        body = dumps(payload)
        cls.stderr.write('{0} {1} {2}\n'.format(type, len(body), body))
        cls.stderr.flush()
        spent, rtype = sys.stdin.t_read_until(' ')
        spent, rsize = sys.stdin.t_read_until(' ', spent=spent)
        spent, rbody = sys.stdin.t_read(int(rsize) + 1, spent=spent)
        if type == 'ERROR':
            raise ValueError(loads(rbody[:-1]))
        return loads(rbody[:-1])

    @classmethod
    def get_input(cls, id):
        done, inputs = cls.send('INPUT', ['include', [id]])
        _id, status, _label, replicas = inputs[0]

        if status == 'busy':
            raise Wait
        if status == 'failed':
            raise DataError("Can't handle broken input", id)
        return [(id, str(url)) for id, url in replicas]

    @classmethod
    def get_inputs(cls, done=False, exclude=[]):
        while done != "done":
            done, inputs = cls.send('INPUT', ['exclude', exclude])
            for id, _status, label, _replicas in inputs:
                if id not in exclude:
                    label = label if label == 'all' else int(label)
                    yield IDedInput((cls, id, label))
                    exclude.append(id)

    @classmethod
    def labelled_input_map(cls, task, inputs):
        from disco.util import ispartitioned, read_index
        from collections import defaultdict
        def update_label_map(lm, i):
            reps = [url for rid, url in i.replicas]
            if ispartitioned(reps):
                for l, url, size in read_index(reps[0]):
                    if i.label in ('all', l):
                        lm[l].append([url])
            else:
                lm[i.label].append(reps)
        label_map = defaultdict(list)
        for i in inputs:
            update_label_map(label_map, i)
        return label_map

    @classmethod
    def concat_input(cls, task, output_label, replicas):
        output = AtomicFile(task.output_path(output_label))
        BUFFER_SIZE = 1024*1024
        for reps in replicas:
            # Use only the first replica for now, since a set of one
            # is the most common case.
            # TODO: handle falling back to alternative replicas.
            inp = open_url(reps[0])
            buf = inp.read(BUFFER_SIZE)
            while (len(buf) > 0):
                output.write(buf)
                buf = inp.read(BUFFER_SIZE)
            inp.close()
        output.close()
        return output.path, output.size()

    @classmethod
    def get_task(cls):
        from disco.task import Task
        return Task(**dict((str(k), v) for k, v in cls.send('TASK').items()))

    def send_outputs(self):
        for output in self.outputs.values():
            output.close()
            self.send('OUTPUT', [output.label, output.path, output.size()])

class Params(object):
    """
    Classic parameter container for tasks.

    This object provides a way to contain custom parameters, or state,
    in your tasks.

    You can specify any number of ``key, value`` pairs to the
    :class:`Params`.  The pairs will be available to task functions
    through the *params* argument.  Each task receives its own copy of
    the initial params object.

    *key* must be a valid Python identifier.  *value* can be any Python
    object.
    """
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class IDedInput(tuple):
    @property
    def worker(self):
        return self[0]

    @property
    def id(self):
        return self[1]

    @property
    def label(self):
        return self[2]

    @property
    def replicas(self):
        return self.worker.get_input(self.id)

    @property
    def isindex(self):
        from disco.util import ispartitioned
        return ispartitioned(self.locations)

    @property
    def locations(self):
        return [r for rid, r in self.replicas]

    def unavailable(self, tried):
        return self.worker.send('INPUT_ERR', [self.id, list(tried)])

    def __str__(self):
        return '{0}'.format([url for rid, url in self.replicas])

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

    def __next__(self):
        return self.next()

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
            self.last, item = next(self.iter)
            return item
        except DataError:
            self.swap(traceback.format_exc())
            raise Wait(0)

    def __next__(self):
        return self.next()

    def swap(self, error=None):
        try:
            def skip(iter, N):
                from itertools import dropwhile
                return dropwhile(lambda n_rec: n_rec[0] < N, enumerate(iter))
            self.iter = skip(self.open(next(self.urls)), self.last + 1)
        except DataError:
            self.swap(traceback.format_exc())
        except StopIteration:
            if error:
                raise DataError("Exhausted all available replicas, "
                                "last error was:\n\n{0}".format(error), self.input)
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
            except Wait as w:
                time.sleep(w.retry_after)

    def input_iter(self, input):
        return InputIter(self.input, task=self.task, **self.kwds)

    @classmethod
    def default_opener(cls, task):
        from disco import schemes
        def open(url):
            return schemes.open(url, task=task)
        return open

class BaseOutput(object):
    def __init__(self, path_type_label):
        self.path, self.type, label = path_type_label
        self.label = 0 if label is None else int(label)

    def size(self):
        return os.path.getsize(self.path)

    def close(self):
        pass

class Output(BaseOutput):
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

    .. attribute:: label

        The label for the output (or None).

    .. attribute:: file

        The underlying output file handle.
    """
    def __init__(self, path_type_label, open=None):
        super(Output, self).__init__(path_type_label)
        self.open = open or DiscoOutput
        self.file = self.open(self.path)

    def close(self):
        self.file.close()

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
            except Wait as w:
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
        from heapq import merge
        iters = [self.input_iter(input) for input in self.input]
        heads = [Wait] * len(iters)
        return merge(*(self.couple(iters, heads, n) for n in range(len(iters))))

if __name__ == '__main__':
    Worker.main()
