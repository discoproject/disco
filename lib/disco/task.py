"""
:mod:`disco.task` -- Disco Tasks
================================

This module defines objects for interfacing with
:term:`tasks <task>` assigned by the master.

"""
import os, time

from itertools import chain, dropwhile, repeat
from disco.events import Input, DataUnavailable, Output
from disco.error import DataError
from disco.fileutils import AtomicFile, ensure_path
from disco.util import hexhash, isiterable, schemesplit

def default_open(url):
    scheme, _url = schemesplit(url)
    scheme_ = 'scheme_%s' % (scheme or 'file')
    mod = __import__('disco.schemes.%s' % scheme_, fromlist=[scheme_])
    file, size, url = mod.input_stream(None, None, url, None)
    return file

def input(id):
    status, replicas = Input(id).send()
    if status == 'busy':
        raise Wait
    if status == 'failed':
        raise DataError("Can't handle broken input", id)
    return replicas

def inputs(done=False, exclude=()):
    while not done:
        done, inputs = Input().send()
        for id, status, urls in inputs:
            if id not in exclude:
                yield id
                exclude += (id, )

def output(task, partition=None, type='disco'):
    if partition is None:
        return task.path(task.uid), type, 'None'
    elif not isinstance(partition, basestring):
        raise ValueError("Partition label must be a string or None")
    return task.path('%s-%s' % (task.mode, partition)), 'part', partition

class TaskInput(object):
    """
    An iterable over one or more :class:`Task` inputs,
    which can gracefully handle corrupted replicas or otherwise failed inputs.

    :type  open: function
    :param open: a function with the following signature::

                        def open(url):
                            ...
                            return file

                used to open input files.
    """
    WAIT_TIMEOUT = 1

    def __init__(self, input, **kwds):
        self.input, self.kwds = input, kwds

    def __iter__(self):
        iter = InputIter(self.input, **self.kwds)
        while iter:
            try:
                for item in iter:
                    yield item
                iter = None
            except Wait:
                time.sleep(self.WAIT_TIMEOUT)

class TaskOutput(object):
    """
    A container for outputs from :class:`tasks <Task>`.

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
        self.open = open or AtomicFile
        self.file = self.open(self.path)

class Task(object):
    """
    Encapsulates the information specific to a particular
    :term:`task` coming from the master.

    Provides convenience functions to :class:`Workers <disco.worker.Worker>`,
    for opening inputs and outputs, and other common operations.

    .. attribute:: host

        The name of the host this task is running on.

    .. attribute:: jobname

        The name of the :term:`job` this task is part of.

    .. attribute:: master

        The name of the master host for this task.

    .. attribute:: mode

        The phase which this task is part of.
        Currently either :term:`map` or :term:`reduce`.

    .. attribute:: taskid

        The id of this task, assigned by the master.

    .. attribute:: uid

        A unique id for this particular task instance.
    """
    def __init__(self,
                 host='',
                 jobname='',
                 master=None,
                 mode=None,
                 taskid=-1):
        self.host = host
        self.jobname = jobname
        self.master = master
        self.mode = mode
        self.taskid = taskid
        self.outputs = {}
        self.uid = '%s:%s-%s-%x' % (mode,
                                    taskid,
                                    hexhash(str((time.time()))),
                                    os.getpid())

    @property
    def jobpath(self):
        return os.path.join(self.host, hexhash(self.jobname), self.jobname)

    @property
    def taskpath(self):
        return os.path.join(hexhash(self.uid), self.uid)

    def makedirs(self):
        ensure_path(self.taskpath)

    def path(self, name):
        """
        :return: The *name* joined to the :attr:`taskpath`.
        """
        return os.path.join(self.taskpath, name)

    def url(self, name, scheme='disco'):
        return '%s://%s/disco/%s/%s/%s' % (scheme, self.host, self.jobpath, self.taskpath, name)

    def input(self, merged=False, **kwds):
        """
        :type  merged: bool
        :param merged: if specified, returns a :class:`MergedInput`.

        :type  kwds: dict
        :param kwds: additional keyword arguments for the :class:`TaskInput`.

        :return: a :class:`TaskInput` to iterate over the inputs from the master.
        """
        if merged:
            return MergedInput(inputs(), **kwds)
        return SerialInput(inputs(), **kwds)

    def output(self, partition=None, **kwds):
        """
        :type  partition: string or None
        :param partition: the label of the output partition to get.

        :type  kwds: dict
        :param kwds: additional keyword arguments for the :class:`TaskOutput`.

        :return: the previously opened :class:`TaskOutput` for *partition*,
                 or if necessary, a newly opened one.
        """
        if partition not in self.outputs:
            self.outputs[partition] = TaskOutput(output(self, partition=partition), **kwds)
        return self.outputs[partition]

    def get(self, key):
        """
        Gets an out-of-band result for the task with the key *key*.

        Given the semantics of OOB results, this means that only the reduce
        phase can access results produced in the preceding map phase.
        """
        from disco.util import load_oob
        return load_oob(self.master, self.jobname, key)

    def put(self, key, value):
        """
        Stores an out-of-band result *value* with the key *key*.

        Key must be unique in this job.
        Maximum key length is 256 characters.
        Only characters in the set ``[a-zA-Z_\-:0-9@]`` are allowed in the key.
        """
        from disco.ddfs import DDFS
        from disco.util import save_oob
        if DDFS.safe_name(key) != key:
            raise DiscoError("OOB key contains invalid characters (%s)" % key)
        save_oob(self.master, self.jobname, key, value)

    def save(self):
        """
        Closes all the outputs, pushes them to :ref:`DDFS`,
        and informs the master of the tag.
        """
        from disco.ddfs import DDFS
        def paths():
            for output in self.outputs.values():
                output.file.close()
                yield output.path
        Output([DDFS(self.master).save(self.jobname, paths()), 'tag']).send()

    def send(self):
        """
        Closes all the outputs and informs the master of their type and location.
        """
        for output in self.outputs.values():
            output.file.close()
            Output([output.path, output.type, output.partition]).send()

    @classmethod
    def request(cls):
        """
        Request the task info from the master and return a :class:`Task` object.
        """
        from disco.events import TaskInfo
        return cls(**dict((str(k), v) for k, v in TaskInfo().send().iteritems()))

class Wait(Exception):
    """File objects can raise this if reading will block."""

class ReplicaIter(object):
    def __init__(self, id_or_urls):
        self.id, self.urls = None, None
        if isiterable(id_or_urls):
            self.urls = id_or_urls
        elif isinstance(id_or_urls, basestring):
            self.urls = id_or_urls,
        else:
            self.id = id_or_urls
        self.used = set()

    def __iter__(self):
        return self

    def next(self):
        urls = set(input(self.id) if self.id != None else self.urls) - self.used
        for url in urls:
            self.used.add(url)
            return url
        if self.id:
            DataUnavailable([self.id, list(self.used)]).send()
        raise StopIteration

class InputIter(object):
    def __init__(self, id_or_urls, open=None, start=0):
        self.urls = ReplicaIter(id_or_urls)
        self.last = start - 1
        self.open = open if open else default_open
        self.swap()

    def __iter__(self):
        return self

    def next(self):
        try:
            self.last, item = self.iter.next()
            return item
        except DataError:
            self.swap()

    def swap(self):
        try:
            def skip(iter, N):
                return dropwhile(lambda (n, rec): n < N, enumerate(iter))
            self.iter = skip(self.open(self.urls.next()), self.last + 1)
        except DataError:
            self.swap()
        except StopIteration:
            raise DataError("Exhausted all available replicas", list(self.urls.used))

class SerialInput(TaskInput):
    """
    Produces an iterator over the records in a list of sequential inputs.
    """
    def __init__(self, inputs, **kwds):
        self.inputs, self.kwds = inputs, kwds

    def __iter__(self):
        for input in self.inputs:
            for record in TaskInput(input, **self.kwds):
                yield record

class ParallelInput(TaskInput):
    """
    Produces an iterator over the unordered records in a set of inputs.

    Usually require the full set of inputs (i.e. will block with streaming).
    """
    def __init__(self, inputs, **kwds):
        self.inputs, self.kwds = inputs, kwds

    def __iter__(self):
        iters = [InputIter(input, **self.kwds) for input in self.inputs]
        while iters:
            iter = iters.pop()
            try:
                for item in iter:
                    yield item
            except Wait:
                if not iters:
                    time.sleep(self.WAIT_TIMEOUT)
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
                time.sleep(self.WAIT_TIMEOUT)
        return heads

class MergedInput(ParallelInput):
    """
    Produces an iterator over the minimal head elements of the inputs.
    """
    def __iter__(self):
        from disco.future import merge
        iters = [InputIter(input, **self.kwds) for input in self.inputs]
        heads = [Wait] * len(iters)
        return merge(*(self.couple(iters, heads, n) for n in xrange(len(iters))))

class ZippedInput(ParallelInput):
    """
    Produces an iterator over tuples of head elements from each of the inputs.
    """
    def __init__(self, inputs, fillvalue=None, **kwds):
        super(ZippedInput, self).__init__(inputs, **kwds)
        self.fillvalue = fillvalue

    def __iter__(self):
        def sentinel(counter=([self.fillvalue] * (len(self.inputs) - 1)).pop):
            yield counter()
        fillers = repeat(self.fillvalue)
        iters = [chain(InputIter(input, **self.kwds), sentinel(), fillers)
                 for input in self.inputs]

        try:
            while True:
                yield tuple(self.fill(iters, [Wait] * len(iters)))
        except IndexError:
            pass
