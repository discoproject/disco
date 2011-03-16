#!/usr/bin/env python
"""
:mod:`disco.worker.classic` -- Classic Disco Runtime Environment
================================================================

Disco master runs :mod:`disco.worker.classic` to execute map and reduce functions
for a Disco job. The module contains several classes and functions that
are used by Disco internally to connect to input sources, write output
files etc. However, this page only documents features that may be useful
for writing new Disco jobs.

As job functions are imported to the :mod:`disco.worker.classic` namespace
for execution, they can use functions in this module directly without
importing the module explicitly.

Classic Worker Options
----------------------

    .. note:: All arguments that are required are marked as such.
              All other arguments are optional.

    :type  map: :func:`disco.worker.classic.func.map`
    :param map: a :term:`pure function` that defines the map task.

    :type  map_init: :func:`disco.worker.classic.func.init`
    :param map_init: initialization function for the map task.
                     This function is called once before the task starts.

    :type  map_input_stream: list of :func:`disco.worker.classic.func.input_stream`
    :param map_input_stream: The given functions are chained together and the final resulting
                             :class:`disco.worker.classic.func.InputStream` object is used
                             to iterate over input entries.

                             (*Added in version 0.2.4*)

    :type  map_output_stream: list of :func:`disco.worker.classic.func.output_stream`
    :param map_output_stream: The given functions are chained together and the
                              :meth:`disco.worker.classic.func.OutputStream.add` method of the last
                              returned :class:`disco.worker.classic.func.OutputStream` object is used
                              to serialize key, value pairs output by the map.
                              (*Added in version 0.2.4*)

    :type  map_reader: ``None`` or :func:`disco.worker.classic.func.input_stream`
    :param map_reader: Convenience function to define the last :func:`disco.worker.classic.func.input_stream`
                       function in the *map_input_stream* chain.

                       Disco worker provides a convenience function
                       :func:`disco.worker.classic.func.re_reader` that can be used to create
                       a reader using regular expressions.

                       If you want to use outputs of an earlier job as inputs,
                       use :func:`disco.worker.classic.func.chain_reader` as the *map_reader*.

                       Default is ``None``.

                       (*Changed after version 0.3.1*)
                       The default map_reader became ``None``.
                       See the note in :func:`disco.worker.classic.func.map_line_reader`
                       for information on how this might affect older jobs.

    :type  reduce: :func:`disco.worker.classic.func.reduce`
    :param reduce: If no reduce function is specified, the job will quit after
                   the map phase has finished.

                   *Added in version 0.3.1*:
                   Reduce supports now an alternative signature,
                   :func:`disco.worker.classic.func.reduce2` which uses an iterator instead
                   of ``out.add()`` to output results.

                   *Changed in version 0.2*:
                   It is possible to define only *reduce* without *map*.
                   For more information, see the FAQ entry :ref:`reduceonly`.

    :type  reduce_init: :func:`disco.worker.classic.func.init`
    :param reduce_init: initialization function for the reduce task.
                        This function is called once before the task starts.

    :type  reduce_input_stream: list of :func:`disco.worker.classic.func.output_stream`
    :param reduce_input_stream: The given functions are chained together and the last
                              returned :class:`disco.worker.classic.func.InputStream` object is
                              given to *reduce* as its first argument.
                              (*Added in version 0.2.4*)

    :type  reduce_output_stream: list of :func:`disco.worker.classic.func.output_stream`
    :param reduce_output_stream: The given functions are chained together and the last
                              returned :class:`disco.worker.classic.func.OutputStream` object is
                              given to *reduce* as its second argument.
                              (*Added in version 0.2.4*)

    :type  reduce_reader: :func:`disco.worker.classic.func.input_stream`
    :param reduce_reader: Convenience function to define the last func:`disco.worker.classic.func.input_stream`
                          if *map* is specified.
                          If *map* is not specified,
                          you can read arbitrary inputs with this function,
                          similar to *map_reader*.
                          (*Added in version 0.2*)

                          Default is :func:`disco.worker.classic.func.chain_reader`.

    :type  combiner: :func:`disco.worker.classic.func.combiner`
    :param combiner: called after the partitioning function, for each partition.

    :type  partition: :func:`disco.worker.classic.func.partition`
    :param partition: decides how the map output is distributed to reduce.

                      Default is :func:`disco.worker.classic.func.default_partition`.

    :type  partitions: int or None
    :param partitions: number of partitions, if any.

                       Default is ``1``.

    :type  merge_partitions: bool
    :param merge_partitions: whether or not to merge partitioned inputs during reduce.

                             Default is ``False``.

    :type  scheduler: dict
    :param scheduler: options for the job scheduler.
                      The following keys are supported:

                       * *max_cores* - use this many cores at most
                                       (applies to both map and reduce).

                                       Default is ``2**31``.

                       * *force_local* - always run task on the node where
                                         input data is located;
                                         never use HTTP to access data remotely.

                       * *force_remote* - never run task on the node where input
                                          data is located;
                                          always use HTTP to access data remotely.

                      (*Added in version 0.2.4*)

    :type  sort: boolean
    :param sort: flag specifying whether the intermediate results,
                 that is, input to the reduce function, should be sorted.
                 Sorting is most useful in ensuring that the equal keys are
                 consequent in the input for the reduce function.

                 Other than ensuring that equal keys are grouped together,
                 sorting ensures that keys are returned in the ascending order.
                 No other assumptions should be made on the comparison function.

                 The external program ``sort`` is used to sort the input on disk.
                 In-memory sort can easily be performed by the tasks themselves.

                 Default is ``False``.

    :type  sort_buffer_size: string
    :param sort_buffer_size: how much memory can be used by external sort.

                Passed as the '-S' option to Unix `sort` (see *man sort*).
                Default is ``10%`` i.e. 10% of the total available memory.

    :type  params: object
    :param params: object that is passed to worker tasks to store state
                   The object is serialized using the *pickle* module,
                   so it should be pickleable.

                   A convience class :class:`Params` is provided that
                   provides an easy way to encapsulate a set of parameters.
                   :class:`Params` allows including
                   :term:`pure functions <pure function>` in the parameters.

    :param ext_params: if either map or reduce function is an external program,
                       typically specified using :func:`disco.util.external`,
                       this object is used to deliver parameters to the program.

                       See :mod:`disco.worker.classic.external`.

    :type  required_files: list of paths or dict
    :param required_files: additional files that are required by the job.
                           Either a list of paths to files to include,
                           or a dictionary which contains items of the form
                           ``(filename, filecontents)``.

                           You can use this parameter to include custom modules
                           or shared libraries in the job.
                           (*Added in version 0.2.3*)

                           .. note::

                                All files will be saved in a flat directory
                                on the worker.
                                No subdirectories will be created.


                            .. note::

                                ``LD_LIBRARY_PATH`` is set so you can include
                                a shared library ``foo.so`` in *required_files*
                                and load it in the job directly as
                                ``ctypes.cdll.LoadLibrary("foo.so")``.
                                For an example, see :ref:`discoext`.

    :param required_modules: required modules to send to the worker
                             (*Changed in version 0.2.3*):
                             Disco tries to guess which modules are needed
                             by your job functions automatically.
                             It sends any local dependencies
                             (i.e. modules not included in the
                             Python standard library) to nodes by default.

                             If guessing fails, or you have other requirements,
                             see :mod:`disco.modutil` for options.


    :type  status_interval: integer
    :param status_interval: print "K items mapped / reduced"
                            for every Nth item.
                            Setting the value to 0 disables messages.

                            Increase this value, or set it to zero,
                            if you get "Message rate limit exceeded"
                            error due to system messages.
                            This might happen if your tasks are really fast.
                            Decrease the value if you want more messages or
                            you don't have that many data items.

                            Default is ``100000``.

    :type  profile: boolean
    :param profile: enable tasks profiling.
                    Retrieve profiling results with :meth:`Disco.profile_stats`.

                    Default is ``False``.

.. autoclass:: Params
        :members:


.. _oob:

Out-of-band results
-------------------
*(new in version 0.2)*

In addition to standard input and output streams, map and reduce tasks can
output results through an auxiliary channel called *out-of-band results* (OOB).
In contrast to the standard output stream, which is sequential, OOB results
can be accessed by unique keys.

Out-of-band results should not be used as a substitute for the normal output
stream. Each OOB key-value pair is saved to an individual file which waste
space when values are small and which are inefficient to random-access in bulk.
Due to these limitations, OOB results are mainly suitable, e.g for outputting
statistics and other metadata about the actual results.

To prevent rogue tasks from overwhelming nodes with a large number of OOB
results, each is allowed to output 1000 results (:func:`put` calls) at maximum.
Hitting this limit is often a sign that you should use the normal output stream
for you results instead.

You can not use OOB results as a communication channel between concurrent tasks.
Concurrent tasks need to be independent to preserve desirable fault-tolerance
and scheduling characteristics of the map/reduce paradigm. However, in the
reduce phase you can access OOB results produced in the preceding map phase.
Similarly you can access OOB results produced by other finished jobs, given
a job name.

You can retrieve OOB results outside tasks using the :meth:`disco.core.Disco.oob_list` and
:meth:`disco.core.Disco.oob_get` functions.

.. autofunction:: put
.. autofunction:: get

Utility functions
-----------------

.. autofunction:: this_partition
.. autofunction:: this_host
.. autofunction:: this_master
"""
import os, sys

from disco import events, util, worker
from disco.worker.classic import external
from disco.worker.classic.func import * # XXX: hack so func fns dont need to import

def status(message):
    return events.Status(message).send()

class Worker(worker.Worker):
    def defaults(self):
        defaults = super(Worker, self).defaults()
        defaults.update({'map_init': init,
                         'map_reader': None,
                         'map_input_stream': (map_input_stream, ),
                         'map_output_stream': (map_output_stream,
                                               disco_output_stream),
                         'combiner': None,
                         'partition': default_partition,
                         'reduce_init': init,
                         'reduce_reader': chain_reader,
                         'reduce_input_stream': (reduce_input_stream, ),
                         'reduce_output_stream': (reduce_output_stream,
                                                  disco_output_stream),
                         'ext_params': {},
                         'merge_partitions': False,
                         'params': None,
                         'sort': False,
                         'sort_buffer_size': '10%',
                         'status_interval': 100000,
                         'version': '.'.join(str(s) for s in sys.version_info[:2])})
        return defaults

    def jobdict(self, job, **jobargs):
        jobdict = super(Worker, self).jobdict(job, **jobargs)
        if self.getitem('merge_partitions', job, **jobargs):
            jobdict.update({'nr_reduces': 1})
        return jobdict

    def jobzip(self, job, **jobargs):
        from disco.modutil import find_modules
        def get(key):
            return self.getitem(key, job, **jobargs)
        if get('required_modules') is None:
            self['required_modules'] = find_modules([obj
                                                     for key in self
                                                     for obj in util.iterify(get(key))
                                                     if callable(obj)],
                                                    exclude=['Task'])
        jobzip = super(Worker, self).jobzip(job, **jobargs)
        for func in ('map', 'reduce'):
            if isinstance(self[func], dict):
                for path, bytes in self[func].iteritems():
                    jobzip.writebytes(os.path.join('ext.%s' % func, path), bytes)
        return jobzip

    def run(self, task):
        global Task
        Task = task
        assert self['version'] == '%s.%s' % sys.version_info[:2], "Python version mismatch"

        def open_hook(file, size, url):
            status("Input is %s" % (util.format_size(size)))
            return file
        self.open_hook = open_hook

        params = self['params']
        if isinstance(self[task.mode], dict):
            params = self['ext_params']
            self[task.mode] = external.prepare(params, task.mode)

        globals_ = globals().copy()
        for module in self['required_modules'] or ():
            name = module[0] if util.iskv(module) else module
            globals_[name.split('.')[-1]] = __import__(name, fromlist=[name])
        for obj in util.flatten(self.values()):
            util.globalize(obj, globals_)

        getattr(self, task.mode)(task, params)
        external.close()

    def map(self, task, params):
        if self['save'] and self['partitions'] and not self['reduce']:
            raise NotImplementedError("Storing partitioned outputs in DDFS is not yet supported")
        entries = self.status_iter(task.input(open=self.opener('map', 'in', params)),
                                   "%s entries mapped")
        bufs = {}
        self['map_init'](entries, params)
        def output(partition):
            return task.output(partition, open=self.opener('map', 'out', params)).file.fds[-1]
        for entry in entries:
            for key, val in self['map'](entry, params):
                part = None
                if self['partitions']:
                    part = str(self['partition'](key, self['partitions'], params))
                if self['combiner']:
                    if part not in bufs:
                        bufs[part] = {}
                    for record in self['combiner'](key, val, bufs[part], False, params) or ():
                        output(part).add(*record)
                else:
                    output(part).add(key, val)
        for part, buf in bufs.items():
            for record in self['combiner'](None, None, buf, True, params) or ():
                output(part).add(*record)

    def reduce(self, task, params):
        from disco.task import input, inputs, SerialInput
        from disco.util import inputlist, ispartitioned, shuffled
        # master should feed only the partitioned inputs to reduce (and shuffle them?)
        # should be able to just use task.input(parallel=True) for normal reduce
        inputs = [input(id) for id in inputs()]
        partition = None
        if ispartitioned(inputs) and not self['merge_partitions']:
            partition = str(task.taskid)
        ordered = self.sort(SerialInput(shuffled(inputlist(inputs, partition=partition)),
                                        open=self.opener('reduce', 'in', params)),
                            task)
        entries = self.status_iter(ordered, "%s entries reduced")
        output = task.output(None, open=self.opener('reduce', 'out', params)).file.fds[-1]
        self['reduce_init'](entries, params)
        if util.argcount(self['reduce']) < 3:
            for record in self['reduce'](entries, *(params, )):
                output.add(*record)
        else:
            self['reduce'](entries, output, params)

    def sort(self, input, task):
        if self['sort']:
            return disk_sort(input,
                             task.path('sort.dl'),
                             sort_buffer_size=self['sort_buffer_size'])
        return input

    def status_iter(self, iterator, message_template):
        status_interval = self['status_interval']
        n = -1
        for n, item in enumerate(iterator):
            if status_interval and (n + 1) % status_interval == 0:
                status(message_template % (n + 1))
            yield item
        status("Done: %s" % (message_template % (n + 1)))

    def opener(self, mode, direction, params):
        if direction == 'in':
            from itertools import chain
            streams = filter(None, chain(self['%s_input_stream' % mode],
                                         (self['%s_reader' % mode],
                                          self.open_hook)))
        else:
            streams = self['%s_output_stream' % mode]
        def open(url):
            return ClassicFile(url, streams, params)
        return open

    @staticmethod
    def open_hook(file, size, url):
        return file

class ClassicFile(object):
    def __init__(self, url, streams, params, fd=None, size=None):
        self.fds = []
        for stream in streams:
            maybe_params = (params,) if util.argcount(stream) == 4 else ()
            fd = stream(fd, size, url, *maybe_params)
            if isinstance(fd, tuple):
                if len(fd) == 3:
                    fd, size, url = fd
                else:
                    fd, url = fd
            self.fds.append(fd)

    def __iter__(self):
        return iter(self.fds[-1])

    def close(self):
        for fd in reversed(self.fds):
            if hasattr(fd, 'close'):
                fd.close()

class Params(object):
    """
    Parameter container for map / reduce tasks.

    This object provides a convenient way to contain custom parameters,
    or state, in your tasks.

    This example shows a simple way of using :class:`Params`::

        def fun_map(e, params):
                params.c += 1
                if not params.c % 10:
                        return [(params.f(e), params.c)]
                return [(e, params.c)]

        disco.new_job(name="disco://localhost",
                      input=["disco://localhost/myjob/file1"],
                      map=fun_map,
                      params=Params(c=0, f=lambda x: x + "!"))

    You can specify any number of key-value pairs to the :class:`Params`.
    The pairs will be available to task functions through the *params* argument.
    Each task receives its own copy of the initial params object.
    *key* must be a valid Python identifier.
    *value* can be any Python object.
    For instance, *value* can be an arbitrary :term:`pure function`,
    such as *params.f* in the previous example.
    """
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __getstate__(self):
        return dict((k, util.pack(v))
            for k, v in self.__dict__.iteritems()
                if not k.startswith('_'))

    def __setstate__(self, state):
        for k, v in state.iteritems():
            self.__dict__[k] = util.unpack(v)

from disco.util import msg, err, data_err

def get(*args, **kwargs):
    return Task.get(*args, **kwargs)

def put(*args, **kwargs):
    return Task.put(*args, **kwargs)

def this_name():
    return Task.jobname

def this_master():
    """Returns hostname and port of the disco master."""
    return Task.master

def this_host():
    """Returns hostname of the node that executes the task currently."""
    return Task.host

def this_partition():
    """
    For a map task, returns an integer between *[0..nr_maps]* that identifies
    the task. This value is mainly useful if you need to generate unique IDs
    in each map task. There are no guarantees about how ids are assigned
    for map tasks.

    For a reduce task, returns an integer between *[0..partitions]* that
    identifies this partition. You can use a custom partitioning function to
    assign key-value pairs to a particular partition.
    """
    return Task.taskid

if __name__ == '__main__':
    Worker.main()
