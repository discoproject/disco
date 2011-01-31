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
.. autofunction:: this_inputs
"""
import os, sys, traceback

from disco.error import DataError, DiscoError
from disco.events import DataUnavailable, TaskFailed
from disco.events import AnnouncePID, Input, Status, WorkerDone, TaskInfo, JobFile
from disco.fileutils import DiscoZipFile
from disco.job import JobDict
from disco.modutil import find_modules
from disco.sysutil import set_mem_limit
from disco.util import (MessageWriter,
                        pack, unpack,
                        read_index, inputlist, ispartitioned,
                        flatten, isiterable, iskv, iterify)

from disco import __file__ as discopath
from disco.worker.classic.func import * # XXX: hack so func fns dont need to import

class Worker(dict):
    def __init__(self, **kwargs):
        super(Worker, self).__init__(self.defaults())
        self.update(kwargs)

    def defaults(self):
        return {'map': None,
                'map_init': init,
                'map_reader': None,
                'map_input_stream': (map_input_stream, ),
                'map_output_stream': (map_output_stream,
                                      disco_output_stream),
                'combiner': None,
                'partition': default_partition,
                'reduce': None,
                'reduce_init': init,
                'reduce_reader': chain_reader,
                'reduce_input_stream': (reduce_input_stream, ),
                'reduce_output_stream': (reduce_output_stream,
                                         disco_output_stream),
                'notifier': notifier,
                'ext_params': {},
                'merge_partitions': False,
                'params': None,
                'partitions': 1,
                'profile': False,
                'required_files': {},
                'required_modules': None,
                'save': False,
                'sort': False,
                'sort_buffer_size': '10%',
                'status_interval': 100000,
                'version': '.'.join(str(s) for s in sys.version_info[:2])}

    def dumps(self):
        from disco.dencode import dumps
        return dumps(dict((k, pack(v)) for k, v in self.iteritems()))

    @classmethod
    def load(cls, jobfile):
        from disco.dencode import load
        worker = dict((k, unpack(v) if k == 'required_modules' else v)
                           for k, v in load(jobfile).iteritems())
        globals_ = globals().copy()
        for req in worker['required_modules'] or ():
            name = req[0] if iskv(req) else req
            globals_[name.split('.')[-1]] = __import__(name, fromlist=[name])
        return cls(**dict((k, v if k == 'required_modules'
                           else unpack(v, globals=globals_))
                          for k, v in worker.iteritems()))

    def jobpack(self, jobdict):
        jobdict = jobdict.copy()
        has_map = bool(self['map'])
        has_reduce = bool(self['reduce'])
        has_partitions = bool(self['partitions'])

        input = inputlist(*jobdict['input'],
                          partition=None if has_map else False,
                          settings=jobdict.settings)

        # -- required modules and files --
        jobhome = DiscoZipFile()
        jobhome.writepy(os.path.dirname(discopath), 'lib')
        if isinstance(self['required_files'], dict):
            for path, bytes in self['required_files'].iteritems():
                    jobhome.writebytes(path, bytes)
        else:
            for path in self['required_files']:
                jobhome.writepath(path)
        if self['required_modules'] is None:
            self['required_modules'] = find_modules([obj
                                                     for val in self.values()
                                                     for obj in iterify(val)
                                                     if callable(obj)])
        for mod in self['required_modules']:
            jobhome.writemodule((mod[0] if iskv(mod) else mod), 'lib')
        for func in ('map', 'reduce'):
            if isinstance(self[func], dict):
                for path, bytes in self[func].iteritems():
                    jobhome.writebytes(os.path.join('ext.%s' % func, path), bytes)
        jobhome.close()

        # -- nr_reduces --
        # ignored if there is not actually a reduce specified
        if has_map:
            # partitioned map has N reduces; non-partitioned map has 1 reduce
            nr_reduces = self['partitions'] or 1
        elif ispartitioned(input):
            # no map, with partitions: len(dir://) specifies nr_reduces
            has_partitions = True
            nr_reduces = 1 + max(int(id)
                                 for dir in input
                                 for id, url in read_index(dir))
        else:
            # no map, without partitions can only have 1 reduce
            has_partitions = False
            nr_reduces = 1

        # merge_partitions iff the inputs to reduce are partitioned
        if self['merge_partitions']:
            if has_partitions:
                nr_reduces = 1
            else:
                raise DiscoError("Can't merge partitions without partitions")

        jobdict.update({'input': input,
                        'jobhome': jobhome.dumps(),
                        'worker': 'lib/disco/worker/classic/worker.py',
                        'map?': has_map,
                        'reduce?': has_reduce,
                        'profile?': self['profile'],
                        'nr_reduces': nr_reduces})
        return jobdict.dumps() + self.dumps()

    @classmethod
    def unpack(cls, jobfile):
        sys.path.insert(0, 'lib')
        return JobDict.load(jobfile), cls.load(jobfile)

    @classmethod
    def work(cls):
        global Task
        from disco.worker.classic.task import task
        AnnouncePID(str(os.getpid())).send()
        jobdict, worker = cls.unpack(open(JobFile().send()))
        set_mem_limit(jobdict.settings['DISCO_WORKER_MAX_MEM'])
        Task = task(worker, jobdict, **TaskInfo().send())
        Status("Starting a new task").send()
        Task.start(*Input().send())
        WorkerDone("Worker done").send()

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
        return dict((k, pack(v))
            for k, v in self.__dict__.iteritems()
                if not k.startswith('_'))

    def __setstate__(self, state):
        for k, v in state.iteritems():
            self.__dict__[k] = unpack(v)

# XXX: deprecate these functions and fully remove global Task
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

def this_inputs():
    """List of input files for this task."""
    return Task.inputs

if __name__ == '__main__':
    try:
        sys.stdout = MessageWriter()
        Worker.work()
    except (DataError, EnvironmentError, MemoryError), e:
        # check the number of open file descriptors (under proc), warn if close to max
        # http://stackoverflow.com/questions/899038/getting-the-highest-allocated-file-descriptor
        # also check for other known reasons for error, such as if disk is full
        DataUnavailable(traceback.format_exc()).send()
        raise
    except Exception, e:
        TaskFailed(MessageWriter.force_utf8(traceback.format_exc())).send()
        raise
