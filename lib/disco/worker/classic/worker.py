#!/usr/bin/env python
"""
:mod:`disco.worker.classic.worker` -- Classic Disco Runtime Environment
=======================================================================

When a Job is constructed using the classic :class:`Worker` defined in this module,
Disco runs the :mod:`disco.worker.classic.worker` module for every job task.
This module reconstructs the :class:`Worker` on the node where it is run,
in order to execute the :term:`job functions` which were used to create it.

Classic Workers resolve all parameters using :meth:`~disco.worker.Worker.getitem`.

Thus, users can subclass :class:`Job` as a convenient way to specify fixed parameters.
For example, here's a simple distributed grep from the Disco ``examples/`` directory:

    .. literalinclude:: ../../../examples/util/grep.py

"""
import os, sys

from disco import util, worker
from disco.worker.classic import external
from disco.worker.classic.func import * # XXX: hack so func fns dont need to import

class Worker(worker.Worker):
    """
    A :class:`disco.worker.Worker`, which additionally supports the following parameters,
    to maintain the **Classic Disco Interface**:

        .. note:: The classic worker tries to guess which modules are needed automatically,
                  for all of the :term:`job functions` specified below,
                  if the *required_modules* parameter is not specified.
                  It sends any local dependencies
                  (i.e. modules not included in the Python standard library)
                  to nodes by default.

                  If guessing fails, or you have other requirements,
                  see :mod:`disco.worker.classic.modutil` for options.

    :type  map: :func:`disco.worker.classic.func.map`
    :param map: a function that defines the map task.

    :type  map_init: :func:`disco.worker.classic.func.init`
    :param map_init: initialization function for the map task.
                     This function is called once before the task starts.

                     .. deprecated:: 0.4
                                *map_init* has not been needed ever since
                                :func:`InputStreams <disco.worker.classic.func.InputStream>`
                                were introduced.
                                Use *map_input_stream* and/or *map_reader* instead.

    :type  map_input_stream: sequence of :func:`disco.worker.classic.func.input_stream`
    :param map_input_stream: The given functions are chained together and the final resulting
                             :class:`disco.worker.classic.func.InputStream` object is used
                             to iterate over input entries.

                             .. versionadded:: 0.2.4

    :type  map_output_stream: sequence of :func:`disco.worker.classic.func.output_stream`
    :param map_output_stream: The given functions are chained together and the
                              :meth:`disco.worker.classic.func.OutputStream.add` method of the last
                              returned :class:`disco.worker.classic.func.OutputStream` object is used
                              to serialize key, value pairs output by the map.

                              .. versionadded:: 0.2.4

    :type  map_reader: ``None`` or :func:`disco.worker.classic.func.input_stream`
    :param map_reader: Convenience function to define the last :func:`disco.worker.classic.func.input_stream`
                       function in the *map_input_stream* chain.

                       If you want to use outputs of an earlier job as inputs,
                       use :func:`disco.worker.classic.func.chain_reader` as the *map_reader*.

                       .. versionchanged:: 0.3.1
                          The default is ``None``.

    :type  combiner: :func:`disco.worker.classic.func.combiner`
    :param combiner: called after the partitioning function, for each partition.

    :type  reduce: :func:`disco.worker.classic.func.reduce`
    :param reduce: If no reduce function is specified, the job will quit after
                   the map phase has finished.

                   .. versionadded:: 0.3.1
                      Reduce now supports an alternative signature,
                      :func:`disco.worker.classic.func.reduce2`,
                      which uses an iterator instead
                      of ``out.add()`` to output results.

                   .. versionchanged:: 0.2
                      It is possible to define only *reduce* without *map*.
                      See also :ref:`reduceonly`.

    :type  reduce_init: :func:`disco.worker.classic.func.init`
    :param reduce_init: initialization function for the reduce task.
                        This function is called once before the task starts.

                     .. deprecated:: 0.4
                                *reduce_init* has not been needed ever since
                                :func:`InputStreams <disco.worker.classic.func.InputStream>`
                                were introduced.
                                Use *reduce_input_stream* and/or *reduce_reader* instead.

    :type  reduce_input_stream: sequence of :func:`disco.worker.classic.func.output_stream`
    :param reduce_input_stream: The given functions are chained together and the last
                              returned :class:`disco.worker.classic.func.InputStream` object is
                              given to *reduce* as its first argument.

                              .. versionadded:: 0.2.4

    :type  reduce_output_stream: sequence of :func:`disco.worker.classic.func.output_stream`
    :param reduce_output_stream: The given functions are chained together and the last
                              returned :class:`disco.worker.classic.func.OutputStream` object is
                              given to *reduce* as its second argument.

                              .. versionadded:: 0.2.4

    :type  reduce_reader: :func:`disco.worker.classic.func.input_stream`
    :param reduce_reader: Convenience function to define the last :func:`disco.worker.classic.func.input_stream`
                          if *map* is specified.
                          If *map* is not specified,
                          you can read arbitrary inputs with this function,
                          similar to *map_reader*.

                          Default is :func:`disco.worker.classic.func.chain_reader`.

                          .. versionadded:: 0.2

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

    :type  merge_partitions: bool
    :param merge_partitions: whether or not to merge partitioned inputs during reduce.

                             Default is ``False``.

    :type  partition: :func:`disco.worker.classic.func.partition`
    :param partition: decides how the map output is distributed to reduce.

                      Default is :func:`disco.worker.classic.func.default_partition`.

    :type  partitions: int or None
    :param partitions: number of partitions, if any.

                       Default is ``1``.

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
                   :class:`Params` allows including functions  in the parameters.

    :param ext_params: if either map or reduce function is an external program,
                       typically specified using :func:`disco.util.external`,
                       this object is used to deliver parameters to the program.

                       See :mod:`disco.worker.classic.external`.


    :type  status_interval: int
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
    """
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
                         'required_files': {},
                         'required_modules': None,
                         'ext_params': {},
                         'params': Params(),
                         'sort': False,
                         'sort_buffer_size': '10%',
                         'status_interval': 100000,
                         'version': '.'.join(str(s) for s in sys.version_info[:2])})
        return defaults

    def jobenvs(self, job, **jobargs):
        envs = super(Worker, self).jobenvs(job, **jobargs)
        envs['LD_LIBRARY_PATH'] = 'lib'
        envs['PYTHONPATH'] = ':'.join(('lib', envs.get('PYTHONPATH', '')))
        return envs

    def jobzip(self, job, **jobargs):
        from disco.util import iskv
        from disco.worker.classic.modutil import find_modules
        jobzip = super(Worker, self).jobzip(job, **jobargs)
        def get(key):
            return self.getitem(key, job, jobargs)
        if isinstance(get('required_files'), dict):
            for path, bytes in get('required_files').iteritems():
                    jobzip.writestr(path, bytes)
        else:
            for path in get('required_files'):
                jobzip.write(path, os.path.join('lib', os.path.basename(path)))
        if get('required_modules') is None:
            self['required_modules'] = find_modules([obj
                                                     for key in self
                                                     for obj in util.iterify(get(key))
                                                     if callable(obj)],
                                                    exclude=['Task'])
        for mod in get('required_modules'):
            if iskv(mod):
                jobzip.writepath(mod[1])
        for func in ('map', 'reduce'):
            if isinstance(get(func), dict):
                for path, bytes in get(func).iteritems():
                    jobzip.writestr(os.path.join('ext.%s' % func, path), bytes)
        return jobzip

    def run(self, task, job, **jobargs):
        global Task
        Task = task
        for key in self:
            self[key] = self.getitem(key, job, jobargs)
        assert self['version'] == '%s.%s' % sys.version_info[:2], "Python version mismatch"

        params = self['params']
        if isinstance(self[task.mode], dict):
            params = self['ext_params']
            self[task.mode] = external.prepare(params, task.mode)

        globals_ = globals().copy()
        for module in self['required_modules']:
            name = module[0] if util.iskv(module) else module
            globals_[name.split('.')[-1]] = __import__(name, fromlist=[name])
        for obj in util.flatten(self.values()):
            util.globalize(obj, globals_)

        getattr(self, task.mode)(task, params)
        external.close()

    def map(self, task, params):
        if self['save'] and self['partitions'] and not self['reduce']:
            raise NotImplementedError("Storing partitioned outputs in DDFS is not yet supported")
        entries = self.status_iter(self.input(task, open=self.opener('map', 'in', params)),
                                   "%s entries mapped")
        bufs = {}
        self['map_init'](entries, params)
        def output(partition):
            return self.output(task, partition, open=self.opener('map', 'out', params)).file.fds[-1]
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

    def reduce_input(self, task, params):
        # master should feed only the partitioned inputs to reduce (and shuffle them?)
        from disco.worker import SerialInput
        from disco.util import inputlist, ispartitioned, shuffled
        inputs = [[url for rid, url in i.replicas] for i in self.get_inputs()]
        partition = None
        if ispartitioned(inputs) and not self['merge_partitions']:
            partition = str(task.taskid)
        return self.sort(SerialInput(shuffled(inputlist(inputs, partition=partition)),
                                     task=task,
                                     open=self.opener('reduce', 'in', params)),
                         task)

    def reduce(self, task, params):
        ordered = self.reduce_input(task, params)
        entries = self.status_iter(ordered, "%s entries reduced")
        output = self.output(task, None, open=self.opener('reduce', 'out', params)).file.fds[-1]
        self['reduce_init'](entries, params)
        if util.argcount(self['reduce']) < 3:
            for record in self['reduce'](entries, *(params, )):
                output.add(*record)
        else:
            self['reduce'](entries, output, params)

    def sort(self, input, task):
        if self['sort']:
            return disk_sort(self,
                             input,
                             task.path('sort.dl'),
                             sort_buffer_size=self['sort_buffer_size'])
        return input

    def status_iter(self, iterator, message_template):
        status_interval = self['status_interval']
        n = -1
        for n, item in enumerate(iterator):
            if status_interval and (n + 1) % status_interval == 0:
                self.send('MSG', message_template % (n + 1))
            yield item
        self.send('MSG', "Done: %s" % (message_template % (n + 1)))

    def opener(self, mode, direction, params):
        if direction == 'in':
            from itertools import chain
            streams = filter(None, chain(self['%s_input_stream' % mode],
                                         [self['%s_reader' % mode]]))
        else:
            streams = self['%s_output_stream' % mode]
        def open(url):
            return ClassicFile(url, streams, params)
        return open

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
    .. deprecated:: 0.4
              :class:`Params` objects aren't generally needed,
              since entire modules are sent with the :class:`Worker`,
              state can be stored as in normal Python.

    Classic parameter container for map / reduce tasks.

    This object provides a way to contain custom parameters,
    or state, in your tasks.

    You can specify any number of ``key, value`` pairs to the :class:`Params`.
    The pairs will be available to task functions through the *params* argument.
    Each task receives its own copy of the initial params object.
    *key* must be a valid Python identifier.
    *value* can be any Python object.
    """
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

from disco.util import msg, err, data_err

def get(*args, **kwargs):
    """See :meth:`disco.task.Task.get`."""
    return Task.get(*args, **kwargs)

def put(*args, **kwargs):
    """See :meth:`disco.task.Task.put`."""
    return Task.put(*args, **kwargs)

def this_inputs():
    """Returns the inputs for the :term:`worker`."""
    return [[url for rid, url in i.replicas] for i in Worker.get_inputs()]

def this_name():
    """Returns the jobname for the current task."""
    return Task.jobname

def this_master():
    """Returns hostname and port of the disco master."""
    return Task.master

def this_host():
    """Returns hostname of the node that executes the current task."""
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
