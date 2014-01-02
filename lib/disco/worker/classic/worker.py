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

import sys
# In Python3, __pycache__ directories and .pyc files are created if
# needed on module imports in the job directory.  When multiple tasks
# of the same job start executing in the same job directory, these
# tasks race in their creation.  The resulting race conditions result
# in random errors in module imports, which cause task and job
# failures.  It appears that Python3 (at least upto Python 3.2) does
# not correctly handle concurrent creation of __pycache__ and .pyc by
# independent processes.  So we turn off the writing of .pyc files for
# Python3.
if sys.version_info[0] == 3:
    sys.dont_write_bytecode = 1

import os
from disco import util, worker
from disco.worker.classic import external
from disco.worker.classic.func import * # XXX: hack so func fns dont need to import
from disco import JOBPACK_VERSION1
from disco.worker import Params

class Worker(worker.Worker):
    """
    A :class:`disco.worker.Worker`, which additionally supports the following parameters,
    to maintain the **Classic Disco Interface**:

    :type  map: :func:`disco.worker.classic.func.map`
    :param map: a function that defines the map task.

    :type  map_init: :func:`disco.worker.classic.func.init`
    :param map_init: initialization function for the map task.
                     This function is called once before the task starts.

                     .. deprecated:: 0.4
                                *map_init* has not been needed ever since
                                :func:`InputStreams <disco.worker.task_io.InputStream>`
                                were introduced.
                                Use *map_input_stream* and/or *map_reader* instead.

    :type  map_input_stream: sequence of :func:`disco.worker.task_io.input_stream`
    :param map_input_stream: The given functions are chained together and the final resulting
                             :class:`disco.worker.task_io.InputStream` object is used
                             to iterate over input entries.

                             .. versionadded:: 0.2.4

    :type  map_output_stream: sequence of :func:`disco.worker.task_io.output_stream`
    :param map_output_stream: The given functions are chained together and the
                              :meth:`disco.worker.task_io.OutputStream.add` method of the last
                              returned :class:`disco.worker.task_io.OutputStream` object is used
                              to serialize key, value pairs output by the map.

                              .. versionadded:: 0.2.4

    :type  map_reader: ``None`` or :func:`disco.worker.task_io.input_stream`
    :param map_reader: Convenience function to define the last :func:`disco.worker.task_io.input_stream`
                       function in the *map_input_stream* chain.

                       If you want to use outputs of an earlier job as inputs,
                       use :func:`disco.worker.task_io.chain_reader` as the *map_reader*.

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
                                :func:`InputStreams <disco.worker.task_io.InputStream>`
                                were introduced.
                                Use *reduce_input_stream* and/or *reduce_reader* instead.

    :type  reduce_input_stream: sequence of :func:`disco.worker.task_io.output_stream`
    :param reduce_input_stream: The given functions are chained together and the last
                              returned :class:`disco.worker.task_io.InputStream` object is
                              given to *reduce* as its first argument.

                              .. versionadded:: 0.2.4

    :type  reduce_output_stream: sequence of :func:`disco.worker.task_io.output_stream`
    :param reduce_output_stream: The given functions are chained together and the last
                              returned :class:`disco.worker.task_io.OutputStream` object is
                              given to *reduce* as its second argument.

                              .. versionadded:: 0.2.4

    :type  reduce_reader: :func:`disco.worker.task_io.input_stream`
    :param reduce_reader: Convenience function to define the last :func:`disco.worker.task_io.input_stream`
                          if *map* is specified.
                          If *map* is not specified,
                          you can read arbitrary inputs with this function,
                          similar to *map_reader*.

                          Default is :func:`disco.worker.task_io.chain_reader`.

                          .. versionadded:: 0.2

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

                   A convenience class :class:`Params` is provided that
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

    jobpack_version = JOBPACK_VERSION1

    def defaults(self):
        defaults = super(Worker, self).defaults()
        defaults.update({'map': None,
                         'map_init': init,
                         'map_reader': None,
                         'map_input_stream': (map_input_stream, ),
                         'map_output_stream': (map_output_stream,
                                               disco_output_stream),
                         'map_shuffle': None,
                         'combiner': None,
                         'merge_partitions': False, # XXX: maybe deprecated
                         'partition': default_partition,
                         'partitions': 1,
                         'reduce': None,
                         'reduce_init': init,
                         'reduce_reader': chain_reader,
                         'reduce_input_stream': (reduce_input_stream, ),
                         'reduce_output_stream': (reduce_output_stream,
                                                  disco_output_stream),
                         'reduce_shuffle': None,
                         'ext_params': {},
                         'params': Params(),
                         'shuffle': None,
                         'sort': False,
                         'sort_buffer_size': '10%',
                         'status_interval': 100000,
                         'version': '.'.join(str(s) for s in sys.version_info[:2])})
        return defaults

    def get_modules(self, job, **jobargs):
        from disco.worker.modutil import find_modules
        from disco.util import iterify
        def get(key):
            return self.getitem(key, job, jobargs)
        return find_modules([obj
                             for key in self
                             for obj in iterify(get(key))
                             if callable(obj)],
                            exclude=['Task'])

    def jobdict(self, job, **jobargs):
        """
        Creates :ref:`jobdict` for the :class:`Worker`.

        Makes use of the following parameters, in addition to those
        defined by the :class:`Worker` itself:

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

        :type  scheduler: dict
        :param scheduler: directly sets :attr:`jobdict.scheduler`.

                          .. deprecated:: 0.5
                                  *scheduler* params are now ignored.

        Uses :meth:`getitem` to resolve the values of parameters.

        :return: the :term:`job dict`.
        """
        from disco.util import isiterable, inputlist, ispartitioned, read_index
        from disco.error import DiscoError
        def get(key, default=None):
            return self.getitem(key, job, jobargs, default)
        has_map = bool(get('map'))
        has_reduce = bool(get('reduce'))
        job_input = get('input', [])
        has_save_results = get('save', False) or get('save_results', False)
        if not isiterable(job_input):
            raise DiscoError("Job 'input' is not a list of input locations,"
                             "or a list of such lists: {0}".format(job_input))
        input = inputlist(job_input,
                          label=None if has_map else False,
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
                                 for id, url, size in read_index(dir))
        else:
            # no map, without partitions can only have 1 reduce
            nr_reduces = 1

        if get('merge_partitions'):
            nr_reduces = 1

        jobdict = super(Worker, self).jobdict(job, **jobargs)
        jobdict.update({'input': input,
                        'worker': self.bin,
                        'map?': has_map,
                        'reduce?': has_reduce,
                        'nr_reduces': nr_reduces,
                        'save_results': has_save_results,
                        'scheduler': get('scheduler', {})})
        return jobdict

    def jobzip(self, job, **jobargs):
        jobzip = super(Worker, self).jobzip(job, **jobargs)
        def get(key):
            return self.getitem(key, job, jobargs)
        # Support for external interface.
        for func in ('map', 'reduce'):
            if isinstance(get(func), dict):
                for path, bytes in get(func).items():
                    jobzip.writestr(os.path.join('ext.{0}'.format(func), path), bytes)
        return jobzip

    def run(self, task, job, **jobargs):
        global Task
        Task = task
        for key in self:
            self[key] = self.getitem(key, job, jobargs)
        assert self['version'] == '{0[0]}.{0[1]}'.format(sys.version_info[:2]), "Python version mismatch"

        params = self['params']
        if isinstance(self[task.stage], dict):
            params = self['ext_params']
            self[task.stage] = external.prepare(params, task.stage)

        globals_ = globals().copy()
        for module in self['required_modules']:
            name = module[0] if util.iskv(module) else module
            globals_[name.split('.')[-1]] = __import__(name, fromlist=[name])
        for obj in util.flatten(self.values()):
            util.globalize(obj, globals_)

        getattr(self, task.stage)(task, params)
        external.close()

    def map(self, task, params):
        if self['save_results'] and self['partitions'] and not self['reduce']:
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
                    part = int(self['partition'](key, self['partitions'], params))
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

    def shuffle(self, task, params):
        inputs = [i for i in self.get_inputs()]
        getattr(self, 'shuffle_' + task.grouping)(task, params, inputs)

    map_shuffle = reduce_shuffle = shuffle

    def shuffle_group_node(self, task, params, inputs):
        from disco.worker import BaseOutput
        label_map = self.labelled_input_map(task, inputs)
        for label in label_map:
            if len(label_map[label]) > 0:
                outpath, _size = self.concat_input(task, label, label_map[label])
                self.outputs[label] = BaseOutput((outpath, 'disco', label))
        self.send('MSG', ("Shuffled {0} inputs into {1} label(s)"
                          .format(len(inputs), len(label_map))))

    def reduce_input(self, task, params):
        # master should feed only the partitioned inputs to reduce (and shuffle them?)
        from disco.worker import SerialInput
        from disco.util import inputlist, ispartitioned, shuffled
        inputs = [[url for rid, url in i.replicas] for i in self.get_inputs()]
        label = None
        if ispartitioned(inputs) and not self['merge_partitions']:
            label = task.group_label
        return self.sort(SerialInput(shuffled(inputlist(inputs, label=label)),
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
        from disco.util import disk_sort
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
        self.send('MSG', "Done: {0}".format(message_template % (n + 1)))

    def opener(self, mode, direction, params):
        if direction == 'in':
            from itertools import chain
            streams = [s for s in chain(self['{0}_input_stream'.format(mode)],
                                        [self['{0}_reader'.format(mode)]]) if s]
        else:
            streams = self['{0}_output_stream'.format(mode)]
        def open(url):
            return ClassicFile(url, streams, params)
        return open

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
    return Task.group_label

if __name__ == '__main__':
    Worker.main()
