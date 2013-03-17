#!/usr/bin/env python
"""
:mod:`disco.worker.pipeline.worker` -- Pipeline Disco Runtime Environment
===========================================================================

When a Job is constructed using the :class:`Worker` defined in this
module, Disco runs the :mod:`disco.worker.pipeline.worker` module for
every job task.  This module reconstructs the :class:`Worker` on the
node where it is run, in order to execute the :term:`job functions`
which were used to create it.

Workers resolve all parameters using :meth:`~disco.worker.Worker.getitem`.

Thus, users can subclass :class:`Job` as a convenient way to specify
fixed parameters.  For example, here's a simple distributed grep from
the Disco ``examples/`` directory:

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

import os, copy
from collections import namedtuple, defaultdict
from itertools import chain
from disco import util, worker
from disco.worker import task_io
from disco.job import JOBPACK_VERSION2

def input_hook(state, input_labels):
    return input_labels

class Stage(object):
    def __init__(self, name='', init=None, process=None, done=None,
                 input_hook=input_hook, input_chain=[], output_chain=[]):
        self.name = name
        self.init = init
        self.done = done
        self.process  = process
        self.taskinfo = None    # initialized just before stage execution
        self.input_hook   = input_hook
        self.input_chain  = input_chain
        self.output_chain = output_chain

    pipeline_input_chain = [task_io.task_input_stream]
    interior_input_chain = [task_io.task_input_stream, task_io.chain_reader]
    default_output_chain = [task_io.task_output_stream, task_io.disco_output_stream]

    @classmethod
    def default_input_chain(cls, pipe_index):
        return (cls.pipeline_input_chain if pipe_index == 0
                else cls.interior_input_chain)

    def _callables(self):
        return [c for c in chain([self.init, self.input_hook, self.process, self.done],
                                 self.input_chain,
                                 self.output_chain)
                if callable(c)]

TaskInfo  = namedtuple('TaskInfo', ['jobname', 'host', 'stage', 'group', 'label'])
DiscoTask = namedtuple('DiscoTask', ['output'])

class Worker(worker.Worker):
    """
    A :class:`disco.pipeline.Worker`, which implements the pipelined
    Disco job model.

    :type  pipeline: list of pairs
    :param pipeline: sequence of pairs of grouping operations and stage names
    """

    jobpack_version = JOBPACK_VERSION2
    group_ops = set(["split",
                     "group_label",
                     "group_node",
                     "group_all",
                     "group_node_label"])

    def defaults(self):
        defaults = super(Worker, self).defaults()
        defaults.update({'pipeline': [],
                         'version': '.'.join(str(s) for s in sys.version_info[:2])})
        return defaults

    def get_modules(self, job, **jobargs):
        from disco.worker.modutil import find_modules
        def get(key, default=None):
            return self.getitem(key, job, jobargs, default)
        return find_modules([obj
                             for g, s in get('pipeline', [])
                             for obj in s._callables()],
                            exclude=['Task'])

    def jobdict(self, job, **jobargs):
        """
        Creates :ref:`jobdict` for the :class:`Worker`.

        Makes use of the following parameters, in addition to those
        defined by the :class:`Worker` itself:

        Uses :meth:`getitem` to resolve the values of parameters.

        :return: the :term:`job dict`.
        """
        from disco.error import DiscoError
        def get(key, default=None):
            return self.getitem(key, job, jobargs, default)
        stages, pipeline = set(), []
        for g, s in get('pipeline', []):
            if g not in self.group_ops:
                raise DiscoError("Unknown grouping {0}".format(g))
            if s.name in stages:
                raise DiscoError("Repeated stage {0}".format(s.name))
            stages.add(s.name)
            pipeline.append((s.name, g))

        from disco.util import isiterable, inputlist
        job_input = get('input', [])
        if not isiterable(job_input):
            raise DiscoError("Job 'input' is not a list of input locations,"
                             "or a list of such lists: {0}".format(job_input))
        input = inputlist(job_input,
                          label=None,
                          settings=job.settings)
        pipe_input = [[0, 0, inp] for inp in input]
        jobdict = super(Worker, self).jobdict(job, **jobargs)
        jobdict.update({'worker': self.bin,
                        'pipeline': pipeline,
                        'inputs' : pipe_input})
        return jobdict

    def run(self, task, job, **jobargs):
        """
        Entry point into the executing pipeline worker task.  This
        initializes the task environment, sets up the current stage,
        and then executes it.
        """
        for key in self:
            self[key] = self.getitem(key, job, jobargs)
        sys_version = '{0[0]}.{0[1]}'.format(sys.version_info[:2])
        assert self['version'] == sys_version, "Python version mismatch"

        # Set up the task environment.
        globals_ = globals().copy()
        for module in self['required_modules']:
            name = module[0] if util.iskv(module) else module
            globals_[name.split('.')[-1]] = __import__(name, fromlist=[name])
        for obj in util.flatten(self.values()):
            util.globalize(obj, globals_)

        # Set up the stage.
        params = self.getitem('params', job, jobargs, worker.Params())
        pipeline = dict([(s.name, (idx, s))
                         for idx, (g, s) in enumerate(self['pipeline'])])
        pipe_idx, stage = pipeline[task.stage]
        stage.taskinfo = TaskInfo(jobname=task.jobname, host=task.host,
                                  stage=task.stage, group=task.group,
                                  label=task.group_label)
        if not stage.input_chain:
            stage.input_chain = Stage.default_input_chain(pipe_idx)
        if not stage.output_chain:
            stage.output_chain = Stage.default_output_chain
        self.run_stage(task, stage, params)

    def make_interface(self, task, stage, params):
        def output_open(url):
            return task_io.ClassicFile(url, stage.output_chain, params)
        def output(label):
            return self.output(task, label, open=output_open).file.fds[-1]
        return DiscoTask(output=output)

    def labelexpand(self, task, stage, input, params):
        def input_open(url):
            return task_io.ClassicFile(url, stage.input_chain, params)
        def make_input(inp):
            return worker.Input(inp, task=task, open=input_open)
        if input.isindex:
            for l, url, sz in sorted(util.read_index(input.locations[0])):
                if input.label in ('all', l):
                    yield l, make_input(url)
        else:
            yield input.label, make_input(input)

    def prepare_input_map(self, task, stage, params):
        map = defaultdict(list)
        for l, i in util.chainify(self.labelexpand(task, stage, i, params)
                                  for i in self.get_inputs()):
            map[l].append(i)
        return map

    def run_stage(self, task, stage, params):
        interface = self.make_interface(task, stage, params)
        state = stage.init(interface, params) if callable(stage.init) else None
        if callable(stage.process):
            input_map = self.prepare_input_map(task, stage, params)
            for label in stage.input_hook(state, input_map.keys()):
                for inp in input_map[label]:
                    stage.process(interface, state, label, inp)
        if callable(stage.done):
            stage.done(interface, state)

if __name__ == '__main__':
    Worker.main()
