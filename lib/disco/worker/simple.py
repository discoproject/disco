#!/usr/bin/env python
"""
:mod:`disco.worker.simple` -- Simple Worker
===========================================

This module defines a :class:`disco.worker.Worker`,
which simply calls a method corresponding to the :attr:`disco.task.Task.stage`.
The method to call is determined using :meth:`disco.worker.Worker.getitem`.
"""
from disco import worker
from disco.job import JOBPACK_VERSION1

class Worker(worker.Worker):
    jobpack_version = JOBPACK_VERSION1

    def run(self, task, job, **jobargs):
        self.getitem(task.stage, job, jobargs)(self, task, **jobargs)

if __name__ == '__main__':
    Worker.main()
