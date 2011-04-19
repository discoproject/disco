#!/usr/bin/env python
"""
:mod:`disco.worker.simple` -- Simple Worker
===========================================

This module defines a :class:`disco.worker.Worker`,
which simply calls a method corresponding to the :attr:`disco.task.Task.mode`.
The method to call is determined using :meth:`disco.worker.Worker.getitem`.
"""
from disco import worker

class Worker(worker.Worker):
    def run(self, task, job, **jobargs):
        self.getitem(task.mode, job, jobargs)(self, task, **jobargs)

if __name__ == '__main__':
    Worker.main()
