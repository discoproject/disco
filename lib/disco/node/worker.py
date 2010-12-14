"""
:mod:`disco.node.worker` -- Runtime environment for Disco jobs
==============================================================

Disco master runs :mod:`disco.node.worker` to execute map and reduce functions
for a Disco job. The module contains several classes and functions that
are used by Disco internally to connect to input sources, write output
files etc. However, this page only documents features that may be useful
for writing new Disco jobs.

As job functions are imported to the :mod:`disco.node.worker` namespace
for execution, they can use functions in this module directly without
importing the module explicitly.

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
import os

from disco.events import AnnouncePID, Status, WorkerDone
from disco.func import * # XXX: hack so disco.func fns dont need to import

class Worker(object):
    def __init__(self, mode, **taskargs):
        from disco import task
        AnnouncePID(os.getpid())
        Status("Received a new %s task!" % mode)
        self.task = getattr(task, mode.capitalize())(**taskargs)
        self.task.run()
        WorkerDone("Worker done")

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
    return Task.id

def this_inputs():
    """List of input files for this task."""
    return Task.inputs
