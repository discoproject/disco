
:mod:`disco_worker` --- Runtime environment for Disco jobs
==========================================================

.. module:: disco_worker
   :synopsis: Runtime environment for Disco jobs
   
Disco master runs :mod:`disco_worker` to execute map and reduce functions
for a Disco job. The module contains several classes and functions that
are used by Disco internally to connect to input sources, write output
files etc. However, this page only documents features that may be useful
for writing new Disco jobs.

As job functions are imported to the :mod:`disco_worker` namespace
for execution, they can use functions in this module directly without
importing the module explicitely.

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

.. function:: put(key, value)

Stores an out-of-band result *value* with the key *key*. Key must be unique in
this job. Maximum key length is 256 characters. Only characters in the set
``[a-zA-Z_\-:0-9]`` are allowed in the key.

.. function:: get(key, [job])

Gets an out-of-band result assigned with the key *key*. The job name *job*
defaults to the current job.

Given the semantics of OOB results (see above), this means that the default
value is only good for the reduce phase which can access results produced
in the preceding map phase.


Utility functions
-----------------

.. function:: this_partition()

For a map task, returns an integer between *[0..nr_maps]* that identifies
the task. This value is mainly useful if you need to generate unique IDs
in each map task. There are no guarantees about how ids are assigned
for map tasks.

For a reduce task, returns an integer between *[0..nr_reduces]* that
identifies this partition. You can use a custom partitioning function to
assign key-value pairs to a particular partition.

.. function:: this_host()

Returns jostname of the node that executes the task currently.

.. function:: this_master()

Returns hostname and port of the disco master.

.. function:: this_inputs()

List of input files for this task.

.. function:: msg(message)

Sends the string *message* to the master for logging. The message is
shown on the web interface. To prevent a rogue job from overwhelming the
master, the maximum *message* size is set to 255 characters and job is
allowed to send at most 10 messages per second.

.. function:: err(message)

Raises an exception with the reason *message*. This terminates the job.

.. function:: data_err(message)

Raises a data error with the reason *message*. This signals the master to re-run
the task on another node. If the same task raises data error on several
different nodes, the master terminates the job. Thus data error should only be
raised if it is likely that the occurred error is temporary.

Typically this function is used by map readers to signal a temporary failure
in accessing an input file.


