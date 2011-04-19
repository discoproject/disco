.. _oob:

Out-of-band results
-------------------

.. versionadded:: 0.2

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
results, each is allowed to output 1000 results at maximum.
Hitting this limit is often a sign that you should use the normal output stream
for you results instead.

You can not use OOB results as a communication channel between concurrent tasks.
Concurrent tasks need to be independent to preserve desirable fault-tolerance
and scheduling characteristics of the map/reduce paradigm. However, in the
reduce phase you can access OOB results produced in the preceding map phase.
Similarly you can access OOB results produced by other finished jobs, given
a job name.

You can retrieve OOB results outside tasks using the
:meth:`disco.core.Disco.oob_list` and
:meth:`disco.core.Disco.oob_get` functions.
