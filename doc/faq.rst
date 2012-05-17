
Disco FAQ
=========

.. contents:: **Common Questions**
   :local:

I tried to install Disco but it doesn't work. Why?
''''''''''''''''''''''''''''''''''''''''''''''''''

See :ref:`troubleshooting`.
If the problem persists,
contact Disco developers :doc:`on IRC or the mailing list <start/getinvolved>`.

How come ``ssh localhost erl`` doesn't use my normal ``$PATH``?
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

::

        ssh localhost erl

is different from::

        ssh localhost
        erl

In general, interactive shells behave differently than non-interactive ones.
For example, see the `Bash Reference Manual`_.

.. _Bash Reference Manual: http://www.gnu.org/software/bash/manual/bashref.html#Interactive-Shells

.. _profiling:

How do I profile programs in Disco?
'''''''''''''''''''''''''''''''''''

Disco can use the
`Profile module <http://docs.python.org/library/profile.html>`_
to profile map and reduce tasks written in Python.
Enable profiling by setting ``profile = True`` in your :class:`disco.job.Job`.

Here's a simple example:

       .. literalinclude:: ../examples/faq/profile.py

.. seealso:: :meth:`disco.core.Disco.profile_stats`
   for accessing profiling results from Python.

.. _debugging:

How do I debug programs in Disco?
'''''''''''''''''''''''''''''''''

Set up a single node Disco cluster locally on your laptop or desktop. It makes
debugging a Disco job almost as easy as debugging any Python script.

.. _reduceonly:

Do I always have to provide a function for map and reduce?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

No, you may specify either :term:`map` or :term:`reduce` or both.
Many simple tasks can be solved with a single map function, without reduce.

It is somewhat less typical to specify only the reduce function.
This case arises when you want to merge results from independent map jobs,
or you want to join several input files without going through the map phase.

See also: :ref:`dataflow`

How many maps can I have? Does a higher number of maps lead to better performance?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

In theory there is no restriction. In practice, the number is of course
limited by the available disk space (for input files) and the amount of
RAM that is required by the Disco master. Disco includes a test case,
in ``tests/test_50k.py`` that starts 50,000 map tasks in parallel. You
should be able to add a few zeroes there without any trouble. If you
perform any stress tests of your own, let us know about your findings!

Each map and reduce instance is allocated exclusive access to a CPU. This
means that the number of parallel processes is limited by the number of
available CPUs. If you have 50,000 map instances but only 50 CPUs, only
50 maps are run in parallel while 49,550 instances are either waiting
in the job queue or marked as ready --- assuming that no other jobs are
running in the system at the same time and your input is split to at
least 50,000 separate files.

The number of maps can never exceed the number of input files as Disco
can't order many maps to process a single input file.
In other words, to run *K* maps in parallel you need at least *K* input files.
See :ref:`chunking` for more on splitting data stored in :ref:`DDFS`.

In general, the question about the expected speedup when increasing
parallelism is a rather complicated one and it depends heavily on the task
at hand. See `Amdahl's Law <http://en.wikipedia.org/wiki/Amdahl's_Law>`_
for more information about the subject. However, unless your tasks are
so light that the execution time is dominated by the overhead caused
by Disco, you can expect to gain some speedup by adding more maps until
the number of maps equals to the number of available CPUs.

I have one big data file, how do I run maps on it in parallel?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

See :ref:`chunking`.

.. _chaining:

How do I pass the output of one map-reduce phase to another?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Many algorithms can be implemented cleanly as a sequence of :term:`mapreduce`
:term:`jobs <job>`.
Chaining jobs together is also efficient, as the job's
results are readily distributed and stored in Disco's internal format.

Here's an example that runs ten jobs in a sequence, using outputs from
the previous job as the input for the next one.
The code can also be found in ``examples/faq/chain.py``.
The job increments each value in the input by one:

      .. literalinclude:: ../examples/faq/chain.py

Assuming that the input files consists of zeroes, this example will
produce a sequence of tens as the result.


How do I print messages to the Web interface from Python?
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Use a normal Python **print** statement.

.. note:: This is meant for simple debugging,
          if you print messages too often, Disco will throttle your worker.
          The master limits the rate of messages coming from workers,
          to prevent it from being overwhelmed.

Internally, Disco wraps everything written to ``sys.stdout``
with appropriate markup for the Erlang worker process,
which it communicates with via ``sys.stderr``.
See also :ref:`worker_protocol`.


My input files are stored in CSV / XML / XYZ format. What is the easiest to use them in Disco?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

See :func:`disco.worker.classic.func.input_stream`.

For `CSV <http://en.wikipedia.org/wiki/Comma-separated_values>`_ files
you can also have a look at
`the csv module <http://docs.python.org/library/csv.html>`_ shipped in
the Python standard library.

Why not `Hadoop <http://hadoop.apache.org>`_?
'''''''''''''''''''''''''''''''''''''''''''''

We see that platforms for distributed computing will be of such high
importance in the future that it is crucial to have a wide variety of
different approaches which produces healthy competition and co-evolution
between the projects. In this respect, Hadoop and Disco can be seen as
complementary projects, similar to `Apache <http://httpd.apache.org>`_,
`Lighttpd <http://lighttpd.net>`_ and `Nginx <http://nginx.net>`_.

It is a matter of taste whether Erlang and Python are more suitable for
the task than Java. We feel much more productive with Python than with
Java. We also feel that Erlang is a perfect match for the Disco core
that needs to handle tens of thousands of tasks in parallel.

Thanks to Erlang, the Disco core is remarkably compact.  It is
relatively easy to understand how the core works, and start
experimenting with it or adapt it to new environments. Thanks to
Python, it is easy to add new features around the core which ensures
that Disco can respond quickly to real-world needs.

.. _ec2:

How do I use Disco on Amazon EC2?
'''''''''''''''''''''''''''''''''

In general, you can use the EC2 cluster as any other Disco cluster.
However, if you want to access result files from your local machine,
you need to set the :envvar:`DISCO_PROXY` setting.
This configures the master node as a proxy,
since the computation nodes on EC2 are not directly accessible.

.. hint:: For instance, you could open an SSH tunnel to the master::

             ssh MASTER -L 8989:localhost:8989

          and set ``DISCO_PROXY=http://localhost:8989``.
