
Disco FAQ
=========

.. contents::

Using Disco
-----------

I tried to install Disco but it doesn't work. Why? 
''''''''''''''''''''''''''''''''''''''''''''''''''

See :ref:`troubleshooting`. If the problem persist, contact
Disco developers `either on IRC or on the mailing list
<http://discoproject.org/getinvolved.html>`_.

Why not `Hadoop <http://hadoop.apache.org>`_?
'''''''''''''''''''''''''''''''''''''''''''''

Why `Vim <http://www.vim.org>`_ and not `Emacs
<http://www.gnu.org/software/emacs/>`_? Currently Hadoop
is probably faster, more scalable, and more featureful than
Disco. It has a great development community and it is used by
`major <http://www.yahoo.com>`_ `Internet <http://www.facebook.com>`_
`companies <http://www.amazon.com>`_. 

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

Thanks to Erlang, the Disco core remarkably compact, currently less
than 2000 lines of code. It is relatively easy to understand how
the core works, and start experimenting with it or adapt it to new
environments. Thanks to Python, it is easy to add new features around
the core which ensures that Disco can respond quickly to real-world needs.


How to debug / profile programs in Disco?
'''''''''''''''''''''''''''''''''''''''''

Use :mod:`homedisco`. It allows you to run Disco jobs locally, as any other
Python program. This means that you can use any Python debugger or profiler for
analyzing your code.

:mod:`homedisco` is not only great for debugging and profiling but
also for development in general. It is highly recommended that you test
your functions first locally with :mod:`homedisco`, before running them
in the normal distributed Disco environment.

Do I always have to provide a function for map and reduce?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Currently a map function is always required but the reduce function is
optional. This might change if someone comes up with a convincing case
that requires a *map-reduce-reduce* sequence that cannot be expressed
cleanly as a *map-reduce-map* or as a *map-map-reduce* sequence.


How many maps can I have? Does higher number of maps lead to better performance?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

In theory there is no restriction. In practice, the number is of course
limited by the available disk space (for input files) and the amount of
RAM that is required by the Disco master. Disco includes a test case,
in ``test/test50k.py`` that starts 50,000 map tasks in parallel. You
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
can't order many maps to process a single input file. In other words,
to run *K* maps in parallel you need at least *K* input files.

In general, the question about the expected speedup when increasing
parallelism is a rather complicated one and it depends heavily on the task
at hand. See `Amdahl's Law <http://en.wikipedia.org/wiki/Amdahl's_Law>`_
for more information about the subject. However, unless your tasks are
so light that the execution time is dominated by the overhead caused
by Disco, you can expect to gain some speedup by adding more maps until
the number of maps equals to the number of available CPUs.

How do I pass the output of one map-reduce phase to another?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Many algorithms can be implemented cleanly as a sequence of consequent
map-reduce jobs. Chaining jobs together is also efficient, as the job's
results are readily distributed and stored in the Disco's internal format.

Here's an example that runs ten jobs in a sequence, using outputs from
the previous job as the input for the next one. The job increments each
value in the input by one::

        from disco.core import Disco, result_iterator
        import sys

        def init_map(line, params):
                return [(int(line) + 1, "")]

        def iter_map(e, params):
                key, value = e
                return [(int(key) + 1, "")]
        
        disco = Disco("disco://localhost")
        results = disco.new_job(name = "inc_init",
                               input = sys.argv[2:],
                               map = init_map).wait()

        for i in range(9):
                results = disco.new_job(name =  "inc_%d" % i, 
                                        input = results,
                                        map = iter_map,
                                        map_reader = disco.chain_reader).wait()

        for key, value in result_iterator(results):
                print key

Assuming that the input files consists of zeroes, this example will
produce a sequence of tens as the result.

Note the following things in the example: You probably need two
separate map functions, like *init_map* and *iter_map* above. The
former handles the initial input from the original input files and the
latter map handles input from the previous map function. When using
:func:`disco.func.chain_reader` as the map reader, which reads results
of a previous job as the input, the input entry for the map function
is naturally a key-value pair whereas in the default case it is a line
of text.

Note that the job name includes a counter variable. This ensures that
each job name is unique, as required by Disco.


How to maintain state across many map / reduce calls?
'''''''''''''''''''''''''''''''''''''''''''''''''''''

Use the parameters object :class:`disco.core.Params` as the closure for
your functions. Here's an example::

        from disco.core import Disco, Params

        def fun_map(e, params):
                params.c += 1
                if not params.c % 10:
                        return [(e, "good")]
                else:
                        return [(e, "not good")]

        Disco("disco://localhost").new_job(
                      name = "params_test",
                      input = ["disco://localhost/myjob/file1"],
                      map = fun_map,
                      params = Params(c = 0))

In this case *params.c* is a counter variable that is incremented in
every call to the map function.

How to send log entries from my functions to the Web interface?
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Use the :func:`disco_worker.msg` function. Here's an example::

        from disco.core import Disco, Params

        def fun_map(e, params):
                params.c += 1
                if not c % 100000:
                        msg("Now processing %dth entry" % params.c)
                return [(e, 1)]

        Disco("disco://localhost").new_job(
                  name = "log_test",
                  input = ["disco://localhost/myjob/file1"],
                  map = fun_map,
                  params = Params(c = 0))

Note that you must not call :func:`disco_worker.msg` too often. If you send more
than 10 messages per second, Disco will kill your job.


My input files are stored in CSV / XML / XYZ format. What is the easiest to use them in Disco?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

If the format is textual, it may be possible to define a regular
expression that can be used to extract input entries from the files. See
:func:`disco.func.re_reader` for more information.



