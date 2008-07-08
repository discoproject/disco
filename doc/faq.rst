
Disco FAQ
=========

.. contents::

Using Disco
-----------

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

        import disco, sys

        def init_map(line, params):
                return [(int(line) + 1, "")]

        def iter_map(e, params):
                key, value = e
                return [(int(key) + 1, "")]
        
        results = disco.job("disco://localhost:5000", "inc_init", sys.argv[2:], init_map)

        for i in range(9):
                results = disco.job("disco://localhost:5000", "inc_%d" % i, results, iter_map,
                                    map_reader = disco.chain_reader)

        for key, value in disco.result_iterator(results):
                print key

Assuming that the input files consists of zeroes, this example will
produce a sequence of tens as the result.

Note the following things in the example: You probably need two
separate map functions, like *init_map* and *iter_map* above. The
former handles the initial input from the original input files and the
latter map handles input from the previous map function. When using
:func:`disco.chain_reader` as the map reader, which reads results of
a previous job as the input, the input entry for the map function is
naturally a key-value pair whereas in the default case it is a line
of text.

Note that the job name includes a counter variable. This ensures that
each job name is unique, as required by Disco.


How to maintain state across many map / reduce calls?
'''''''''''''''''''''''''''''''''''''''''''''''''''''

Use the parameters object :class:`disco.Params` as the closure for your
functions. Here's an example::

        def fun_map(e, params):
                params.c += 1
                if not params.c % 10:
                        return [(e, "good")]
                else:
                        return [(e, "not good")]

        disco.job("disco://localhost:5000", 
                      ["disco://localhost/myjob/file1"],
                      fun_map,
                      params = disco.Params(c = 0))

In this case *params.c* is a counter variable that is incremented in
every call to the map function.

How to send log entries from my functions to the Web interface?
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Use the :func:`disco_worker.msg` function. Here's an example::

        def fun_map(e, params):
                params.c += 1
                if not c % 100000:
                        msg("Now processing %dth entry" % params.c)
                return [(e, 1)]

        disco.job("disco://localhost:5000", 
                  ["disco://localhost/myjob/file1"],
                  fun_map,
                  params = disco.Params(c = 0))

Note that you must not call :func:`disco_worker.msg` too often. If you send more
than 10 messages per second, Disco will kill your job.

Can I query / clean / kill Disco jobs in a shell script?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Yes. Use the command line interface provided by the :mod:`discoapi` module.

My input files are stored in CSV / XML / XYZ format. What is the easiest to use them in Disco?
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

If the format is textual, it may be possible to define a regular expression that
can be used to extract input entries from the files. See
:func:`disco_worker.re_reader` for more information. 



