
:mod:`homedisco` --- Test and debug Disco jobs locally
======================================================

.. module:: homedisco
   :synopsis: Test and debug Disco jobs locally

A high-performance computing cluster is a great environment for executing
data-intensive programs. However, it is not a pleasant development
environment. Especially during the early phases of development of new
:term:`job functions`, when even the most trivial syntax errors might
be lurking in the code, length of a single edit-run-debug cycle may feel
unnecessarily long when the code is run through the cluster. Also, when
an existing job appears to be slow or faulty, one could benefit from a
good profiler or debugger.

:mod:`homedisco` makes development of Disco jobs as easy
as ordinary Python programs. It creates a job request using
:meth:`disco.core.Disco.new_job` similarly to a normal Disco request,
but instead of sending it to the master, it instantiates a local
:mod:`disco_worker` and passes the request to it. This allows local
execution of exactly the same map and reduce tasks as you would run in
the distributed environment.

As a result, you can treat your job functions as a normal Python
program and use standard Python debuggers and profilers to analyze the
code. When the code performs as expected locally, you can start using
it in the normal Disco environment without any modifications. Besides
some marginal cases, you can expect the code to work in Disco as well
as it works locally.

:mod:`homedisco` can be found in the `disco/util` directory in the
Disco distribution.

.. _usehomedisco:

Usage
-----

:mod:`homedisco` runs a single map or reduce instance at a time. Nothing
is run in parallel, nor is the map task automatically followed by reduce,
although both of them may be specified in the parameters. This way you
can test your map and reduce functions independently from each other
and focus on edit-run-debug cycle with one task without running the
other. 

:mod:`homedisco` tasks may read any inputs, remote or local, as any
other Disco job. However, results from a task are always written to a
new directory that is automatically created under the directory where
:mod:`homedisco` is run.

As an important exception to the normal Disco environment,
:mod:`homedisco` does not have any rate limits for sending log messages
with :func:`disco_worker.msg`. This makes debugging job functions easier,
as the functions can produce arbitrary amounts of debugging output.

The following example illustriates usage of the module::
        
        def fun_map(e, params):
                return [(e, e)]
        
        def fun_reduce(iter, out, params):
                for k, v in iter:
                        out.add("red:" + k, v)
        
        f = file("homedisco-test", "w")
        print >> f, "dog\ncat\npossum"
        f.close()

        map_hd = HomeDisco("map")
        reduce_hd = HomeDisco("reduce")
        
        res = map_hd.job("disco://localhost:5000", "homedisco",\
                        ["homedisco-test"], fun_map, reduce = fun_reduce)
        
        res = reduce_hd.job("disco://localhost:5000", "homedisco",\
                        res, fun_map, reduce = fun_reduce)
        
        for k, v in disco.result_iterator(res):
                print "KEY", k, "VALUE", v

Map and reduce functions are defined as usual. This example writes its
own input file in ``homedisco-test`` but it could as well read any input
file either locally or from an external source, as any Disco job.

We need two separate :class:`homedisco.HomeDisco` environments: One for
running the map task, *map_hd*, and one for the reduce, *reduce_hd*. Using
these environments, we can call :meth:`homedisco.HomeDisco.job` that
works exactly like :meth:`disco.core.Disco.new_job`. Outputs of the map
task are given as inputs to the reduce task. In the end, we print out
the results using :func:`disco.core.result_iterator`.

Since :meth:`homedisco.HomeDisco.job` runs only single instance of
the given task, the map task accepts only one input, in contrast to
:meth:`disco.core.Disco.new_job` that can take several. Similarly,
if you have several partitions (i.e. *nr_reduces* is larger than one),
only one of them will be processed by the reduce task, as specified by
the *partition* parameter in :class:`homedisco.HomeDisco`. However, the
reduce task may take several inputs in which case only data belonging to
the specified partition will be used from the files, as long as they are
saved in the ``chunk://`` format --- usually Disco handles this issue
correctly by itself.

Note that the format of result files that are produced by the map
task depends whether the map is used alone or whether it is followed
by reduce. Thus if you want to read outputs of the map task with
:func:`disco.core.result_iterator`, you must not specify *reduce* in
:meth:`homedisco.HomeDisco.job`. However, if your map task is followed
by reduce, as in the above example, you should specify the parameter
*reduce* as usual.

Module contents
---------------

.. class:: HomeDisco(mode, partition)

   Creates a new local environment for execution of Disco tasks. *mode*
   must be either a string "map" or "reduce" depending on the task
   that will be run. If *mode* is reduce, the parameter *partition*
   specifies from which partition the reduce will access its data. By
   default *partition = 0*.

   .. method:: HomeDisco.job(...)

      Runs a Disco task locally. It takes exactly the same parameters
      as :func:`disco.job`. This way you can test and debug your
      job easily simply by replacing a :func:`disco.job` call with a
      :meth:`homedisco.HomeDisco.job` call. The *master* parameter as
      defined in :func:`disco.job` is ignored, although it is required.
      
      Returns a list of URLs to (local) result files, similarly to
      :func:`disco.job`.

      Note that this call runs only a single map or reduce
      instance. Nothing is run in parallel, nor is the map task
      automatically followed by reduce, although both of them may be
      specified in the parameters. See :ref:`usehomedisco` above for
      usage instructions.









