
:mod:`disco` --- Disco client
=============================

.. module:: disco
   :synopsis: Main interface to Disco

The :mod:`disco` module provides a high-level interface for running Disco jobs.

This module also contains a number of generic functions that can be used
in jobs, as well as utility functions and objects that help in specifying
jobs and retrieving their results.

For a lower-level interface for Disco's Web API, see :mod:`discoapi`.

A new job is started with the :func:`disco.job` function. It takes all
information needed to run a job and posts the job request to a Disco
master.  In the default case, it blocks to wait for the results. The
results are not fetched automatically to the calling host, but they can
be explicitely retrieved with :func:`disco.result_iterator`.

A Disco job may contain several user-defined functions, as specified
below. When writing custom functions, take into account the following 
features of the disco worker environment:

- Only the specified function is included in the request. The function
  can't call any other functions specified in your source file nor it can't
  refer to any global names, including any imported modules. If you need
  a special module in your function, import it within the function body.
  Use of global variables or functions with side-effects, such as
  external files besides the given input, is strongly discouraged.

- The function should not print anything to the stdout or stderr.
  Instead, you can use the function :func:`disco_worker.msg` to
  send messages to the status display. You can use the function
  :func:`disco_worker.data_err`, to abort the task on this node and
  request transfer to another node. Otherwise it is a good idea to just
  let the task die if any exceptions occur -- do not catch any exceptions
  from which you can't recover.

In short, this means all user-provded functions must be pure (see
:term:`pure function`).

The module :mod:`disco` exports the following classes and functions:

.. class:: Params([key = value])

   Parameter container for map / reduce tasks. This object provides a convenient
   way to contain custom parameters, or state, in your tasks. 

   This example shows a simple way of using :class:`Params`::
        
        def fun_map(e, params):
                if not params.c % 10:
                        return [(params.f(e), params.c)]
                else:
                        return [(e, params.c)]
                params.c += 1

        disco.job("disco://localhost:5000",
                  ["disco://localhost/myjob/file1"],
                  fun_map,
                  params = disco.Params(c = 0, f = lambda x: x + "!"))

   You can specify any number of key-value pairs to the :class:`Params`
   constructor.  The pairs will be delivered as-is to map and reduce
   functions through the *params* argument. *Key* must be a valid Python
   identifier but *value* can be any Python object. For instance, *value*
   can be an arbitrary :term:`pure function`, such as *params.f* in the
   previous example.

.. function:: default_partition(key, nr_reduces)

   Default partitioning function. Defined as::

        def default_partition(key, nr_reduces):
                return hash(str(key)) % nr_reduces

.. function:: make_range_partition(min_val, max_val)

   Returns a new partitioning function that partitions keys in the range
   *[min_val:max_val]* to equal sized partitions. The number of partitions is
   defined by *nr_reduces* in :func:`disco.job`. 

.. function:: nop_reduce(iter, out, params)

   No-op reduce. Defined as::

        for k, v in iter:
                out.add(k, v)

   This function can be used to combine results per partition from many
   map functions to a single result file per partition.

.. function:: map_line_reader(fd, sze, fname)

   Default input reader function. Reads inputs line by line. 

.. function:: chain_reader(fd, sze, fname)

   Reads output of a map / reduce job as the input for a new job. You must specify this
   function as *map_reader* in :func:`disco.job` if you want to use outputs of a
   previous map / reduce job as the input for another job.

.. function:: external(files)

   Packages an external program, together with other files it depends
   on, to be used either as a map or reduce function. *Files* must be
   a list of paths to files so that the first file points at the actual
   executable.
   
   This example shows how to use an external program, *cmap* that needs a
   configuration file *cmap.conf*, as the map function::

        disco.job("disco://localhost:5000",
                  ["disco://localhost/myjob/file1"],
                  fun_map = disco.external(
                        ["/home/john/bin/cmap", "/home/john/cmap.conf"]))

   All files listed in *files* are copied to the same directory so any file
   hierarchy is lost between the files.

.. function:: result_iterator(results[, notifier])

   Iterates the key-value pairs in job results. *results* is a list of
   results, as returned by func:`disco.job` in the synchronous mode or
   func:`discoapi.wait` or func:`discoapi.results` in the asynchronous
   mode.

   *notifier* is a function that accepts a single parameter, a URL of
   the result file, that is called when the iterator moves to the next
   result file.

.. function:: job(master, name, input_files, fun_map[, map_reader, reduce, partition, combiner, nr_maps, nr_reduces, sort, params, mem_sort_limit, async, clean, chunked, ext_params])

   Starts a new Disco job. The first four parameters are required, which define
   the disco master to be used, name of the job, input files, and a map
   function. The rest of the parameters are optional.

     * *master* - a URL pointing at the Disco master, for instance ``disco://localhost:5000``.

     * *name* - the job name. The ``@[timestamp]`` suffix is appended
       to the name to ensure uniqueness. If you start more than one job
       per second, you cannot rely on the timestamp which increments only
       once per second. In any case, users are strongly recommended to devise a
       good naming scheme of their own. Only characters in ``[a-zA-Z0-9_]``
       are allowed in the job name.

     * *input_files* - a list of input files for the map function. Each
       input must be specified in one of the following four protocols:

         * ``http://www.example.com/data`` - any HTTP address
         * ``disco://cnode03/bigtxt/file_name`` - Disco address. Refers to ``cnode03:/var/disco/bigtxt/file_name``. Currently this is an alias for ``http://cnode03:8989/bigtxt/file_name``.
         * ``dir://cnode03/jobname/`` - Result directory. This format is used by Disco internally.
         * ``/home/bob/bigfile.txt`` - a local file. Note that the file must either exist on all the nodes or you must make sure that the job is run only on the nodes where the file exists. Due to these restrictions, this form has only limited use.

     * *fun_map* - a :term:`pure function` that defines the map task. The
       function takes two parameter, the input entry and the parameters,
       and it outputs a list of key-value pairs in tuples. An example::

                def fun_map(e, params):
                        return [(w, 1) for w in e.split()]

       This example takes a line of text as input in *e*, tokenizes it, and returns
       a list of words as the output. The argument *params* is the object
       specified by *params* in :func:`disco.job`. It may be used to maintain state
       between several calls to the map function.

     * *map_reader* - a function that parses input entries from an input file. By
       default :func:`disco.map_line_reader`. The function is defined as follows::

                def map_reader(fd, size, fname)

       where *fd* is a file object connected to the input file, *size* is the input
       size (may be *None*), and *fname* is the input file name. The reader function
       must read at most *size* bytes from *fd*. The function parses the stream and
       yields input entries to the map function.

       Disco worker provides a convenience function :func:`disco_worker.re_reader`
       that can be used to create parser based on regular expressions.

       If you want to use outputs of an earlier job as inputs, use
       :func:`disco.chain_reader` as the *map_reader*.

     * *reduce* - a :term:`pure function` that defines the reduce task. 













