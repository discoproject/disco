
:mod:`disco` --- Disco client
=============================

.. module:: disco
   :synopsis: Main interface to Disco

The :mod:`disco` module provides a high-level interface for running Disco jobs.

This module also contains a number of generic functions that can be used
in jobs, as well as utility functions and objects that help specifying
jobs and retrieving their results.

For a lower-level interface for Disco's Web API, see :mod:`discoapi`.

A new job is started with the :func:`disco.job` function. The function
is provided with all information needed to run a job, which it packages
and sends to the master. In the default case, it blocks to wait for the
results. Once the job has finished, it returns a list of URLs to the
result files, which can be retrieved with :func:`disco.result_iterator`.

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
   hierarchy is lost between the files. For more information, see :ref:`discoext`.

.. function:: result_iterator(results[, notifier])

   Iterates the key-value pairs in job results. *results* is a list of
   results, as returned by :func:`disco.job` in the synchronous mode
   or :meth:`discoapi.Disco.wait` or :meth:`discoapi.Disco.results`
   in the asynchronous mode.

   *notifier* is a function that accepts a single parameter, a URL of
   the result file, that is called when the iterator moves to the next
   result file.

.. function:: job(master, name, input_files, fun_map[, map_reader, reduce, partition, combiner, nr_maps, nr_reduces, sort, params, mem_sort_limit, async, clean, chunked, ext_params])

   Starts a new Disco job. The first four parameters are required, which define
   the disco master to be used, name of the job, input files, and a map
   function. The rest of the parameters are optional.

   :func:`disco.job` raises a :class:`discoapi.JobException` if an error occurs
   when the job is run.

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
       function takes two parameters, an input entry and a parameter object,
       and it outputs a list of key-value pairs in tuples. For instance::

                def fun_map(e, params):
                        return [(w, 1) for w in e.split()]

       This example takes a line of text as input in *e*, tokenizes it, and returns
       a list of words as the output. The argument *params* is the object
       specified by *params* in :func:`disco.job`. It may be used to maintain state
       between several calls to the map function.

       The map task can also be an external program. For more information, see
       :ref:`discoext`.
        
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

     * *reduce* - a :term:`pure function` that defines the reduce task. The
       function takes three parameters, an iterator to the intermediate
       key-value pairs produced by the map function. an output object that
       handles the results, and a parameter object. For instance::

                def fun_reduce(iter, out, params):
                        d = {}
                        for w, c in iter:
                                if w in d:
                                        d[w] += 1
                                else:
                                        d[w] = 1
                        for w, c in d.iteritems():
                                out.add(w, c)
      
       Counts how many teams each key appears in the intermediate results.

       By default no reduce function is specified and the job will quit after
       the map functions have finished.
       
       The reduce task can also be an external program. For more
       information, see :ref:`discoext`.

     * *partition* - a :term:`pure function` that defines the partitioning
       function, that is, the function that decides how the map outputs
       are distributed to the reduce functions. The function is defined as
       follows::

                def partition(key, nr_reduces)

       where *key* is a key returned by the map function and *nr_reduces* the
       number of reduce functions. The function returns an integer between 0 and
       *nr_reduces* that defines to which reduce instance this key-value pair is
       assigned.

       The default partitioning function is func:`disco.default_partition`.

     * *combiner* - a :term:`pure function` that can be used to post-process
       results of the map function. The function is defined as follows::

                def combiner(key, value, comb_buffer, flush)

       where the first two parameters correspond to a single key-value
       pair from the map function. The third parameter, *comb_buffer*,
       is an accumulator object, a dictionary, that combiner can use to
       save its state. Combiner must control the *comb_buffer* size,
       to prevent it from consuming too much memory. 

       The combiner has two main modes of operation, depending on
       the value it returns. Most often, it is used to merge results
       using an internal buffer, *comb_buffer*. The buffer is flushed
       by the worker if the function returns the boolean value *True*.
       Alternatively, you can ignore the buffer and use the combiner only
       to rewrite key-value pairs by returning a tuple. If the returned
       value is neither *True* nor a tuple, the key-value pair is ignored.

       The last parameter, *flush*, is a boolean value that instructs combiner
       to transform *comb_buffer* to valid key-value pairs for the output stream.
       After the combiner call returns, the worker will iterate through the
       *comb_buffer*, add values to the output stream and empty the buffer. This
       feature allows the combiner function to store data in *comb_buffer* in
       any way it likes and transform it to proper stringified key-value pairs
       only when needed. This is the first mode of operation.

       In the second mode of operation, the combiner function returns a
       key-value pair in a tuple, the pair is immediately added to the
       output stream. Since combiner is called after partitioning, you
       can use the map function to output a key-value pair that contains
       necessary information in the key for the partitioning function, and
       use the combiner to filter out the partitioning information from
       the key and reformat the key-value pair for the reduce function.

     * *nr_maps* - the number of parallel map operations. By default,
       ``nr_maps = len(input_files)``. Note that making this value
       larger than ``len(input_files)`` has no effect. You can only save
       resources by making the value smaller than that.

     * *nr_reduces* - the number of parallel reduce operations. This equals
       to the number of partitions. By default, ``nr_reduces = max(nr_maps / 2, 1)``.

     * *sort* - a boolean value that specifies whether the intermediate results,
       that is, input to the reduce function, should be sorted. Sorting is most
       useful in ensuring that the equal keys are consequent in the input for
       the reduce function.

       Other than ensuring that equal keys are grouped together, sorting
       ensures that numerical keys are returned in the ascending order. No
       other assumptions should be made on the comparison function.

       Sorting is performed in memory, if the total size of the input data
       is less than *mem_sort_limit* bytes. If it is larger, the external
       program ``sort`` is used to sort the input on disk.
       
       True by default.

     * *params* - an arbitrary object that is passed to the map and reduce
       function as the second argument. The object is serialized using the
       *pickle* module, so it should be pickleable.

       A convience class :class:`disco.Params` is provided that
       provides an easy way to encapsulate a set of parameters for the
       functions. As a special feature, :class:`disco.Params` allows
       including functions in the parameters by making them pickleable.

       By default, *params* is an empty :class:`disco.Params` object.

     * *mem_sort_limit* - sets the maximum size for the input that can be sorted
       in memory. The larger inputs are sorted on disk. By default 256MB.

     * *async* - by default the :func:`disco.job` is synchronous i.e. it blocks
       until the job has finished and returns URLs to the result files.
       
       By setting *async = True*, the function returns immediately when
       the request has been sent and returns the job ID. After this,
       you can use functions in the module :mod:`discoapi` for querying
       the job status, receiving the results etc.

     * *clean* - clean the job records from the master after the results have
       been returned, if the job was succesful. By default true. If set to
       false, you must use either :func:`discoapi.Disco.clean` or the web interface
       manually to clean the job records.

     * *chunked* - if the reduce function is specified, the worker saves
       results from a single map instance to a single file that includes
       key-value pairs for all partitions. When the reduce function is
       executed, the worker knows how to retrieve pairs for each partition
       from the files separately. This is called the chunked mode.

       If no reduce is specified, results for each partition are saved
       to a separate file. This produces *M \* P* files where *M* is the number
       of maps and *P* is the number of reduces. This number can potentially be
       large, so the *chunked* parameter can be used to enable or disable the
       chunked mode, overriding the default behavior.

       Usually there is no need to use this parameter.
     
     * *ext_params* - if either map or reduce function is an external program,
       typically specified using the :func:`disco.external` function, this
       parameter is used to deliver a parameter set to the program.

       The default C interface for external Disco functions uses
       the *netstring* module to encode the parameter set. Hence the
       *ext_params* value must be a dictionary consisting of string-string
       pairs.

       However, if the external program doesn't use the default C
       interface, it can receive parameters in any format. In this case,
       the *ext_params* value can be an arbitrary string which can be
       decoded by the program properly.
       
       For more information, see :ref:`discoext`.

 












