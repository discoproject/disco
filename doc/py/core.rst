
:mod:`disco.core` --- Client interface for Disco
================================================

.. module:: disco.core
   :synopsis: Client interface for Disco

The :mod:`disco.core` module provides a high-level interface for
communication with the Disco master. It provides functions for submitting
new jobs, querying status of the system, and getting results of jobs.

The :class:`Disco` object encapsulates connection to the Disco
master. Once a connection has been established, you can use the
object to query status of the system, or submit a new job with the
:meth:`Disco.new_job` method. See the :mod:`disco.func` module for more
information about constructing Disco jobs.

:meth:`Disco.new_job` is provided with all information needed to run
a job, which it packages and sends to the master. The method returns
immediately and returns a :class:`Job` object that corresponds to the
newly started job.

All methods in :class:`Disco` that are related to individual jobs, namely

 - :meth:`Disco.wait`
 - :meth:`Disco.jobinfo`
 - :meth:`Disco.results`
 - :meth:`Disco.events`
 - :meth:`Disco.jobspec`
 - :meth:`Disco.clean`
 - :meth:`Disco.purge`
 - :meth:`Disco.kill`
 - :meth:`Disco.oob_get`
 - :meth:`Disco.oob_list`
 - :meth:`Disco.profile_stats`

are also accessible through the :class:`Job` object, so you can say
`job.wait()` instead of `disco.wait(job.name)`. However, the job methods
in :class:`Disco` come in handy if you want to manipulate a job that is
identified by a job name (:attr:`Job.name`) instead of a :class:`Job`
object.

If you have access only to results of a job, you can extract the job
name from an address with the :func:`disco.util.jobname` function. A typical
case is that you are done with results of a job and they are not needed
anymore. You can delete the unneeded job files as follows::
        
        from disco.core import Disco
        from disco.util import jobname

        Disco(master).purge(jobname(results[0]))


:class:`Disco` --- Interface to the Disco master
------------------------------------------------

.. class:: Disco(host)

   Opens and encapsulates connection to the Disco master.

   *host* is the address of the Disco master, for instance
   ``disco://localhost``. See :func:`disco.util.disco_host` for more
   information on how *host* is interpreted.

   .. method:: Disco.request(url[, data, raw_handle])

   Requests *url* at the master. If a string *data* is specified, a POST request
   is made with *data* as the request payload. If *raw_handle* is set to *True*,
   a file handle to the results is returned. By default a string is returned
   that contains the reply for the request. This method is mostly used by other
   methods in this class internally.

   .. method:: Disco.nodeinfo()

   Returns a dictionary describing status of the nodes that are managed by
   this Disco master.
   
   .. method:: Disco.joblist()

   Returns a list of jobs and their statuses.

   .. method:: Disco.kill(name)

   Kills the job *name*.

   .. method:: Disco.clean(name)

   Cleans records of the job *name*. Note that after the job records
   have been cleaned, there is no way to obtain addresses to the result
   files from the master. However, no files are actually deleted by
   :meth:`Disco.clean`, in contrast to :meth:`Disco.purge`. This function
   provides a way to notify the master not to bother about the job anymore,
   although you want to keep results of the job for future use. If you
   won't need the results, use :meth:`Disco.purge`.

   .. method:: Disco.purge(name)

   Deletes all records and files related to the job *name*. This implies
   :meth:`Disco.clean`.

   .. method:: Disco.jobspec(name)

   Returns the raw job request package, as constructed by
   :meth:`Disco.new_job`, for the job *name*.
   
   .. method:: Disco.events(name[, offset = 0])
   
   (*Added in version 0.2.3*) 

   Returns an iterator that iterates over current events of the job, starting
   from the oldest event. It is safe to call this function while the job is running.
   
   The iterator returns tuples ``(offset, event)``. You can pass an *offset* value
   to this function, to make the iterator skip over the events before the specified
   *offset*. This provides an efficient way to monitor job events continuously. 
   For a built-in example, see the *show* parameter in :meth:`Disco.wait`.

   .. method:: Disco.results(jobspec[, timeout = 5000])

   This function returns a list of results for a single job or for many
   concurrently running jobs, depending on the type of *jobspec*.

   If *jobspec* is a string (job name) or the function is called through the
   job object (``job.results()``), this function returns a list of results for
   the job if the results become available in *timeout* milliseconds. If not,
   returns an empty list.

   (*Added in version 0.2.1*): If *jobspec* is a list of jobs, the function waits at most 
   for *timeout* milliseconds for at least one on the jobs to finish. In 
   this mode, *jobspec* can be a list of strings (job names), a list of job 
   objects, or a list of result entries as returned by this function. Two 
   lists are returned: a list of finished jobs and a list of still active jobs. 
   Both the lists contain elements of the following type::
       
       ["job name", ["status", [results]]]
 
   where status is either ``unknown_job``, ``dead``, ``active`` or ``ready``.
   
   You can use the latter mode as an efficient way to wait for several jobs
   to finish. Consider the following example that prints out results of jobs
   as soon as they finish. Here ``jobs`` is initially a list of jobs, 
   produced by several calls to :meth:`Disco.new_job`:: 

       while jobs:
           ready, jobs = disco.results(jobs)
           for name, results in ready:
               for k, v in result_iterator(results[1]):
                   print k, v
               disco.purge(name)
   
   Note how the list of active jobs, ``jobs``, returned by :meth:`Disco.results` 
   can be used as the input to the function itself.

   .. method:: Disco.jobinfo(name)

   Returns a dictionary containing information about the job *name*.

   .. method:: Disco.oob_get(name, key)

   Returns an out-of-band value assigned to *key* for the job *name*. 
   The key-value pair was stored with a :func:`disco_worker.put` call
   in the job *name*.

   .. method:: Disco.oob_list(name)

   Returns all out-of-band keys for the job *name*. Keys were stored by
   the job *name* using the :func:`disco_worker.put` function.

   .. method:: Disco.profile_stats(name[, mode])

   (*Added in version 0.2.1*)  

   Returns results of profiling of the given job *name*. The job 
   must have been run with the ``profile`` flag enabled.

   You can restrict results specifically to the map or reduce task
   by setting *mode* either to ``"map"`` or ``"reduce"``. By default 
   results include both the map and the reduce phases. Results are
   accumulated from all nodes.

   The function returns a `pstats.Stats object <http://docs.python.org/library/profile.html#the-stats-class>`_.
   You can print out results as follows::

        job.profile_stats().print_stats()

   .. method:: Disco.wait(name[, poll_interval, timeout, clean, show])

   Block until the job *name* has finished. Returns a list URLs to the
   results files which is typically processed with :func:`result_iterator`.
   
   :meth:`Disco.wait` polls the server for the job status every
   *poll_interval* seconds. It raises a :class:`disco.JobException` if the
   job hasn't finished in *timeout* seconds, if specified.
   
   *clean* is a convenience parameter which, if set to `True`,
   calls :meth:`Disco.clean` when the job has finished. This makes
   it possible to execute a typical Disco job in one line::
   
        results = disco.new_job(...).wait(clean = True)

   Note that this only removes records from the master, but not the
   actual result files. Once you are done with the results, call::

        disco.purge(disco.util.jobname(results[0]))

   to delete the actual result files.

   *show* (*Added in version 0.2.3*) enables console output of job events. If
   you want to see output of the job only occasionally, you can control this
   parameter also using the environment variable ``DISCO_EVENTS`` that is
   used to set the value of *show* if it is not specified explicitely. See
   ``DISCO_EVENTS`` in :ref:`settings` for more information.

   .. method:: Disco.new_job(...)

   Submits a new job request to the master. This method accepts the same
   set of keyword as the constructor of the :class:`Job` object below. The
   `master` argument for the :class:`Job` constructor is provided by
   this method. Returns a :class:`Job` object that corresponds to the
   newly submitted job request.

:class:`Job` --- Disco job
--------------------------

.. class:: Job(master, [name, input, map, map_reader, map_writer, reduce, reduce_reader, reduce_writer, partition, combiner, nr_maps, nr_reduces, sort, params, mem_sort_limit, chunked, ext_params, required_files, required_modules, status_interval, profile])

   Starts a new Disco job. You seldom instantiate this class
   directly. Instead, the :meth:`Disco.new_job` is used to start a job
   on a particular Disco master. :meth:`Disco.new_job` accepts the same
   set of keyword arguments as specified below.

   The constructor returns immediately after a job request has been
   submitted. A typical pattern in Disco scripts is to run a job
   synchronously, that is, to block the script until the job has
   finished. This is accomplished as follows::
        
        from disco.core import Disco
        results = Disco(master).new_job(...).wait(clean = True)

   Note that job methods of the :class:`Disco` class are directly
   accessible through the :class:`Job` object, such as :meth:`Disco.wait`
   above.

   The constructor raises a :class:`JobException` if an error occurs
   when the job is started.

   All arguments that are required are marked as such. All other arguments
   are optional.

     * *master* - an instance of the :class:`Disco` class that identifies
       the Disco master runs this job. This argument is required but
       it is provided automatically when the job is started using
       :meth:`Disco.new_job`.

     * *name* - the job name (**required**). The ``@[timestamp]`` suffix is appended
       to the name to ensure uniqueness. If you start more than one job
       per second, you cannot rely on the timestamp which increments only
       once per second. In any case, users are strongly recommended to devise a
       good naming scheme of their own. Only characters in ``[a-zA-Z0-9_]``
       are allowed in the job name.

     * *input* - a list of input files for the map function (**required**). Each
       input must be specified in one of the following protocols:

         * ``http://www.example.com/data`` - any HTTP address
         * ``disco://cnode03/bigtxt/file_name`` - Disco address. Refers to ``cnode03:/var/disco/bigtxt/file_name``. Currently this is an alias for ``http://cnode03:[DISCO_PORT]/bigtxt/file_name``.
         * ``dir://cnode03/jobname/`` - Result directory. This format is used by Disco internally.
         * ``/home/bob/bigfile.txt`` - a local file. Note that the file must either exist on all the nodes or you must make sure that the job is run only on the nodes where the file exists. Due to these restrictions, this form has only limited use.
         * ``raw://some_string`` - pseudo-address; instead of fetching data from a remote source, use ``some_string`` in the address as data. Useful for specifying dummy inputs for generator maps.

       (*Added in version 0.2.2*): An input entry can be a list of inputs: This
       lets you specify multiple redundant versions of an input file. If a list
       of redundant inputs is specified, scheduler chooses the input that is 
       located on the node with the lowest load at the time of scheduling. 
       Redundant inputs are tried one by one until the task succeeds. Redundant
       inputs require that the *map* function is specified.

     * *map* - a :term:`pure function` that defines the map task. 
       The function takes two parameters, an input entry and a parameter object,
       and it outputs a list of key-value pairs in tuples. For instance::

                def fun_map(e, params):
                        return [(w, 1) for w in e.split()]

       This example takes a line of text as input in *e*, tokenizes it, and returns
       a list of words as the output. The argument *params* is the object
       specified by *params* in :func:`disco.job`. It may be used to maintain state
       between several calls to the map function.

       The map task can also be an external program. For more information, see
       :ref:`discoext`.
        
     * *map_reader* - a function that parses input entries from
       an input file. By default :func:`disco.func.map_line_reader`. The function is defined 
       as follows::

                def map_reader(fd, size, fname)

       where *fd* is a file object connected to the input file, *size* is the input
       size (may be *None*), and *fname* is the input file name. The reader function
       must read at most *size* bytes from *fd*. The function parses the stream and
       yields input entries to the map function.

       Disco worker provides a convenience function :func:`disco.func.re_reader`
       that can be used to create parser based on regular expressions.

       If you want to use outputs of an earlier job as inputs, use
       :func:`disco.func.chain_reader` as the *map_reader*.

     * *map_writer* - (*Added in version 0.2*) a function that serializes map results to
       an intermediate result file. This function is defined as follows::

                def map_writer(fd, key, value, params)
       
       where *fd* is a file object conneted to an output file. *key* and *value*
       are an output pair from the *map* function. *params* is the parameter
       object specified by the *params* parameter. By default, *map_writer* is
       :func:`disco.func.netstr_writer`.
       
       Remember to specify *reduce_reader* that can read the format produced
       by *map_writer*.

       This function comes in handy e.g. when *reduce* is not specified and you
       want *map* to output results in a specific format. Another
       typical case is to use :func:`disco.func.object_writer` as *map_writer*
       and :func:`disco.func.object_reader` as *reduce_reader* so
       you can output arbitrary Python objects in *map*, not only strings.

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
      
       Counts how many teams each key appears in the intermediate results. If 
       no reduce function is specified, the job will quit after
       the map phase has finished. 
       
       The reduce task can also be an external program. For more
       information, see :ref:`discoext`.
       
       *Changed in version 0.2*: It is possible to define only *reduce*
       without *map*. In this case the *nr_reduces* parameter is required
       as well. For more information, see the FAQ entry :ref:`reduceonly`.
  
     * *reduce_reader* - (*Added in version 0.2*) a function that deserializes
       intermediate results serialized by *map_writer*. The function signature
       is the same as in *map_reader*. By default, *reduce_reader* is
       :func:`disco.func.netstr_reader`. 
       
       This function needs to match with *map_writer*, if *map* is specified.
       If *map* is not specified, you can read arbitrary input files with this
       function, similarly to *map_reader*.

     * *reduce_writer* - (*Added in version 0.2*) a function that serializes
       reduce results to a result file. The function signature is the same as
       in *map_writer*. By default, *reduce_writer* is
       :func:`disco.func.netstr_writer`.

       You can use this function to output results in an arbitrary format from
       your map/reduce job. If you use :func:`result_iterator` to read
       results of the job, set its *reader* parameter to a function
       that can read the format produced by *reduce_writer*.

     * *partition* - a :term:`pure function` that defines the partitioning
       function, that is, the function that decides how the map outputs
       are distributed to the reduce functions. The function is defined as
       follows::

                def partition(key, nr_reduces, params)

       where *key* is a key returned by the map function and *nr_reduces* the
       number of reduce functions. The function returns an integer between 0 and
       *nr_reduces* that defines to which reduce instance this key-value pair is
       assigned. *params* is an user-defined object as defined by the *params*
       parameter in :meth:`Disco.job`.

       The default partitioning function is :func:`disco.func.default_partition`.

     * *combiner* - a :term:`pure function` that can be used to post-process
       results of the map function. The function is defined as follows::

                def combiner(key, value, comb_buffer, done, params)

       where the first two parameters correspond to a single key-value
       pair from the map function. The third parameter, *comb_buffer*,
       is an accumulator object, a dictionary, that combiner can use to
       save its state. Combiner must control the *comb_buffer* size,
       to prevent it from consuming too much memory, for instance, by
       calling *comb_buffer.clear()* after a block of results has been
       processed. *params* is an user-defined object as defined by the
       *params* parameter in :func:`disco.job`.
       
       Combiner function may return an iterator of key-value pairs
       (tuples) or *None*.

       Combiner function is called after the partitioning function, so
       there are *nr_reduces* separate *comb_buffers*, one for each reduce
       partition. Combiner receives all key-value pairs from the map
       functions before they are saved to intermediate results. Only the
       pairs that are returned by the combiner are saved to the results.

       After the map functions have consumed all input entries,
       combiner is called for the last time with the *done* flag set to
       *True*. This is the last opportunity for the combiner to return
       an iterator to the key-value pairs it wants to output.

     * *nr_maps* - the number of parallel map operations. By default,
       ``nr_maps = len(input_files)``. Note that making this value
       larger than ``len(input_files)`` has no effect. You can only save
       resources by making the value smaller than that.

     * *nr_reduces* - the number of parallel reduce operations. This equals
       to the number of partitions. By default, ``nr_reduces = max(nr_maps / 2, 1)``.

     * *map_init* - initialization function for the map task. This function
       is called once before the actual processing starts with *fun_map*.
       The *map_init* function is defined as follows::
                
                def init(input_iter, params)

       where *input_iter* is an instance of *map_reader* that produces 
       for this map task. The second argument, *params*, is the parameter
       object specified in the ``new_job`` call.

       Typically *map_init* is used to initialize some modules in the worker
       environment (e.g. ``ctypes.cdll.LoadLibrary()``), to initialize some
       values in *params*, or to skip unneeded entries in the beginning 
       of the input stream.

     * *reduce_init* - initialization function for the reduce task. This
       function is called once before the actual processing starts with
       the *reduce* function. The function is defined similarly to *map_init* 
       above. In this case, *input_iter* is a generator object that produces
       key-value pairs belonging to this partition.

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
       
       False by default.

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

     * *chunked* - (*Deprecated in version 0.2.2*) if the reduce function is 
       specified, the worker saves results from a single map instance to a 
       single file that includes key-value pairs for all partitions. When the 
       reduce function is executed, the worker knows how to retrieve pairs for 
       each partition from the files separately. This is called the chunked mode.

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

     * *required_files* - (*Added in version 0.2.3*) is a list of additional 
       files that are required by the job. You can either specify a list of
       paths to files that should be included, or a dictionary which contains
       file names as keys and file contents as the corresponding values. Note
       that all files will be saved in a flat directory - no subdirectories 
       are created.

       You can use this parameter to include custom modules or shared libraries
       in the job. Note that ``LD_LIBRARY_PATH`` is set so that you can include
       a shared library ``foo.so`` in *required_files* and load it in the job
       directly as ``ctypes.cdll.LoadLibrary("foo.so")``. For an example, see 
       :ref:`discoext`.

     * *required_modules* - (*Changed in version 0.2.3*) Disco tries to guess
       which modules are needed by your job functions automatically. It sends
       any local dependencies (i.e. modules not included in the Python standard
       library) to nodes by default.

       If the guessing fails, or you have other special requirements, see
       :mod:`disco.modutil` for options. Note that a *required_modules* list 
       specified for an earlier Disco version still works as intended.

     * *status_interval* - print out "K items mapped / reduced" for
       every Nth item. By default 100000. Setting the value to 0 disables
       messages.

       Increase this value, or set it to zero, if you get "Message rate limit
       exceeded" error due to system messages. This might happen if your map /
       reduce task is really fast. Decrease the value if you want to follow 
       your task in more real-time or you don't have many data items.

     * *profile* - Enable tasks profiling. By default false. Retrieve profiling
       results with the :meth:`Disco.profile_stats` function.

    .. attribute:: Job.name

       Name of the job. You can store or transfer the name string if
       you need to identify the job in another process. In this case,
       you can use the job methods in :class:`Disco` directly.

    .. attribute:: Job.master

       An instance of the :class:`Disco` class that identifies the Disco
       master that runs this job.


.. class:: Params([key = value])

   Parameter container for map / reduce tasks. This object provides a convenient
   way to contain custom parameters, or state, in your tasks. 

   This example shows a simple way of using :class:`Params`::
        
        def fun_map(e, params):
                params.c += 1
                if not params.c % 10:
                        return [(params.f(e), params.c)]
                else:
                        return [(e, params.c)]

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

.. function:: result_iterator(results[, notifier, reader])

   Iterates the key-value pairs in job results. *results* is a list of
   results, as returned by :meth:`Disco.wait`.

   *notifier* is a function that accepts a single parameter, a URL of
   the result file, that is called when the iterator moves to the next
   result file.

   *reader* specifies a custom reader function. Specify this to match
   with a custom *map_writer* or *reduce_writer*. By default, *reader*
   is :func:`disco.func.netstr_reader`.

.. class:: JobException

   Raised when job fails on Disco master.

   .. attribute:: msg
 
   Error message.

   .. attribute:: JobException.name

   Name of the failed job.

   .. attribute:: JobException.master
   
   Address of the Disco master that produced the error.

