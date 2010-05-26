"""
:mod:`disco.core` --- Client interface for Disco
================================================

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

.. autoclass:: Disco
        :members:
.. autoclass:: JobDict
        :members: pack, unpack
.. autoclass:: Job
        :members:
.. autoclass:: Params
        :members:
.. autofunction:: result_iterator
"""
import sys, os, time, marshal
from tempfile import NamedTemporaryFile
from itertools import chain
from warnings import warn

from disco import func, util
from disco.comm import download, json
from disco.error import DiscoError, JobError, CommError
from disco.eventmonitor import EventMonitor
from disco.modutil import find_modules
from disco.netstring import decode_netstring_fd, encode_netstring_fd
from disco.settings import DiscoSettings

class Continue(Exception):
    pass

class Disco(object):
    """
    Opens and encapsulates connection to the Disco master.

    :param master: address of the Disco master,
                   for instance ``disco://localhost``.
    """
    def __init__(self, master):
        self.master = master

    def request(self, url, data=None, redir=False, offset=0):
        """
        Requests *url* at the master.

        If a string *data* is specified, a POST request
        is made with *data* as the request payload.

        A string is returned that contains the reply for the request.
        This method is mostly used by other methods in this class internally.
        """
        try:
            return download('%s%s' % (self.master, url),
                            data=data,
                            redir=redir,
                            offset=offset)
        except CommError, e:
            e.msg += " (is disco master running at %s?)" % self.master
            raise

    def get_config(self):
        return json.loads(self.request('/disco/ctrl/load_config_table'))

    def set_config(self, config):
        response = json.loads(self.request('/disco/ctrl/save_config_table', json.dumps(config)))
        if response != 'table saved!':
            raise DiscoError(response)

    config = property(get_config, set_config)

    @property
    def ddfs(self):
        from disco.ddfs import DDFS
        return DDFS(self.master)

    def nodeinfo(self):
        """
        Returns a dictionary describing status of the nodes that are managed by
        this Disco master.
        """
        return json.loads(self.request('/disco/ctrl/nodeinfo'))

    def joblist(self):
        """Returns a list of jobs and their statuses."""
        return json.loads(self.request('/disco/ctrl/joblist'))

    def blacklist(self, node):
        """
        Blacklists *node* so that tasks are no longer run on it.

        (*Added in version 0.2.4*)
        """
        self.request('/disco/ctrl/blacklist', '"%s"' % node)

    def whitelist(self, node):
        """
        Whitelists *node* so that the master may submit tasks to it.

        (*Added in version 0.2.4*)
        """
        self.request('/disco/ctrl/whitelist', '"%s"' % node)

    def oob_get(self, name, key):
        """
        Returns an out-of-band value assigned to *key* for the job *name*.

        See :mod:`disco.node.worker` for more information on using OOB.
        """
        try:
            return util.load_oob(self.master, name, key)
        except CommError, e:
            if e.code == 404:
                raise DiscoError("Unknown key or job name")
            raise

    def oob_list(self, name):
        """
        Returns all out-of-band keys for the job *name*.

        OOB data is stored by the tasks of job *name*,
        using the :func:`disco_worker.put` function.
        """
        urls = self.ddfs.get(util.ddfs_oobname(name))['urls']
        return list(set(self.ddfs.blob_name(replicas[0])
                        for replicas in urls))

    def profile_stats(self, name, mode=''):
        """
        Returns results of profiling of the given job *name*.

        The job must have been run with the ``profile`` flag enabled.

        You can restrict results specifically to the map or reduce task
        by setting *mode* either to ``"map"`` or ``"reduce"``. By default
        results include both the map and the reduce phases. Results are
        accumulated from all nodes.

        The function returns a `pstats.Stats object <http://docs.python.org/library/profile.html#the-stats-class>`_.
        You can print out results as follows::

                job.profile_stats().print_stats()

        (*Added in version 0.2.1*)
        """
        prefix = 'profile-%s' % mode
        f = [s for s in self.oob_list(name) if s.startswith(prefix)]
        if not f:
            raise JobError("No profile data", self.master, name)

        import pstats
        stats = pstats.Stats(Stats(self.oob_get(name, f[0])))
        for s in f[1:]:
            stats.add(Stats(self.oob_get(name, s)))
        return stats

    def new_job(self, name, **kwargs):
        """
        Submits a new job request to the master.

        This method accepts the same set of keyword args as :class:`Job`.
        The `master` argument for the :class:`Job` constructor is provided by
        this method. Returns a :class:`Job` object that corresponds to the
        newly submitted job request.
        """
        return Job(self, name=name).run(**kwargs)

    def kill(self, name):
        """Kills the job *name*."""
        self.request('/disco/ctrl/kill_job', '"%s"' % name)

    def clean(self, name):
        """
        Cleans records of the job *name*.

        Note that after the job records have been cleaned,
        there is no way to obtain addresses to the result files from the master.
        However, no data is actually deleted by :meth:`Disco.clean`,
        in contrast to :meth:`Disco.purge`.

        If you won't need the results, use :meth:`Disco.purge`.

        .. todo:: deprecate cleaning
        """
        self.request('/disco/ctrl/clean_job', '"%s"' % name)

    def purge(self, name):
        """Deletes all records and files related to the job *name*."""
        self.request('/disco/ctrl/purge_job', '"%s"' % name)

    def jobdict(self, name):
        from cStringIO import StringIO
        return JobDict.unpack(StringIO(self.jobpack(name)))

    def jobpack(self, name):
        return self.request('/disco/ctrl/parameters?name=%s' % name, redir=True)

    def jobspec(self, name):
        """
        Returns the raw job request package, as constructed by
        :meth:`Disco.new_job`, for the job *name*.

        .. todo:: deprecate this in favor of jobdict/jobpack
        """
        return self.jobdict(name)

    def events(self, name, offset=0):
        """
        Returns an iterator that iterates over job events, ordered by time.

        It is safe to call this function while the job is running.

        The iterator returns tuples ``(offset, event)``. You can pass an *offset* value
        to this function, to make the iterator skip over the events before the specified
        *offset*. This provides an efficient way to monitor job events continuously.
        See ``DISCO_EVENTS`` in :mod:`disco.settings` for more information on how to enable
        the console output of job events.

        (*Added in version 0.2.3*)
        """
        def event_iter(events):
            offs = offset
            lines = events.splitlines()
            for i, line in enumerate(lines):
                if len(line):
                    offs += len(line) + 1
                    try:
                        event = tuple(json.loads(line))
                    except ValueError:
                        break
                    # HTTP range request doesn't like empty ranges:
                    # Let's ensure that at least the last newline
                    # is always retrieved.
                    if i == len(lines) - 1 and events.endswith('\n'):
                       offs -= 1
                    yield offs, event
        return event_iter(self.rawevents(name, offset=offset))

    def rawevents(self, name, offset=0):
        return self.request("/disco/ctrl/rawevents?name=%s" % name,
                            redir=True,
                            offset=offset)

    def mapresults(self, name):
        return json.loads(
            self.request('/disco/ctrl/get_mapresults?name=%s' % name))

    def results(self, jobspec, timeout=2000):
        """
        Returns a list of results for a single job or for many
        concurrently running jobs, depending on the type of *jobspec*.

        If *jobspec* is a string (job name) or the function is called through the
        job object (``job.results()``), this function returns a list of results for
        the job if the results become available in *timeout* milliseconds. If not,
        returns an empty list.

        (*Added in version 0.2.1*)
        If *jobspec* is a list of jobs, the function waits at most
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
        """
        jobspecifier = JobSpecifier(jobspec)
        data = json.dumps([timeout, list(jobspecifier.jobnames)])
        results = json.loads(self.request('/disco/ctrl/get_results', data))

        if isinstance(jobspec, basestring):
            return results[0][1]

        others, active = [], []
        for result in results:
            if result[1][0] == 'active':
                active.append(result)
            else:
                others.append(result)
        return others, active

    def jobinfo(self, name):
        """Returns a dictionary containing information about the job *name*."""
        return json.loads(self.request('/disco/ctrl/jobinfo?name=%s' % name))

    def check_results(self, name, start_time, timeout, poll_interval):
        status, results = self.results(name, timeout=poll_interval)
        if status == 'ready':
            return results
        if status != 'active':
            raise JobError("Job status %s" % status, self.master, name)
        if timeout and time.time() - start_time > timeout:
            raise JobError("Timeout", self.master, name)
        raise Continue()

    def wait(self, name,
             poll_interval=2,
             timeout=None,
             clean=False,
             show=DiscoSettings()['DISCO_EVENTS']):
        """
        Block until the job *name* has finished. Returns a list URLs to the
        results files which is typically processed with :func:`result_iterator`.

        :meth:`Disco.wait` polls the server for the job status every
        *poll_interval* seconds. It raises a :class:`disco.JobError` if the
        job hasn't finished in *timeout* seconds, if specified.

        :param clean: if set to `True`, calls :meth:`Disco.clean`
                      when the job has finished.

                      Note that this only removes records from the master,
                      but not the actual result files.
                      Once you are done with the results, call::

                        disco.purge(disco.util.jobname(results[0]))

                      to delete the actual result files.

        :param show: enables console output of job events.
                     You can control this parameter also using the environment
                     variable ``DISCO_EVENTS``, which provides the default.
                     See ``DISCO_EVENTS`` in :mod:`disco.settings`.
                     (*Added in version 0.2.3*)
        """
        event_monitor = EventMonitor(Job(self, name=name),
                                     format=show,
                                     poll_interval=poll_interval)
        start_time    = time.time()
        while True:
            event_monitor.refresh()
            try:
                return self.check_results(name, start_time,
                                          timeout, poll_interval * 1000)
            except Continue:
                continue
            finally:
                if clean:
                    self.clean(name)
                event_monitor.refresh()

    def result_iterator(self, *args, **kwargs):
        kwargs['ddfs'] = self.master
        return result_iterator(*args, **kwargs)

class Params(object):
    """
    Parameter container for map / reduce tasks.

    This object provides a convenient way to contain custom parameters,
    or state, in your tasks.

    This example shows a simple way of using :class:`Params`::

        def fun_map(e, params):
                params.c += 1
                if not params.c % 10:
                        return [(params.f(e), params.c)]
                return [(e, params.c)]

        disco.new_job(name="disco://localhost",
                      input=["disco://localhost/myjob/file1"],
                      map=fun_map,
                      params=disco.core.Params(c=0, f=lambda x: x + "!"))

    You can specify any number of key-value pairs to the :class:`Params`.
    The pairs will be available to task functions through the *params* argument.
    Each task receives its own copy of the initial params object.
    *key* must be a valid Python identifier.
    *value* can be any Python object.
    For instance, *value* can be an arbitrary :term:`pure function`,
    such as *params.f* in the previous example.
    """
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __getstate__(self):
        return dict((k, util.pack(v))
            for k, v in self.__dict__.iteritems()
                if not k.startswith('_'))

    def __setstate__(self, state):
        for k, v in state.iteritems():
            self.__dict__[k] = util.unpack(v)

class JobSpecifier(list):
    def __init__(self, jobspec):
        super(JobSpecifier, self).__init__([jobspec]
            if isinstance(jobspec, basestring) else jobspec)

    @property
    def jobnames(self):
        for job in self:
            if isinstance(job, basestring):
                yield job
            elif isinstance(job, list):
                yield job[0]
            else:
                yield job.name

    def __str__(self):
        return '{%s}' % ', '.join(self.jobnames)

class JobDict(util.DefaultDict):
    """
    :meth:`Disco.new_job` and :meth:`Job.run`
    accept the same set of keyword arguments as specified below.

    .. note:: All arguments that are required are marked as such.
              All other arguments are optional.

    :type  input: **required**, list of inputs or list of list of inputs
    :param input: Each input must be specified in one of the following ways:

                   * ``http://www.example.com/data`` - any HTTP address
                   * ``disco://cnode03/bigtxt/file_name`` - Disco address. Refers to ``cnode03:/var/disco/bigtxt/file_name``. Currently this is an alias for ``http://cnode03:[DISCO_PORT]/bigtxt/file_name``.
                   * ``dir://cnode03/jobname/`` - Result directory. This format is used by Disco internally.
                   * ``/home/bob/bigfile.txt`` - a local file. Note that the file must either exist on all the nodes or you must make sure that the job is run only on the nodes where the file exists. Due to these restrictions, this form has only limited use.
                   * ``raw://some_string`` - pseudo-address; instead of fetching data from a remote source, use ``some_string`` in the address as data. Useful for specifying dummy inputs for generator maps.
                   * ``tag://tagname`` - a tag stored in :ref:`DDFS` (*Added in version 0.3*)

                  (*Added in version 0.2.2*):
                  An input entry can be a list of inputs:
                  This lets you specify redundant versions of an input file.
                  If a list of redundant inputs is specified,
                  the scheduler chooses the input that is located on the node
                  with the lowest load at the time of scheduling.
                  Redundant inputs are tried one by one until the task succeeds.
                  Redundant inputs require that the *map* function is specified.

    :type  map: :func:`disco.func.map`
    :param map: a :term:`pure function` that defines the map task.

    :type  map_init: :func:`disco.func.init`
    :param map_init: initialization function for the map task.
                     This function is called once before the task starts.

    :type  map_input_stream: list of :func:`disco.func.input_stream`
    :param map_input_stream: The given functions are chained together and the final resulting
                             :class:`disco.func.InputStream` object is used
                             to iterate over input entries.

                             (*Added in version 0.2.4*)

    :type  map_output_stream: list of :func:`disco.func.output_stream`
    :param map_output_stream: The given functions are chained together and the 
                              :meth:`disco.func.OutputStream.add` method of the last
                              returned :class:`disco.func.OutputStream` object is used
                              to serialize key, value pairs output by the map.
                              (*Added in version 0.2.4*)

    :type  map_reader: :func:`disco.func.input_stream`
    :param map_reader: Convenience function to define the last :func:`disco.func.input_stream`
                       function in the *map_input_stream* chain.
    
                       Disco worker provides a convenience function
                       :func:`disco.func.re_reader` that can be used to create
                       a reader using regular expressions.

                       If you want to use outputs of an earlier job as inputs,
                       use :func:`disco.func.chain_reader` as the *map_reader*.

                       Default is :func:`disco.func.map_line_reader`.

    :param map_writer: (*Deprecated in version 0.3*) This function comes in 
                       handy e.g. when *reduce* is not
                       specified and you want *map* output in a specific format.
                       Another typical case is to use
                       :func:`disco.func.object_writer` as *map_writer* and
                       :func:`disco.func.object_reader` as *reduce_reader*
                       so you can produce arbitrary Python objects in *map*.

                       Remember to specify a *reduce_reader*
                       that can read the format produced by *map_writer*.
                       (*Added in version 0.2*)

    :type  reduce: :func:`disco.func.reduce`
    :param reduce: If no reduce function is specified, the job will quit after
                   the map phase has finished.

                   *Changed in version 0.2*:
                   It is possible to define only *reduce* without *map*.
                   For more information, see the FAQ entry :ref:`reduceonly`.

    :type  reduce_init: :func:`disco.func.init`
    :param reduce_init: initialization function for the reduce task.
                        This function is called once before the task starts.

    :type  reduce_input_stream: list of :func:`disco.func.output_stream`
    :param reduce_input_stream: The given functions are chained together and the last
                              returned :class:`disco.func.InputStream` object is 
                              given to *reduce* as its first argument.
                              (*Added in version 0.2.4*)
    
    :type  reduce_output_stream: list of :func:`disco.func.output_stream`
    :param reduce_output_stream: The given functions are chained together and the last
                              returned :class:`disco.func.OutputStream` object is 
                              given to *reduce* as its second argument.
                              (*Added in version 0.2.4*)

    :type  reduce_reader: :func:`disco.func.input_stream`
    :param reduce_reader: This function needs to match with *map_writer*,
                          if *map* is specified.
                          If *map* is not specified,
                          you can read arbitrary inputs with this function,
                          similar to *map_reader*.
                          (*Added in version 0.2*)

                          Default is :func:`disco.func.chain_reader`.

    :param reduce_writer: (*Deprecated in version 0.3*) You can use this function to output results
                          in an arbitrary format from your map/reduce job.
                          If you use :func:`result_iterator` to read results,
                          set its *reader* parameter to a function
                          that can read the format produced by *reduce_writer*.
                          (*Added in version 0.2*)

    :type  combiner: :func:`disco.func.combiner`
    :param combiner: called after the partitioning function, for each partition.

    :type  partition: :func:`disco.func.partition`
    :param partition: decides how the map output is distributed to reduce.

                      Default is :func:`disco.func.default_partition`.

    :type  partitions: int or None
    :param partitions: number of partitions, if any.

                       Default is ``1``.

    :type  merge_partitions: bool
    :param merge_partitions: whether or not to merge partitioned inputs during reduce.

                             Default is ``False``.

    :type  nr_reduces: *Deprecated in version 0.3* integer
    :param nr_reduces: Use *partitions* instead.

    :type  scheduler: dict
    :param scheduler: options for the job scheduler.
                      The following keys are supported:

                       * *max_cores* - use this many cores at most
                                       (applies to both map and reduce).

                                       Default is ``2**31``.

                       * *force_local* - always run task on the node where
                                         input data is located;
                                         never use HTTP to access data remotely.

                       * *force_remote* - never run task on the node where input
                                          data is located;
                                          always use HTTP to access data remotely.

                      (*Added in version 0.2.4*)

    :type  sort: boolean
    :param sort: flag specifying whether the intermediate results,
                 that is, input to the reduce function, should be sorted.
                 Sorting is most useful in ensuring that the equal keys are
                 consequent in the input for the reduce function.

                 Other than ensuring that equal keys are grouped together,
                 sorting ensures that keys are returned in the ascending order.
                 No other assumptions should be made on the comparison function.

                 Sorting is performed in memory, if the total size of the data
                 is less than *mem_sort_limit* bytes.
                 If it is larger, the external program ``sort`` is used
                 to sort the input on disk.

                 Default is ``False``.

    :type  params: :class:`Params`
    :param params: object that is passed to worker tasks to store state
                   The object is serialized using the *pickle* module,
                   so it should be pickleable.

                   A convience class :class:`Params` is provided that
                   provides an easy way to encapsulate a set of parameters.
                   :class:`Params` allows including
                   :term:`pure functions <pure function>` in the parameters.

    :type  mem_sort_limit: integer
    :param mem_sort_limit: maximum size of data that can be sorted in memory.
                           The larger inputs are sorted on disk.

                           Default is ``256 * 1024**2``.

    :param ext_params: if either map or reduce function is an external program,
                       typically specified using :func:`disco.util.external`,
                       this object is used to deliver parameters to the program.

                       The default C interface for external Disco functions uses
                       :mod:`netstring` to encode the parameter dictionary.
                       Hence the *ext_params* value must be a dictionary
                       string ``(key, value)`` pairs.

                       However, if the external program doesn't use the default
                       C interface, it can receive parameters in any format.
                       In this case, the *ext_params* value can be an arbitrary
                       string which can be decoded by the program properly.

                       For more information, see :ref:`discoext`.

    :type  required_files: list of paths or dict
    :param required_files: additional files that are required by the job.
                           Either a list of paths to files to include,
                           or a dictionary which contains items of the form
                           ``(filename, filecontents)``.

                           You can use this parameter to include custom modules
                           or shared libraries in the job.
                           (*Added in version 0.2.3*)

                           .. note::

                                All files will be saved in a flat directory
                                on the worker.
                                No subdirectories will be created.


                            .. note::

                                ``LD_LIBRARY_PATH`` is set so you can include
                                a shared library ``foo.so`` in *required_files*
                                and load it in the job directly as
                                ``ctypes.cdll.LoadLibrary("foo.so")``.
                                For an example, see :ref:`discoext`.

    :param required_modules: required modules to send to the worker
                             (*Changed in version 0.2.3*):
                             Disco tries to guess which modules are needed
                             by your job functions automatically.
                             It sends any local dependencies
                             (i.e. modules not included in the
                             Python standard library) to nodes by default.

                             If guessing fails, or you have other requirements,
                             see :mod:`disco.modutil` for options.


    :type  status_interval: integer
    :param status_interval: print "K items mapped / reduced"
                            for every Nth item.
                            Setting the value to 0 disables messages.

                            Increase this value, or set it to zero,
                            if you get "Message rate limit exceeded"
                            error due to system messages.
                            This might happen if your tasks are really fast.
                            Decrease the value if you want more messages or
                            you don't have that many data items.

                            Default is ``100000``.

    :type  profile: boolean
    :param profile: enable tasks profiling.
                    Retrieve profiling results with :meth:`Disco.profile_stats`.

                    Default is ``False``.
    """
    defaults = {'input': (),
                'map': None,
                'map_init': func.noop,
                'map_reader': func.map_line_reader,
                'map_input_stream': (func.map_input_stream, ),
                'map_output_stream': (func.map_output_stream,
                                      func.disco_output_stream),
                'combiner': None,
                'partition': func.default_partition,
                'reduce': None,
                'reduce_init': func.noop,
                'reduce_reader': func.chain_reader,
                'reduce_input_stream': (func.reduce_input_stream, ),
                'reduce_output_stream': (func.reduce_output_stream,
                                         func.disco_output_stream),
                'ext_map': False,
                'ext_reduce': False,
                'ext_params': None,
                'mem_sort_limit': 256 * 1024**2,
                'merge_partitions': False,
                'params': Params(),
                'partitions': 1,
                'prefix': '',
                'profile': False,
                'required_files': None,
                'required_modules': None,
                'scheduler': {'max_cores': '%d' % 2**31},
                'save': False,
                'sort': False,
                'status_interval': 100000,
                'version': '.'.join(str(s) for s in sys.version_info[:2]),
                # deprecated
                'nr_reduces': 0,
                'map_writer': None,
                'reduce_writer': None
                }
    default_factory = defaults.__getitem__

    functions = set(['map',
                     'map_init',
                     'map_reader',
                     'map_writer',
                     'combiner',
                     'partition',
                     'reduce',
                     'reduce_init',
                     'reduce_reader',
                     'reduce_writer'])

    scheduler_keys = set(['force_local', 'force_remote', 'max_cores'])

    io_mappings =\
        [('map_reader', 'map_input_stream', func.reader_wrapper),
         ('map_writer', 'map_output_stream', func.writer_wrapper),
         ('reduce_reader', 'reduce_input_stream', func.reader_wrapper),
         ('reduce_writer', 'reduce_output_stream', func.writer_wrapper)]

    stacks = set(['map_input_stream',
                  'map_output_stream',
                  'reduce_input_stream',
                  'reduce_output_stream'])

    def __init__(self, *args, **kwargs):
        super(JobDict, self).__init__(*args, **kwargs)

        # -- backwards compatibility --
        if 'fun_map' in self and 'map' not in self:
            self['map'] = self.pop('fun_map')

        if 'input_files' in kwargs and 'input' not in self:
            self['input'] = self.pop('input_files')

        if 'reduce_writer' in self or 'map_writer' in self:
            warn("Writers are deprecated - use output_stream.add() instead",
                    DeprecationWarning)

        # -- required modules and files --
        if self['required_modules'] is None:
            functions = util.flatten(util.iterify(self[f])
                                     for f in chain(self.functions, self.stacks))
            self['required_modules'] = find_modules([f for f in functions
                                                     if callable(f)])

        # -- external flags --
        if isinstance(self['map'], dict):
            self['ext_map'] = True
        if isinstance(self['reduce'], dict):
            self['ext_reduce'] = True

        # -- input --
        ddfs = self.pop('ddfs', None)
        self['input'] = [list(util.iterify(url))
                         for i in self['input']
                         for url in util.urllist(i, listdirs=bool(self['map']),
                                                 ddfs=ddfs)]

        # partitions must be an integer internally
        self['partitions'] = self['partitions'] or 0

        # set nr_reduces: ignored if there is not actually a reduce specified
        if self['map']:
            # partitioned map has N reduces; non-partitioned map has 1 reduce
            self['nr_reduces'] = self['partitions'] or 1
        elif self.input_is_partitioned:
            # Only reduce, with partitions: len(dir://) specifies nr_reduces
            self['nr_reduces'] = len(util.parse_dir(self['input'][0][0]))
        else:
            # Only reduce, without partitions can only have 1 reduce
            self['nr_reduces'] = 1

        # merge_partitions iff the inputs to reduce are partitioned
        if self['merge_partitions']:
            if self['partitions'] or self.input_is_partitioned:
                self['nr_reduces'] = 1
            else:
                raise DiscoError("Can't merge partitions without partitions")

        # -- scheduler --
        scheduler = self.__class__.defaults['scheduler'].copy()
        scheduler.update(self['scheduler'])
        if int(scheduler['max_cores']) < 1:
            raise DiscoError("max_cores must be >= 1")
        self['scheduler'] = scheduler

        # -- sanity checks --
        if not self['map'] and not self['reduce']:
            raise DiscoError("Must specify map and/or reduce")

        for key in self:
            if key not in self.defaults:
                raise DiscoError("Unknown job argument: %s" % key)

    def pack(self):
        """Pack up the :class:`JobDict` for sending over the wire."""
        jobpack = {}

        if self['required_files']:
            if not isinstance(self['required_files'], dict):
                self['required_files'] = util.pack_files(self['required_files'])
        else:
            self['required_files'] = {}

        self['required_files'].update(util.pack_files(
            o[1] for o in self['required_modules'] if util.iskv(o)))

        for key in self.defaults:
            if key == 'input':
                jobpack['input'] = ' '.join(
                    '\n'.join(reversed(list(util.iterify(url))))
                        for url in self['input'])
            elif key in ('nr_reduces', 'prefix'):
                jobpack[key] = str(self[key])
            elif key == 'scheduler':
                scheduler = self['scheduler']
                for key in scheduler:
                    jobpack['sched_%s' % key] = str(scheduler[key])
            elif self[key] is None:
                pass
            elif key in self.stacks:
                jobpack[key] = util.pack_stack(self[key])
            else:
                jobpack[key] = util.pack(self[key])
        return encode_netstring_fd(jobpack)

    @classmethod
    def unpack(cls, jobpack, globals={}):
        """Unpack the previously packed :class:`JobDict`."""

        jobdict = cls.defaults.copy()
        jobdict.update(**decode_netstring_fd(jobpack))

        for key in cls.defaults:
            if key == 'input':
                jobdict['input'] = [i.split()
                                    for i in jobdict['input'].split(' ')]
            elif key == 'nr_reduces':
                jobdict[key] = int(jobdict[key])
            elif key == 'scheduler':
                for key in cls.scheduler_keys:
                    if 'sched_%s' % key in jobdict:
                        jobdict['scheduler'][key] = jobdict.pop('sched_%s' % key)
            elif key == 'prefix':
                pass
            elif jobdict[key] is None:
                pass
            elif key in cls.stacks:
                jobdict[key] = util.unpack_stack(jobdict[key], globals=globals)
            else:
                jobdict[key] = util.unpack(jobdict[key], globals=globals)
        # map readers and writers to streams
        for oldio, stream, wrapper in cls.io_mappings:
            if jobdict[oldio]:
                jobdict[stream].append(wrapper(jobdict[oldio]))
        return cls(**jobdict)

    @property
    def input_is_partitioned(self):
        return all(url.startswith('dir://')
                   for urls in self['input']
                   for url in urls)

class Job(object):
    """
    Creates a Disco job with the given name.

    Use :meth:`Job.run` to start the job.

    You need not instantiate this class directly.
    Instead, the :meth:`Disco.new_job` can be used to create and start a job.

    :param master: An instance of the :class:`Disco` class that identifies
                   the Disco master runs this job. This argument is required but
                   it is provided automatically when the job is started using
                   :meth:`Disco.new_job`.

    :param name: The job name.
                 When you create a handle for an existing job, the name is used as given.
                 When you create a new job, the name given is used by Disco as a
                 prefix to construct a unique name, which is then stored in the instance.

                 .. note::

                        Only characters in ``[a-zA-Z0-9_]`` are allowed in the job name.

    All methods in :class:`Disco` that are related to individual jobs, namely

        - :meth:`Disco.clean`
        - :meth:`Disco.events`
        - :meth:`Disco.kill`
        - :meth:`Disco.jobinfo`
        - :meth:`Disco.jobspec`
        - :meth:`Disco.oob_get`
        - :meth:`Disco.oob_list`
        - :meth:`Disco.profile_stats`
        - :meth:`Disco.purge`
        - :meth:`Disco.results`
        - :meth:`Disco.wait`

    are also accessible through the :class:`Job` object, so you can say
    `job.wait()` instead of `Disco.wait(job.name)`. However, the job methods
    in :class:`Disco` come in handy if you want to manipulate a job that is
    identified by a job name (:attr:`Job.name`) instead of a :class:`Job`
    object.

    If you have access only to results of a job, you can extract the job
    name from an address with the :func:`disco.util.jobname` function. A typical
    case is that you are done with results of a job and they are not needed
    anymore. You can delete the unneeded job files as follows::

        from disco.core import Job
        from disco.util import jobname

        Job(master, jobname(results[0])).purge()
    """
    proxy_functions = ('clean',
                       'events',
                       'kill',
                       'jobinfo',
                       'jobspec',
                       'oob_get',
                       'oob_list',
                       'profile_stats',
                       'purge',
                       'results',
                       'mapresults',
                       'wait')

    def __init__(self, master, name):
        self.master  = master
        self.name    = name

    def __getattr__(self, attr):
        if attr in self.proxy_functions:
            from functools import partial
            return partial(getattr(self.master, attr), self.name)
        raise AttributeError("%r has no attribute %r" % (self, attr))

    class JobDict(JobDict):
        def __init__(self, job, *args, **kwargs):
            self.job = job
            super(Job.JobDict, self).__init__(*args, **kwargs)

        def default_factory(self, attr):
            try:
                return getattr(self.job, attr)
            except AttributeError:
                return self.defaults.__getitem__(attr)

    def run(self, **kwargs):
        """
        Returns the job immediately after the request has been submitted.

        A typical pattern in Disco scripts is to run a job synchronously,
        that is, to block the script until the job has finished.
        This is accomplished as follows::

                from disco.core import Disco
                results = Disco(master).new_job(...).wait()

        Note that job methods of the :class:`Disco` class are directly
        accessible through the :class:`Job` object, such as :meth:`Disco.wait`
        above.

        A :class:`JobError` is raised if an error occurs while starting the job.
        """
        if 'nr_reduces' in kwargs:
            from warnings import warn
            warn("Use partitions instead of nr_reduces", DeprecationWarning)
            if 'partitions' in kwargs or 'merge_partitions' in kwargs:
                raise DeprecationWarning("Cannot specify nr_reduces with "
                                         "partitions and/or merge_partitions")
            kwargs['partitions'] = kwargs.pop('nr_reduces')

        jobpack = Job.JobDict(self,
                              prefix=self.name,
                              ddfs=self.master.master,
                              **kwargs).pack()
        reply = json.loads(self.master.request('/disco/job/new', jobpack))
        if reply[0] != 'ok':
            raise DiscoError("Failed to start a job. Server replied: " + reply)
        self.name = reply[1]
        return self

def result_iterator(results,
                    notifier=None,
                    reader=func.chain_reader,
                    input_stream=(func.map_input_stream, ),
                    params=None,
                    ddfs=None,
                    tempdir=None):
    """
    Iterates the key-value pairs in job results. *results* is a list of
    results, as returned by :meth:`Disco.wait`.

    :param notifier: a function called when the iterator moves to the
                     next result file::

                      def notifier(url):
                          ...

                     *url* may be a list if results are replicated.

    :param reader: a custom reader function.
                   Specify this to match with a custom *map_writer* or *reduce_writer*.
                   By default, *reader* is :func:`disco.func.netstr_reader`.

    :param tempdir: if results are replicated, *result_iterator* ensures that only
                    valid replicas are used. By default, this is done by downloading
                    and parsing results first to a temporary file. If the temporary
                    file was created succesfully, the results are returned,
                    otherwise an alternative replica is used.

                    If *tempdir=None* (default), the system default temporary
                    directory is used (typically ``/tmp``). An alternative path
                    can be set with *tempdir="path"*. Temporary files can be disabled
                    with *tempdir=False*, in which case results are read in memory.
    """
    from disco.task import Task
    task = Task()
    task.params = params
    task.input_stream = list(input_stream)
    if reader:
        task.input_stream.append(func.reader_wrapper(reader))
    task.insert_globals(task.input_stream)
    for result in results:
        for url in util.urllist(result, ddfs=ddfs):
            if notifier:
                notifier(url)
            if type(url) == list:
                iter = process_url_safe(url, tempdir, task)
            else:
                iter, sze, url = task.connect_input(url)
            for x in iter:
                yield x

def process_url_safe(urls, tempdir, task):
    while urls:
        try:
            in_stream, sze, url = task.connect_input(urls[0])
            return list(in_stream) if tempdir == False\
                else disk_buffer(tempdir, in_stream)
        except:
            urls = urls[1:]
            if not urls:
                raise

def disk_buffer(tempdir, in_stream):
    fd = NamedTemporaryFile(prefix='discores-', dir=tempdir)
    n = util.ilen(marshal.dump(x, fd.file) for x in in_stream)
    fd.seek(0)
    return (marshal.load(fd.file) for i in range(n))

class Stats(object):
    def __init__(self, prof_data):
        self.stats = marshal.loads(prof_data)

    def create_stats(self):
        pass
