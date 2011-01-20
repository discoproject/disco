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
.. autofunction:: result_iterator
"""
import os, time

from disco import func, json, util
from disco.comm import download
from disco.error import DiscoError, JobError, CommError
from disco.eventmonitor import EventMonitor
from disco.fileutils import Chunker, CHUNK_SIZE
from disco.job import Job, JobDict
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

    def request(self, url, data=None, offset=0):
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
                            offset=offset)
        except CommError, e:
            if e.code == None:
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

        See :mod:`disco.worker.classic.worker` for more information on using OOB.
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

        This method accepts the same set of keyword args as :meth:`Job.run`.
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
        return JobDict.unpack(self.jobpack(name))

    def jobpack(self, name):
        return self.request('/disco/ctrl/parameters?name=%s' % name)

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
            lines = events.split('\n')
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
        try:
            status, results = self.results(name, timeout=poll_interval)
        except CommError, e:
            status = 'active'
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

class ChunkIter(object):
    def __init__(self, url,
                 chunk_size=CHUNK_SIZE,
                 reader=None,
                 **kwargs):
        self.url = url
        self.chunk_size= chunk_size
        self.kwargs = kwargs
        self.kwargs.update(dict(reader=reader))

    def __iter__(self):
        chunker = Chunker(chunk_size=self.chunk_size)
        for chunk in chunker.chunks(RecordIter([self.url], **self.kwargs)):
            yield chunk

class RecordIter(object):
    """
    Produces an iterator over the records in a list of inputs.

    :type  urls: list of urls
    :param urls: urls of the inputs
                 e.g. as returned by :meth:`Disco.wait`.

    :type  notifier: function
    :param notifier: called when the iterator moves to the next url::

                      def notifier(url[s]):
                          ...

                     .. note::

                         notifier argument is a list if urls are replicated.

    :type  reader: :func:`disco.func.input_stream`
    :param reader: used to read from a custom :func:`disco.func.output_stream`.
    """
    def __init__(self, urls,
                 notifier=func.noop,
                 reader=func.chain_reader,
                 input_stream=(func.map_input_stream, ),
                 params=None,
                 ddfs=None):
        from disco.worker.classic.task import Map
        self.task = Map(jobdict=JobDict(map_input_stream=input_stream,
                                        map_reader=reader,
                                        params=params))
        self.urls = urls
        self.notifier = notifier
        self.ddfs = ddfs

    def __iter__(self):
        for urls in self.urls:
            for replicas in util.urllist(urls, ddfs=self.ddfs):
                self.notifier(replicas)
                for entry in self.try_replicas(list(util.iterify(replicas))):
                    yield entry

    def try_replicas(self, urls, start=0):
        while urls:
            try:
                for entry in self.entries(urls.pop(0), start=start):
                    yield entry
                    start += 1
            except Exception:
                if not urls:
                    raise

    def entries(self, url, start=0):
        fd, _size, _url = self.task.connect_input(url)
        for n, entry in enumerate(fd):
            if n >= start:
                yield entry

def result_iterator(*args, **kwargs):
    return RecordIter(*args, **kwargs)

class Stats(object):
    def __init__(self, prof_data):
        from marshal import loads
        self.stats = loads(prof_data)

    def create_stats(self):
        pass
