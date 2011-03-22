"""
:mod:`disco.core` -- Disco Core Library
=======================================

The :mod:`disco.core` module provides a high-level interface for
communication with the Disco master. It provides functions for submitting
new jobs, querying the status of the system, and getting results of jobs.
"""
import os, time

from disco import func, json, util
from disco.comm import download
from disco.error import DiscoError, JobError, CommError
from disco.eventmonitor import EventMonitor
from disco.fileutils import Chunker, CHUNK_SIZE
from disco.job import Job
from disco.settings import DiscoSettings

from disco.worker.classic.worker import Params # backwards compatibility XXX: deprecate

class Continue(Exception):
    pass

class Disco(object):
    """
    The :class:`Disco` object provides an interface to the Disco master.
    It can be used to query the status of the system, or to submit a new job.
    See the :mod:`disco.job` module for more information about constructing jobs.

    :meth:`Disco.new_job` is provided with all information needed to run
    a job, which it packages and sends to the master. The method returns
    immediately and returns a :class:`Job` object that corresponds to the
    newly started job.

    :type  master: url
    :param master: address of the Disco master, e.g. ``disco://localhost``.
    """
    def __init__(self, master=None, settings=None):
        self.settings = settings or DiscoSettings()
        self.master = master or self.settings['DISCO_MASTER']

    def __repr__(self):
        return 'Disco master at %s' % self.master

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
        urls = self.ddfs.get(self.ddfs.job_oob(name))['urls']
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
        Submits a new job request to the master using the classic interface.

        This method accepts the same set of keyword args as
        :meth:`disco.worker.classic.job.Job.run`.
        Returns a :class:`disco.worker.classic.job.Job`.
        """
        return Job(name=name, master=self.master).run(**kwargs)

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
        return self.jobpack(name).jobdict

    def jobpack(self, name):
        from cStringIO import StringIO
        from disco.job import JobPack
        return JobPack.load(StringIO(self.request('/disco/ctrl/parameters?name=%s' % name)))

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
        def jobname(job):
            if isinstance(job, basestring):
                return job
            elif isinstance(job, list):
                return job[0]
            return job.name
        jobnames = [jobname(job) for job in util.iterify(jobspec)]
        data = json.dumps([timeout, jobnames])
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

    def wait(self, name, poll_interval=2, timeout=None, clean=False, show=None):
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
        if show is None:
            show = self.settings['DISCO_EVENTS']
        event_monitor = EventMonitor(Job(name=name, master=self.master),
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
        for chunk in chunker.chunks(classic_iterator([self.url], **self.kwargs)):
            yield chunk

def classic_iterator(urls,
                     reader=func.chain_reader,
                     input_stream=(func.map_input_stream, ),
                     notifier=func.notifier,
                     params=None,
                     ddfs=None):
    """
    An iterator over records as seen by the classic map interface.

    :type  reader: :func:`disco.classic.worker.func.input_stream`
    :param reader: shortcut for the last input stream applied.

    :type  input_stream: sequence of :func:`disco.classic.worker.func.input_stream`
    :param input_stream: used to read from a custom file format.

    :type  notifier: :func:`disco.classic.worker.func.notifier`
    :param notifier: called when the task opens a url.
    """
    from disco.task import TaskInput
    from disco.worker.classic.worker import Worker
    worker = Worker(map_reader=reader, map_input_stream=input_stream)
    settings = DiscoSettings(DISCO_MASTER=ddfs) if ddfs else DiscoSettings()
    for input in util.inputlist(urls, settings=settings):
        notifier(input)
        for record in TaskInput(input, open=worker.opener('map', 'in', params)):
            yield record

def result_iterator(*args, **kwargs):
    """Backwards compatible alias for :func:`classic_iterator`"""
    return classic_iterator(*args, **kwargs)

class Stats(object):
    def __init__(self, prof_data):
        from marshal import loads
        self.stats = loads(prof_data)

    def create_stats(self):
        pass
