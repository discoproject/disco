"""
:mod:`disco.core` -- Disco Core Library
=======================================

The :mod:`disco.core` module provides a high-level interface for
communication with the Disco master. It provides functions for submitting
new jobs, querying the status of the system, and getting results of jobs.
"""
import os, time, sys

from disco import func, json, util
from disco.comm import download
from disco.error import DiscoError, JobError, CommError
from disco.eventmonitor import EventMonitor
from disco.job import Job
from disco.settings import DiscoSettings
from disco.util import proxy_url
from disco.worker.classic.worker import Params # backwards compatibility XXX: deprecate

class Continue(Exception):
    pass

class Disco(object):
    """
    The :class:`Disco` object provides an interface to the Disco master.
    It can be used to query the status of the system, or to submit a new job.

    :type  master: url
    :param master: address of the Disco master, e.g. ``disco://localhost``.

    .. seealso:: :meth:`Disco.new_job` and :mod:`disco.job`
       for more on creating jobs.
    """
    def __init__(self, master=None, settings=None, proxy=None):
        self.settings = settings or DiscoSettings()
        self.proxy  = proxy or self.settings['DISCO_PROXY']
        self.master = master or self.settings['DISCO_MASTER']

    def __repr__(self):
        return 'Disco master at %s' % self.master

    def request(self, url, data=None, offset=0):
        try:
            return download(proxy_url('%s%s' % (self.master, url), proxy=self.proxy),
                            data=data,
                            offset=offset)
        except CommError, e:
            if e.code == None:
                e.msg += " (is disco master running at %s?)" % self.master
            raise

    def submit(self, jobpack):
        status, body = json.loads(self.request('/disco/job/new', jobpack))
        if status != 'ok':
            raise DiscoError("Failed to submit jobpack: %s" % body)
        return body

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

        .. versionadded:: 0.2.4
        """
        self.request('/disco/ctrl/blacklist', '"%s"' % node)

    def whitelist(self, node):
        """
        Whitelists *node* so that the master may submit tasks to it.

        .. versionadded:: 0.2.4
        """
        self.request('/disco/ctrl/whitelist', '"%s"' % node)

    def oob_get(self, jobname, key):
        """
        Returns an out-of-band value assigned to *key* for the job.

        OOB data can be stored and retrieved for job tasks using
        :meth:`disco.task.Task.get` and :meth:`disco.task.Task.put`.
        """
        try:
            return util.load_oob(self.master, jobname, key)
        except CommError, e:
            if e.code == 404:
                raise DiscoError("Unknown key or jobname")
            raise

    def oob_list(self, jobname):
        """
        Returns all out-of-band keys for the job.

        .. seealso:: :ref:`oob`
        """
        urls = self.ddfs.get(self.ddfs.job_oob(jobname))['urls']
        return list(set(self.ddfs.blob_name(replicas[0])
                        for replicas in urls))

    def profile_stats(self, jobname, mode='', stream=sys.stdout):
        """
        Returns results of job profiling.
        :ref:`jobdict` must have had the ``profile`` flag enabled.

        :type  mode: 'map' or 'reduce' or ''
        :param mode: restricts results to the map or reduce phase, or not.

        :type  stream: file-like object
        :param stream: alternate output stream.
                       See the `pstats.Stats constructor <http://docs.python.org/library/profile.html#pstats.Stats>`_.

        The function returns a `pstats.Stats object <http://docs.python.org/library/profile.html#the-stats-class>`_.
        For instance, you can print out results as follows::

                job.profile_stats().sort_stats('cumulative').print_stats()

        .. versionadded:: 0.2.1
        """
        prefix = 'profile-%s' % mode
        f = [s for s in self.oob_list(jobname) if s.startswith(prefix)]
        if not f:
            raise JobError(Job(name=jobname, master=self), "No profile data")

        import pstats
        stats = pstats.Stats(Stats(self.oob_get(jobname, f[0])), stream=stream)
        for s in f[1:]:
            stats.add(Stats(self.oob_get(jobname, s)))
        stats.strip_dirs()
        stats.sort_stats('cumulative')
        return stats

    def new_job(self, name, **jobargs):
        """
        Submits a new job request to the master using :class:`disco.job.Job`::

                return Job(name=name, master=self.master).run(**jobargs)
        """
        return Job(name=name, master=self.master).run(**jobargs)

    def kill(self, jobname):
        """Kills the job."""
        self.request('/disco/ctrl/kill_job', '"%s"' % jobname)

    def clean(self, jobname):
        """
        Deletes job metadata.

        .. deprecated:: 0.4
                Use :meth:`Disco.purge` to delete job results,
                deleting job metadata only is strongly discouraged.

        .. note:: After the job has been cleaned,
                  there is no way to obtain the result urls from the master.
                  However, no data is actually deleted by :meth:`Disco.clean`,
                  in contrast to :meth:`Disco.purge`.
        """
        self.request('/disco/ctrl/clean_job', '"%s"' % jobname)

    def purge(self, jobname):
        """Deletes all metadata and data related to the job."""
        self.request('/disco/ctrl/purge_job', '"%s"' % jobname)

    def jobpack(self, jobname):
        """Return the :class:`disco.job.JobPack` submitted for the job."""
        from cStringIO import StringIO
        from disco.job import JobPack
        return JobPack.load(StringIO(self.request('/disco/ctrl/parameters?name=%s' % jobname)))

    def events(self, jobname, offset=0):
        """
        Returns an iterator that iterates over job events, ordered by time.
        It is safe to call this function while the job is running,
        thus it provides an efficient way to monitor job events continuously.
        The iterator yields tuples ``offset, event``.

        :type  offset: int
        :param offset: skip events that occurred before this *offset*

        .. versionadded:: 0.2.3

        .. seealso:: :envvar:`DISCO_EVENTS`
           for information on how to enable the console output of job events.
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
        return event_iter(self.rawevents(jobname, offset=offset))

    def rawevents(self, jobname, offset=0):
        return self.request("/disco/ctrl/rawevents?name=%s" % jobname,
                            offset=offset)

    def mapresults(self, jobname):
        return json.loads(
            self.request('/disco/ctrl/get_mapresults?name=%s' % jobname))

    def results(self, jobspec, timeout=2000):
        """
        Returns a list of results for a single job or for many
        concurrently running jobs, depending on the type of *jobspec*.

        :type  jobspec: :class:`disco.job.Job`, string, or list
        :param jobspec: If a job or job name is provided,
                        return a tuple which looks like::

                                status, results

                        If a list is provided,
                        return two lists: inactive jobs and active jobs.
                        Both the lists contain elements of the following type::

                                jobname, (status, results)

                        where status is one of:
                        ``'unknown job'``,
                        ``'dead'``,
                        ``'active'``, or
                        ``'ready'``.

        :type  timeout: int
        :param timeout: wait at most this many milliseconds,
                        for at least one on the jobs to finish.

        Using a list of jobs is a more efficient way to wait
        for multiple jobs to finish.
        Consider the following example that prints out results
        as soon as the jobs (initially ``active``) finish::

                while active:
                  inactive, active = disco.results(jobs)
                  for jobname, (status, results) in inactive:
                    if status == 'ready':
                      for k, v in result_iterator(results):
                        print k, v
                      disco.purge(jobname)

        Note how the list of active jobs, ``active``,
        returned by :meth:`Disco.results`,
        can be used as the input to this function as well.
        """
        def jobname(job):
            if isinstance(job, Job):
                return job.name
            elif isinstance(job, basestring):
                return job
            return job[0]
        jobnames = [jobname(job) for job in util.iterify(jobspec)]
        results = json.loads(self.request('/disco/ctrl/get_results',
                                          json.dumps([timeout, jobnames])))
        others, active = [], []
        for jobname, (status, result) in results:
            if isinstance(jobspec, (Job, basestring)):
                return status, result
            elif status == 'active':
                active.append((jobname, (status, result)))
            else:
                others.append((jobname, (status, result)))
        return others, active

    def jobinfo(self, jobname):
        """Returns a dict containing information about the job."""
        return json.loads(self.request('/disco/ctrl/jobinfo?name=%s' % jobname))

    def check_results(self, jobname, start_time, timeout, poll_interval):
        try:
            status, results = self.results(jobname, timeout=poll_interval)
        except CommError, e:
            status = 'active'
        if status == 'ready':
            return results
        if status != 'active':
            raise JobError(Job(name=jobname, master=self), "Status %s" % status)
        if timeout and time.time() - start_time > timeout:
            raise JobError(Job(name=jobname, master=self), "Timeout")
        raise Continue()

    def wait(self, jobname, poll_interval=2, timeout=None, clean=False, show=None):
        """
        Block until the job has finished.
        Returns a list of the result urls.

        :type  poll_interval: int
        :param poll_interval: the number of seconds between job status requests.

        :type  timeout: int or None
        :param timeout: if specified, the number of seconds before returning or
                        raising a :class:`disco.JobError`.

        :type  clean: bool
        :param clean: if `True`,
                      call :meth:`Disco.clean` when the job has finished.

                      .. deprecated:: 0.4

        :type  show: bool or string
        :param show: enables console output of job events.
                     The default is provided by :envvar:`DISCO_EVENTS`.

                     .. versionadded:: 0.2.3
        """
        if show is None:
            show = self.settings['DISCO_EVENTS']
        event_monitor = EventMonitor(Job(name=jobname, master=self.master),
                                     format=show,
                                     poll_interval=poll_interval)
        start_time    = time.time()
        while True:
            event_monitor.refresh()
            try:
                return self.check_results(jobname, start_time,
                                          timeout, poll_interval * 1000)
            except Continue:
                continue
            finally:
                if clean:
                    self.clean(jobname)
                event_monitor.refresh()

    def result_iterator(self, *args, **kwargs):
        kwargs['ddfs'] = self.master
        return result_iterator(*args, **kwargs)

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
    from disco.worker import Input
    from disco.worker.classic.worker import Worker
    worker = Worker(map_reader=reader, map_input_stream=input_stream)
    settings = DiscoSettings(DISCO_MASTER=ddfs) if ddfs else DiscoSettings()
    for input in util.inputlist(urls, settings=settings):
        if isinstance(input, basestring):
            dest = proxy_url(input, to_master=False)
        elif isinstance(input, tuple):
            dest = tuple([proxy_url(i, to_master=False) for i in input])
        else:
            dest = [proxy_url(i, to_master=False) for i in input]
        notifier(dest)
        for record in Input(dest, open=worker.opener('map', 'in', params)):
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
