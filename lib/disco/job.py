"""
:mod:`disco.job` -- Disco Jobs
==============================

This module contains the core objects for creating and interacting with Disco jobs.
Often, :class:`Job` is the only thing you need
in order to start running distributed computations with Disco.

:term:`Jobs <job>` in Disco are used to encapsulate and schedule computation pipelines.
A job specifies a :term:`worker`, the worker environment, a list of inputs,
and some additional information about how to run the job.
For a full explanation of how the job is specified to the Disco :term:`master`,
see :ref:`jobpack`.

A typical pattern in Disco scripts is to run a job synchronously,
that is, to block the script until the job has finished.
This can be accomplished using the :meth:`Job.wait` method::

        from disco.job import Job
        results = Job(name).run(**jobargs).wait()
"""
import os, sys, time

from disco import func, json, task, util
from disco.error import JobError
from disco.util import hexhash, isiterable, load_oob, save_oob
from disco.settings import DiscoSettings

class Job(object):
    """
    Creates a Disco Job with the given name, master, worker, and settings.
    Use :meth:`Job.run` to start the job.

    :type  name: string
    :param name: the job name.
                 When you create a handle for an existing job, the name is used as given.
                 When you create a new job, the name given is used as the
                 :attr:`jobdict.prefix` to construct a unique name,
                 which is then stored in the instance.

    :type  master: url of master or :class:`disco.core.Disco`
    :param master: the Disco master to use for submitting or querying the job.

    :type  worker: :class:`disco.worker.Worker`
    :param worker: the worker instance used to create and run the job.
                   If none is specified, the job creates a worker using
                   its :attr:`Job.Worker` attribute.

    :type settings: :class:`disco.settings.DiscoSettings`

    .. attribute:: Worker

                Defaults to :class:`disco.worker.classic.worker.Worker`.
                If no `worker` parameter is specified,
                :attr:`Worker` is called with no arguments to construct the :attr:`worker`.
    """
    from disco.worker.classic.worker import Worker
    proxy_functions = ('clean',
                       'events',
                       'kill',
                       'jobinfo',
                       'jobpack',
                       'oob_get',
                       'oob_list',
                       'profile_stats',
                       'purge',
                       'results',
                       'mapresults',
                       'wait')
    """
    These methods from :class:`disco.core.Disco`,
    which take a jobname as the first argument,
    are also accessible through the :class:`Job` object:

        - :meth:`disco.core.Disco.clean`
        - :meth:`disco.core.Disco.events`
        - :meth:`disco.core.Disco.kill`
        - :meth:`disco.core.Disco.jobinfo`
        - :meth:`disco.core.Disco.jobpack`
        - :meth:`disco.core.Disco.oob_get`
        - :meth:`disco.core.Disco.oob_list`
        - :meth:`disco.core.Disco.profile_stats`
        - :meth:`disco.core.Disco.purge`
        - :meth:`disco.core.Disco.results`
        - :meth:`disco.core.Disco.wait`

    For instance, you can use `job.wait()` instead of `disco.wait(job.name)`.
    The job methods in :class:`disco.core.Disco` come in handy if you want to manipulate
    a job that is identified by a jobname instead of a :class:`Job` object.
    """

    def __init__(self, name=None, master=None, worker=None, settings=None):
        from disco.core import Disco
        self.name = name or type(self).__name__
        self.disco = master if isinstance(master, Disco) else Disco(master)
        self.worker = worker or self.Worker()
        self.settings = settings or DiscoSettings()

    def __getattr__(self, attr):
        if attr in self.proxy_functions:
            from functools import partial
            return partial(getattr(self.disco, attr), self.name)
        raise AttributeError("%r has no attribute %r" % (self, attr))

    def run(self, **jobargs):
        """
        Creates the :class:`JobPack` for the worker using
        :meth:`disco.worker.Worker.jobdict`,
        :meth:`disco.worker.Worker.jobenvs`,
        :meth:`disco.worker.Worker.jobhome`,
        :meth:`disco.task.jobdata`,
        and attempts to submit it.

        :type  jobargs: dict
        :param jobargs: runtime parameters for the job.
                        Passed to the :class:`disco.worker.Worker`
                        methods listed above, along with the job itself.

        :raises: :class:`disco.error.JobError` if the submission fails.
        :return: the :class:`Job`, with a unique name assigned by the master.
        """
        jobpack = JobPack(self.worker.jobdict(self, **jobargs),
                          self.worker.jobenvs(self, **jobargs),
                          self.worker.jobhome(self, **jobargs),
                          task.jobdata(self, jobargs))
        self.name = self.disco.submit(jobpack.dumps())
        return self

class SimpleJob(Job):
    from disco.worker.simple import Worker

class JobChain(dict):
    def wait(self, poll_interval=1):
        while sum(self.walk()) < len(self):
            time.sleep(poll_interval)
        return self

    def walk(self):
        for job in self:
            status, results = job.results()
            if status == 'unknown job':
                input = util.chainify(self.inputs(job))
                if not any(i is None for i in input):
                    job.run(input=input)
            elif status == 'active':
                pass
            elif status == 'ready':
                yield 1
            else:
                raise JobError(job, "Status %s" % status)

    def inputs(self, job):
        for input in util.iterify(self[job]):
            if isinstance(input, Job):
                status, results = input.results()
                if status in ('unknown job', 'active'):
                    yield [None]
                elif status == 'ready':
                    yield results
                else:
                    raise JobError(input, "Status %s" % status)
            else:
                yield [input]

    def purge(self):
        for job in self:
            job.purge()

class JobPack(object):
    """
    This class implements :ref:`jobpack` in Python.
    The attributes correspond to the fields in the :term:`job pack` file.
    Use :meth:`dumps` to serialize the :class:`JobPack` for sending to the master.

    .. attribute:: jobdict

                   The dictionary of job parameters for the :term:`master`.

                   See also :ref:`jobdict`.

    .. attribute:: jobenvs

                   The dictionary of environment variables to set before the
                   :term:`worker` is run.

                   See also :ref:`jobenvs`.

    .. attribute:: jobhome

                   The zipped archive to use when initializing the :term:`job home`.
                   This field should contain the contents of the serialized archive.

                   See also :ref:`jobhome`.

    .. attribute:: jobdata

                   Binary data that the builtin :class:`disco.worker.Worker`
                   uses for serializing itself.

                   See also :ref:`jobdata`.
    """
    MAGIC = (0xd5c0 << 16) + 0x0001
    HEADER_FORMAT = "!IIIII"
    HEADER_SIZE = 128
    def __init__(self, jobdict, jobenvs, jobhome, jobdata):
        self.jobdict = jobdict
        self.jobenvs = jobenvs
        self.jobhome = jobhome
        self.jobdata = jobdata

    @classmethod
    def header(self, offsets, magic=MAGIC, format=HEADER_FORMAT, size=HEADER_SIZE):
        from struct import pack
        toc = pack(format, magic, *(o for o in offsets))
        return toc + '\0' * (size - len(toc))

    def contents(self, offset=HEADER_SIZE):
        for field in (json.dumps(self.jobdict),
                      json.dumps(self.jobenvs),
                      self.jobhome,
                      self.jobdata):
            yield offset, field
            offset += len(field)

    def dumps(self):
        """
        Return the serialized :class:`JobPack`.

        Essentially encodes the :attr:`jobdict` and :attr:`jobenvs` dictionaries,
        and prepends a valid header.
        """
        offsets, fields = zip(*self.contents())
        return self.header(offsets) + ''.join(fields)

    @classmethod
    def offsets(cls, jobfile, magic=MAGIC, format=HEADER_FORMAT, size=HEADER_SIZE):
        from struct import calcsize, unpack
        jobfile.seek(0)
        header = [i for i in unpack(format, jobfile.read(calcsize(format)))]
        assert header[0] == magic, "Invalid jobpack magic."
        assert header[1] == size, "Invalid jobpack header."
        assert header[1:] == sorted(header[1:]), "Invalid jobpack offsets."
        return header[1:]

    @classmethod
    def load(cls, jobfile):
        """Load a :class:`JobPack` from a file."""
        return PackedJobPack(jobfile)

class PackedJobPack(JobPack):
    def __init__(self, jobfile):
        self.jobfile = jobfile

    @property
    def jobdict(self):
        dict_offset, envs_offset, home_offset, data_offset = self.offsets(self.jobfile)
        self.jobfile.seek(dict_offset)
        return json.loads(self.jobfile.read(envs_offset - dict_offset))

    @property
    def jobenvs(self):
        dict_offset, envs_offset, home_offset, data_offset = self.offsets(self.jobfile)
        self.jobfile.seek(envs_offset)
        return json.loads(self.jobfile.read(home_offset - envs_offset))

    @property
    def jobhome(self):
        dict_offset, envs_offset, home_offset, data_offset = self.offsets(self.jobfile)
        self.jobfile.seek(home_offset)
        return self.jobfile.read(data_offset - home_offset)

    @property
    def jobdata(self):
        dict_offset, envs_offset, home_offset, data_offset = self.offsets(self.jobfile)
        self.jobfile.seek(data_offset)
        return self.jobfile.read()
