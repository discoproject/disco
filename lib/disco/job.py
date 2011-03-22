"""
:mod:`disco.job` -- Disco Jobs
==============================


XXX: fix these docs
    :meth:`Disco.new_job` and :meth:`Job.run`
    accept the same set of keyword arguments as specified below.

    :type  input: list of inputs or list of list of inputs
    :param input: Each input must be specified in one of the following ways:

                   * ``http://www.example.com/data`` - any HTTP address
                   * ``disco://cnode03/bigtxt/file_name`` - Disco address. Refers to ``cnode03:/var/disco/bigtxt/file_name``. Currently this is an alias for ``http://cnode03:[DISCO_PORT]/bigtxt/file_name``.
                   * ``dir://cnode03/jobname/`` - Result directory. This format is used by Disco internally.
                   * ``/home/bob/bigfile.txt`` - a local file. Note that the file must either exist on all the nodes or you must make sure that the job is run only on the nodes where the file exists. Due to these restrictions, this form has only limited use.
                   * ``raw://some_string`` - pseudo-address; instead of fetching data from a remote source, use ``some_string`` in the address as data. Useful for specifying dummy inputs for generator maps.
                   * ``tag://tagname`` - a tag stored in :ref:`DDFS` (*Added in version 0.3*)

                  (*Added in version 0.3.2*)
                  Tags can be token protected.
                  For the data in tags to be used as job inputs,
                  the tags should be resolved into the constituent urls or replica sets,
                  and provided as the value of the input parameter.

                  (*Added in version 0.2.2*):
                  An input entry can be a list of inputs:
                  This lets you specify redundant versions of an input file.
                  If a list of redundant inputs is specified,
                  the scheduler chooses the input that is located on the node
                  with the lowest load at the time of scheduling.
                  Redundant inputs are tried one by one until the task succeeds.
                  Redundant inputs require that the *map* function is specified.

"""
import os, sys, time

from disco import func, json
from disco.error import DiscoError
from disco.util import hexhash, isiterable, netloc, load_oob, save_oob
from disco.settings import DiscoSettings

class JobPack(object):
    """
    +---------------- 4
    | magic / version |
    +---------------- 8 -------------- 12 ------------- 16 ------------- 20
    | jobdict offset  | jobenvs offset | jobhome offset | jobdata offset |
    +--------------------------------------------------------------------+
    |                         ...  reserved ...                          |
    128 -----------------------------------------------------------------+
    |                               jobdict                              |
    +--------------------------------------------------------------------+
    |                               jobenvs                              |
    +--------------------------------------------------------------------+
    |                               jobhome                              |
    +--------------------------------------------------------------------+
    |                               jobdata                              |
    +--------------------------------------------------------------------+
    """
    MAGIC = (0xd5c0 << 16) + 0x0001
    HEADER_FORMAT = "IIIII"
    HEADER_SIZE = 128
    def __init__(self, *fields):
        self.jobdict, self.jobenvs, self.jobhome, self.jobdata = fields

    def header(self, offsets, magic=MAGIC, format=HEADER_FORMAT, size=HEADER_SIZE):
        from socket import htonl
        from struct import pack
        toc = pack(format, htonl(magic), *(htonl(o) for o in offsets))
        return toc + '\0' * (size - len(toc))

    def contents(self, offset=HEADER_SIZE):
        for field in (json.dumps(self.jobdict),
                      json.dumps(self.jobenvs),
                      self.jobhome,
                      self.jobdata):
            yield offset, field
            offset += len(field)

    def dumps(self):
        offsets, fields = zip(*self.contents())
        return self.header(offsets) + ''.join(fields)

    @classmethod
    def offsets(cls, jobfile, magic=MAGIC, format=HEADER_FORMAT, size=HEADER_SIZE):
        from socket import ntohl
        from struct import calcsize, unpack
        jobfile.seek(0)
        header = [ntohl(i) for i in unpack(format, jobfile.read(calcsize(format)))]
        if header[0] != magic:
            raise DiscoError("Invalid jobpack magic.")
        if header[1] != size:
            raise DiscoError("Invalid jobpack header.")
        if header[1:] != sorted(header[1:]):
            raise DiscoError("Invalid jobpack offsets.")
        return header[1:]

    @classmethod
    def load(cls, jobfile):
        return PackedJobPack(jobfile)

    @classmethod
    def request(cls):
        from disco.events import JobFile
        return cls.load(open(JobFile().send()))

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
        return json.loads(self.jobfile.read(home_offset - dict_offset))

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

class Job(object):
    """
    Creates a Disco Job with the given master, name, worker, and settings.
    Returns the job immediately after the request has been submitted.

    :type  master: :class:`disco.core.Disco`
    :param master: Identifies the Disco master runs this job.

    :type  name: string
    :param name: The job name.
                 When you create a handle for an existing job, the name is used as given.
                 When you create a new job, the name given is used by Disco as a
                 prefix to construct a unique name, which is then stored in the instance.

                 .. note::

                        Only characters in ``[a-zA-Z0-9_]`` are allowed in the job name.

    :type worker: :class:`disco.worker.Worker`

    :type settings: :class:`disco.settings.DiscoSettings`

    Use :meth:`Job.run` to start the job.

    A typical pattern in Disco scripts is to run a job synchronously,
    that is, to block the script until the job has finished.
    This is accomplished as follows::

        from disco.job import Job
        results = Job(name, ...).run().wait()

    Note that job methods of :class:`disco.core.Disco` objects are directly
    accessible through the :class:`Job` object, such as :meth:`Job.wait`
    above.

    A :class:`JobError` is raised if an error occurs while starting the job.

    The following methods from :class:`disco.core.Disco`,
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

    If you have access only to results of a job, you can extract the job
    name from an address with the :func:`disco.util.jobname` function.
    A typical case is that you no longer need the results of a job.
    You can delete the unneeded job files as follows::

        from disco.job import Job
        from disco.util import jobname

        Job(master, jobname(results[0])).purge()
    """
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
    from disco.worker.classic.worker import Worker as worker

    def __init__(self, name=None, master=None, worker=None, settings=DiscoSettings()):
        from disco.core import Disco
        self.name = name or type(self).__name__
        self.disco = master if isinstance(master, Disco) else Disco(master)
        self.worker = worker or self.worker()
        self.settings = settings

    def __getattr__(self, attr):
        if attr in self.proxy_functions:
            from functools import partial
            return partial(getattr(self.disco, attr), self.name)
        raise AttributeError("%r has no attribute %r" % (self, attr))

    def run(self, **jobargs):
        jobpack = JobPack(self.worker.jobdict(self, **jobargs),
                          self.worker.jobenvs(self, **jobargs),
                          self.worker.jobhome(self, **jobargs),
                          self.worker.jobdata(self, **jobargs))
        status, response = json.loads(self.disco.request('/disco/job/new',
                                                         jobpack.dumps()))
        if status != 'ok':
            raise DiscoError("Failed to start job. Server replied: %s" % response)
        self.name = response
        return self
