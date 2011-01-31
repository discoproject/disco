"""
:mod:`disco.job` --- Disco Jobs
===============================

.. autoclass:: Task
        :members:
.. autoclass:: JobDict
        :members:
.. autoclass:: Job
        :members:
"""
import os, sys, time

from disco import dencode, func, json
from disco.error import DiscoError
from disco.util import hexhash, isiterable, netloc, load_oob, save_oob
from disco.settings import DiscoSettings

class Task(object):
    def __init__(self,
                 worker,
                 jobdict,
                 jobname='',
                 taskid=-1,
                 mode=None,
                 host=''):
        self.worker = worker
        self.jobdict = jobdict
        self.jobname = jobname
        self.taskid = taskid
        self.mode = mode
        self.host = host
        self.uid = '%s:%s-%s-%x' % (mode,
                                    taskid,
                                    hexhash(str((time.time()))),
                                    os.getpid())

    @property
    def jobpath(self):
        return os.path.join(self.host, hexhash(self.jobname), self.jobname)

    @property
    def master(self):
        return self.jobdict.settings['DISCO_MASTER']

    @property
    def taskpath(self):
        return os.path.join(hexhash(self.uid), self.uid)

    def path(self, name):
        return os.path.join(self.taskpath, name)

    def url(self, name, scheme='disco'):
        return '%s://%s/disco/%s/%s/%s' % (scheme, self.host, self.jobpath, self.taskpath, name)

    def open_url(self, url):
        from disco.comm import open_url
        file, _size, _url = open_url(url)
        return file

    def open_replicas(self, input, start=0):
        from itertools import dropwhile
        replicas = list(input)
        while replicas:
            try:
                records = enumerate(self.open_url(replicas.pop(0)))
                for start, record in dropwhile(lambda (n, rec): n < start, records):
                    yield record
            except Exception:
                start += 1
                if not replicas:
                    raise

    def open(self, input):
        if isiterable(input):
            return self.open_replicas(input)
        return self.open_url(input)

    def read(self, *inputs):
        for input in inputs:
            for record in self.open(input):
                yield record

    def start(self, *inputs):
        if self.jobdict['profile?']:
            return self.run_profile(*inputs)
        return self.run(*inputs)

    def run(self, *inputs):
        return self.read(*inputs)

    def run_profile(self, *inputs):
        from cProfile import runctx
        name = 'profile-%s' % self.uid
        path = self.path(name)
        runctx('self.run(*inputs)', globals(), locals(), path)
        self.put(name, open(path).read())

    def get(self, key):
        """
        Gets an out-of-band result for the task with the key *key*.

        Given the semantics of OOB results, this means that only the reduce
        phase can access results produced in the preceding map phase.
        """
        return load_oob(self.master, self.jobname, key)

    def put(self, key, value):
        """
        Stores an out-of-band result *value* with the key *key*.

        Key must be unique in this job.
        Maximum key length is 256 characters.
        Only characters in the set ``[a-zA-Z_\-:0-9@]`` are allowed in the key.
        """
        from disco.ddfs import DDFS
        if DDFS.safe_name(key) != key:
            raise DiscoError("OOB key contains invalid characters (%s)" % key)
        save_oob(self.master, self.jobname, key, value)

class JobDict(dict):
    """
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

    def __init__(self, settings, **jobargs):
        self.settings = settings
        super(JobDict, self).__init__(self.defaults())
        self.update(jobargs)

    def copy(self):
        return type(self)(self.settings, **super(JobDict, self).copy())

    def defaults(self):
        return {'input': (),
                'jobhome': '',
                'worker': self.settings['DISCO_WORKER'],
                'map?': False,
                'reduce?': False,
                'profile?': False,
                'nr_reduces': 0,
                'prefix': '',
                'scheduler': {'force_local': False,
                              'force_remote': False,
                              'max_cores': int(2**31),},
                'owner': self.settings['DISCO_JOB_OWNER']}

    def dumps(self):
        """Pack up the :class:`JobDict` for sending over the wire."""
        return dencode.dumps(self)

    @classmethod
    def load(cls, jobfile, settings=DiscoSettings()):
        """Unpack the previously packed :class:`JobDict` from a file."""
        return cls(settings, **dencode.load(jobfile))

    @classmethod
    def loads(cls, jobpack, settings=DiscoSettings()):
        return cls(settings, **dencode.loads(jobpack))

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

    :type worker: :class:`disco.core.Worker`

    :type settings: :class:`disco.settings.DiscoSettings`

    Use :meth:`Job.run` to start the job.

    A typical pattern in Disco scripts is to run a job synchronously,
    that is, to block the script until the job has finished.
    This is accomplished as follows::

        from disco.core import Job
        results = Job(master, name, ...).run().wait()

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
        - :meth:`disco.core.Disco.jobspec`
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

    def __init__(self, name, master=None, worker=None, settings=DiscoSettings()):
        from disco.core import Disco
        from disco.worker.classic.worker import Worker
        self.name = name
        self.master = master or Disco()
        self.worker = worker or Worker()
        self.settings = settings

    def __getattr__(self, attr):
        if attr in self.proxy_functions:
            from functools import partial
            return partial(getattr(self.master, attr), self.name)
        raise AttributeError("%r has no attribute %r" % (self, attr))

    def run(self, **kwargs):
        for key in self.worker:
            if key in kwargs:
                self.worker[key] = kwargs.pop(key)
            elif hasattr(self, key):
                self.worker[key] = getattr(self, key)
        jobdict = JobDict(self.settings, prefix=self.name, **kwargs)
        jobpack = self.worker.jobpack(jobdict)
        status, response = json.loads(self.master.request('/disco/job/new', jobpack))
        if status != 'ok':
            raise DiscoError("Failed to start job. Server replied: %s" % response)
        self.name = response
        return self
