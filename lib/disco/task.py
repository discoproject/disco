"""
:mod:`disco.task` -- Disco Tasks
================================

This module defines objects for interfacing with
:term:`tasks <task>` assigned by the master.

"""
import os, time

from disco import dPickle
from disco.util import hexhash

def jobdata(*objs):
    """
    :return: :ref:`jobdata` needed for instantiating the :class:`disco.job.Job` on the node.
    """
    return dPickle.dumps(objs, -1)

class Task(object):
    """
    Encapsulates the information specific to a particular
    :term:`task` coming from the master.

    .. attribute:: host

        The name of the host this task is running on.

    .. attribute:: jobname

        The name of the :term:`job` this task is part of.

    .. attribute:: master

        The name of the master host for this task.

    .. attribute:: mode

        The phase which this task is part of.
        Currently either :term:`map` or :term:`reduce`.

    .. attribute:: taskid

        The id of this task, assigned by the master.

    .. attribute:: uid

        A unique id for this particular task instance.
    """
    def __init__(self,
                 host='',
                 jobfile='',
                 jobname='',
                 master=None,
                 disco_port=None,
                 put_port=None,
                 ddfs_data='',
                 disco_data='',
                 mode=None,
                 taskid=-1):
        from disco.job import JobPack
        self.host = host
        self.jobfile = jobfile
        self.jobname = jobname
        self.jobpack = JobPack.load(open(jobfile))
        self.jobobjs = dPickle.loads(self.jobpack.jobdata)
        self.master = master
        self.disco_port = disco_port
        self.put_port = put_port
        self.ddfs_data = ddfs_data
        self.disco_data = disco_data
        self.mode = mode
        self.taskid = taskid
        self.outputs = {}
        self.uid = '%s:%s-%s-%x' % (mode,
                                    taskid,
                                    hexhash(str((time.time()))),
                                    os.getpid())

    @property
    def jobpath(self):
        return os.path.join(self.host, hexhash(self.jobname), self.jobname)

    @property
    def taskpath(self):
        return os.path.join(hexhash(self.uid), self.uid)

    def makedirs(self):
        from disco.fileutils import ensure_path
        ensure_path(self.taskpath)

    def output(self, partition=None, type='disco'):
        if partition is None:
            return self.path(self.uid), type, '0'
        elif not isinstance(partition, basestring):
            raise ValueError("Partition label must be a string or None")
        return self.path('%s-%s' % (self.mode, partition)), 'part', partition

    def path(self, name):
        """
        :return: The *name* joined to the :attr:`taskpath`.
        """
        return os.path.join(self.taskpath, name)

    def url(self, name, scheme='disco'):
        return '%s://%s/disco/%s/%s/%s' % (scheme, self.host, self.jobpath, self.taskpath, name)

    def get(self, key):
        """
        Gets an out-of-band result for the task with the key *key*.

        Given the semantics of OOB results, this means that only the reduce
        phase can access results produced in the preceding map phase.
        """
        from disco.util import load_oob
        return load_oob(self.master, self.jobname, key)

    def put(self, key, value):
        """
        Stores an out-of-band result *value* with the key *key*.

        Key must be unique in this job.
        Maximum key length is 256 characters.
        Only characters in the set ``[a-zA-Z_\-:0-9@]`` are allowed in the key.
        """
        from disco.ddfs import DDFS
        from disco.util import save_oob
        if DDFS.safe_name(key) != key:
            raise DiscoError("OOB key contains invalid characters (%s)" % key)
        save_oob(self.master, self.jobname, key, value)
