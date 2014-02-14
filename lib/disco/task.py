"""
:mod:`disco.task` -- Disco Tasks
================================

This module defines objects for interfacing with
:term:`tasks <task>` assigned by the master.

"""
import os, time

from disco.compat import basestring, integer_types
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
                 stage=None,
                 group=None,
                 grouping=None,
                 taskid=-1):
        from disco.job import JobPack
        from disco.ddfs import DDFS
        self.host = host
        self.jobfile = jobfile
        self.jobname = jobname
        self.jobpack = JobPack.load(open(jobfile, 'rb'))
        self.jobobjs = dPickle.loads(self.jobpack.jobdata)
        self.master = master
        self.disco_port = disco_port
        self.put_port = put_port
        self.ddfs_data = ddfs_data
        self.disco_data = disco_data
        self.stage = stage
        self.group = '{0[0]}-{0[1]}'.format(group)
        self.group_label, self.group_host = group
        self.grouping = grouping
        self.taskid = taskid
        self.outputs = {}
        self.uid = '{0}:{1}-{2}-{3}-{4}'.format(self.stage,
                                                DDFS.safe_name(self.group),
                                                self.taskid,
                                                hexhash(str((time.time())).encode()),
                                                os.getpid())

    @property
    def jobpath(self):
        return os.path.join(self.host, hexhash(self.jobname), self.jobname)

    @property
    def taskpath(self):
        return os.path.join(hexhash(self.uid.encode()), self.uid)

    def makedirs(self):
        from disco.fileutils import ensure_path
        ensure_path(self.taskpath)

    def output_filename(self, label):
        if not isinstance(label, integer_types):
            raise ValueError("Output label ({0} : {1}) must be an integer or None".format(label, type(label)))
        return '{0}-{1}-{2}'.format(self.stage, self.group, label)

    def output_path(self, label):
        return self.path(self.output_filename(label))

    def output(self, label=None, typ='disco'):
        if label is None:
            return self.path(self.uid), typ, 0
        return self.output_path(label), 'part', label

    def path(self, name):
        """
        :return: The *name* joined to the :attr:`taskpath`.
        """
        return os.path.join(self.taskpath, name)

    def url(self, name, scheme='disco'):
        return '{0}://{1}/disco/{2}/{3}/{4}'.format(scheme, self.host, self.jobpath, self.taskpath, name)

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
        Stores an out-of-band result *value* (bytes) with the key *key*.

        Key must be unique in this job.
        Maximum key length is 256 characters.
        Only characters in the set ``[a-zA-Z_\-:0-9@]`` are allowed in the key.
        """
        from disco.ddfs import DDFS
        from disco.util import save_oob
        from disco.error import DiscoError
        if DDFS.safe_name(key) != key:
            raise DiscoError("OOB key contains invalid characters ({0})".format(key))
        save_oob(self.master, self.jobname, key, value)
