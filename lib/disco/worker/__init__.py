#!/usr/bin/env python
"""
:mod:`disco.worker` -- Python Worker Interface
==============================================

In Disco, :term:`workers <worker>` do the brunt of the data processing work.
When a :class:`disco.job.Job` is created, it gets passed a :class:`Worker` instance,
which is responsible for defining the fields used by the :class:`disco.job.JobPack`.
In most cases, you don't need to define your own Worker subclass in order to run a job.
The Worker classes defined in :mod:`disco` will take care of the details
of creating the fields necessary for the :class:`disco.job.JobPack`,
and when executed on the nodes,
will handle the implementation of the :ref:`worker_protocol`.

There is perhaps a subtle, but important, distinction between
a :term:`worker` and a :class:`Worker`.
The former refers to any binary that gets executed on the nodes,
specified by :attr:`jobdict.worker`.
The latter is a Python class,
which handles details of submitting the job on the client side,
as well as controlling the execution of user-defined code on the nodes.
A :class:`Worker` can be subclassed trivially to create a new :term:`worker`,
without having to worry about fulfilling many of the requirements
for a well-behaving worker.
In short,
a :class:`Worker` provides Python library support for a Disco :term:`worker`.
Those wishing to write a worker in a language besides Python may make use of
the Worker class for submitting jobs to the master,
but generally need to handle the :ref:`worker_protocol`
in the language used for the worker executable.

The :class:`Classic Worker <disco.worker.classic.worker.Worker>`
is a subclass of :class:`Worker`,
which implements the classic Disco :term:`mapreduce` interface.

The following steps illustrate the sequence of events for running a :term:`job`
using a standard :class:`Worker`:

#. (client) instantiate a :class:`disco.job.Job`
        #. if a worker is supplied, use that worker
        #. otherwise, create a worker using :attr:`disco.job.Job.Worker`
           (the default is :class:`disco.worker.classic.worker.Worker`)
#. (client) call :meth:`disco.job.Job.run`
        #. create a :class:`disco.job.JobPack` using:
           :meth:`Worker.jobdict`,
           :meth:`Worker.jobenvs`,
           :meth:`Worker.jobhome`,
           :meth:`Worker.jobdata`
        #. submit the :class:`disco.job.JobPack` to the master
#. (node) master unpacks the :term:`job home`
#. (node) master executes the :attr:`jobdict.worker` with
   current working directory set to the :term:`job home` and
   environment variables set from :ref:`jobenvs`
#. (node) worker requests the :class:`disco.task.Task` from the master
#. (node) worker runs the :term:`task` and reports the output to the master
"""
import cPickle, os, sys, traceback

class Worker(dict):
    """
    A :class:`Worker` is a :class:`dict` subclass,
    with special methods defined for serializing itself,
    and possibly reinstantiating itself on the nodes where :term:`tasks <task>` are run.

    The :class:`Worker` base class makes use of the following parameters:

    :type  input: list of urls or list of list of urls
    :param input: used to set :attr:`jobdict.input`.
                  Disco natively handles the following url schemes:

                  * ``http://...`` - any HTTP address
                  * ``file://...`` or no scheme - a local file.
                    The file must exist on all nodes where the tasks are run.
                    Due to these restrictions, this form has only limited use.
                  * ``tag://...`` - a tag stored in :ref:`DDFS`
                  * ``raw://...`` - pseudo-address: use the address itself as data.
                  * ``dir://...`` - used by Disco internally.
                  * ``disco://...`` - used by Disco internally.

                  .. seealso:: :mod:`disco.schemes`.

    :type  map: function or None
    :param map: called when the :class:`Worker` is :meth:`run` with a
                :class:`disco.task.Task` in mode *map*.
                Also used to determine the value of :attr:`jobdict.map?`.

    :type  reduce: function or None
    :param reduce: called when the :class:`Worker` is :meth:`run` with a
                :class:`disco.task.Task` in mode *reduce*.
                Also used to determine the value of :attr:`jobdict.reduce?`.

    :type  required_files: list of paths or dict
    :param required_files: additional files that are required by the worker.
                           Either a list of paths to files to include,
                           or a dictionary which contains items of the form
                           ``(filename, filecontents)``.

                           .. versionchanged:: 0.4
                              The worker includes *required_files* in :meth:`jobhome`,
                              so they are available relative to the working directory
                              of the worker.

    :type  required_modules: list of modules or module names
    :param required_modules: required modules to send with the worker.

                             .. versionchanged:: 0.4
                                Can also be a list of module objects.

    :type  save: bool
    :param save: whether or not to save the output to :ref:`DDFS`.

    :type  scheduler: dict
    :param scheduler: directly sets :attr:`jobdict.scheduler`.

    :type  profile: bool
    :param profile: directly sets :attr:`jobdict.profile?`.
    """
    def __init__(self, **kwargs):
        super(Worker, self).__init__(self.defaults())
        self.update(kwargs)

    @property
    def bin(self):
        """
        The path to the :term:`worker` binary, relative to the :term:`job home`.
        Used to set :attr:`jobdict.worker` in :meth:`jobdict`.
        """
        return os.path.join('lib', '%s.py' % self.__module__.replace('.', '/'))

    def defaults(self):
        """
        :return: dict of default values for the :class:`Worker`.
        """
        return {'input': (),
                'map': None,
                'merge_partitions': False, # XXX: maybe deprecated
                'reduce': None,
                'required_files': {},
                'required_modules': None,
                'save': False,
                'scheduler': {},
                'partitions': 1,  # move to classic once partitions are dynamic
                'profile': False}

    def getitem(self, key, job, **jobargs):
        if key in jobargs:
            return jobargs[key]
        elif hasattr(job, key):
            return getattr(job, key)
        return self.get(key)

    def jobdict(self, job, **jobargs):
        """
        :return: the :term:`job dict`.
        """
        from disco.util import inputlist, ispartitioned, read_index
        def get(key):
            return self.getitem(key, job, **jobargs)
        has_map = bool(get('map'))
        has_reduce = bool(get('reduce'))
        input = inputlist(get('input'),
                          partition=None if has_map else False,
                          settings=job.settings)

        # -- nr_reduces --
        # ignored if there is not actually a reduce specified
        # XXX: master should always handle this
        if has_map:
            # partitioned map has N reduces; non-partitioned map has 1 reduce
            nr_reduces = get('partitions') or 1
        elif ispartitioned(input):
            # no map, with partitions: len(dir://) specifies nr_reduces
            nr_reduces = 1 + max(int(id)
                                 for dir in input
                                 for id, url in read_index(dir))
        else:
            # no map, without partitions can only have 1 reduce
            nr_reduces = 1

        if get('merge_partitions'):
            nr_reduces = 1

        return {'input': input,
                'worker': self.bin,
                'map?': has_map,
                'reduce?': has_reduce,
                'profile?': get('profile'),
                'nr_reduces': nr_reduces,
                'prefix': job.name,
                'scheduler': get('scheduler'),
                'owner': job.settings['DISCO_JOB_OWNER']}

    def jobenvs(self, job, **jobargs):
        """
        :return: :ref:`jobenvs` dict.
        """
        settings = job.settings
        settings['LC_ALL'] = 'C'
        settings['LD_LIBRARY_PATH'] = 'lib'
        settings['PYTHONPATH'] = ':'.join((settings.get('PYTHONPATH', ''), 'lib'))
        return settings.env

    def jobhome(self, job, **jobargs):
        """
        :return: the :term:`job home` (serialized).

        Calls :meth:`jobzip` to create the :class:`disco.fileutils.DiscoZipFile`.
        """
        jobzip = self.jobzip(job, **jobargs)
        jobzip.close()
        return jobzip.dumps()

    def jobzip(self, job, **jobargs):
        """
        A hook provided by the :class:`Worker` for creating the :term:`job home` zip.

        :return: a :class:`disco.fileutils.DiscoZipFile`.
        """
        from clx import __file__ as clxpath
        from disco import __file__ as discopath
        from disco.fileutils import DiscoZipFile
        from disco.util import iskv
        def get(key):
            return self.getitem(key, job, **jobargs)
        jobzip = DiscoZipFile()
        jobzip.writepy(os.path.dirname(clxpath), 'lib')
        jobzip.writepy(os.path.dirname(discopath), 'lib')
        jobzip.writemodule(job.__module__)
        jobzip.writemodule(self.__module__)
        if isinstance(get('required_files'), dict):
            for path, bytes in get('required_files').iteritems():
                    jobzip.writebytes(path, bytes)
        else:
            for path in get('required_files'):
                jobzip.writepath(path)
        for mod in get('required_modules') or ():
            jobzip.writemodule((mod[0] if iskv(mod) else mod), 'lib')
        return jobzip

    def jobdata(self, job, **jobargs):
        """
        :return: :ref:`jobdata` needed for instantiating the :class:`Worker` on the node.
        """
        return cPickle.dumps((self, job, jobargs), -1)

    def start(self, task, job, **jobargs):
        from disco.sysutil import set_mem_limit
        set_mem_limit(job.settings['DISCO_WORKER_MAX_MEM'])
        task.makedirs()
        if self.getitem('profile', job, **jobargs):
            from cProfile import runctx
            name = 'profile-%s' % task.uid
            path = task.path(name)
            runctx('self.run(task, job, **jobargs)', globals(), locals(), path)
            task.put(name, open(path).read())
        else:
            self.run(task, job, **jobargs)
        self.end(task, job, **jobargs)

    def run(self, task, job, **jobargs):
        """
        Called to do the actual work of processing the :class:`disco.task.Task`.
        """
        self.getitem(task.mode, job, **jobargs)(task, job, **jobargs)

    def end(self, task, job, **jobargs):
        from disco.events import Status
        if not self['save'] or (task.mode == 'map' and self['reduce']):
            task.send()
            Status("Results sent to master").send()
        else:
            task.save()
            Status("Results saved to DDFS").send()

    @classmethod
    def unpack(cls, jobpack):
        try:
            from imp import find_module, load_module
            __disco__ = load_module('__disco__', *find_module('__main__', ['lib']))
            sys.modules['__main__'].__dict__.update(__disco__.__dict__)
        except ImportError:
            pass
        return cPickle.loads(jobpack.jobdata)

    @classmethod
    def main(cls):
        """
        The main method used to bootstrap the :class:`Worker` when it is being executed.

        It is enough for the module to define::

                if __name__ == '__main__':
                    Worker.main()

        .. note:: It is critical that subclasses check if they are executing
                  in the ``__main__`` module, before running :meth:`main`,
                  as the worker module is also generally imported on the client side.
        """
        from disco.error import DataError
        from disco.events import AnnouncePID, WorkerDone, DataUnavailable, TaskFailed
        from disco.job import JobPack
        from disco.task import Task
        from disco.fileutils import NonBlockingInput
        from disco.util import MessageWriter
        try:
            sys.stdin = NonBlockingInput(sys.stdin, timeout=600)
            sys.stdout = MessageWriter()
            AnnouncePID(str(os.getpid())).send()
            worker, job, jobargs = cls.unpack(JobPack.request())
            worker.start(Task.request(), job, **jobargs)
            WorkerDone().send()
        except (DataError, EnvironmentError, MemoryError), e:
            # check the number of open file descriptors (under proc), warn if close to max
            # http://stackoverflow.com/questions/899038/getting-the-highest-allocated-file-descriptor
            # also check for other known reasons for error, such as if disk is full
            DataUnavailable(traceback.format_exc()).send()
            raise
        except Exception, e:
            TaskFailed(MessageWriter.force_utf8(traceback.format_exc())).send()
            raise

if __name__ == '__main__':
    Worker.main()
