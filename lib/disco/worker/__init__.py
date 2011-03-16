"""
:mod:`disco.worker` -- Python Worker Basics and Interfaces
==========================================================

        for record in task.input(open=...):
                pass

"""
import cPickle, os, sys, traceback

class Worker(dict):
    def __init__(self, **kwargs):
        super(Worker, self).__init__(self.defaults())
        self.update(kwargs)

    @property
    def bin(self):
        return os.path.join('lib', '%s.py' % self.__module__.replace('.', '/'))

    def defaults(self):
        return {'map': None,
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
        from disco.util import inputlist, ispartitioned, read_index
        def get(key):
            return self.getitem(key, job, **jobargs)
        has_map = bool(get('map'))
        has_reduce = bool(get('reduce'))
        input = inputlist(get('input') or (),
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

        return {'input': input,
                'worker': self.bin,
                'map?': has_map,
                'reduce?': has_reduce,
                'profile?': get('profile'),
                'nr_reduces': nr_reduces,
                'prefix': job.name,
                'scheduler': get('scheduler'),
                'owner': job.settings['DISCO_JOB_OWNER']}

    def jobhome(self, job, **jobargs):
        jobzip = self.jobzip(job, **jobargs)
        jobzip.close()
        return jobzip.dumps()

    def jobzip(self, job, **jobargs):
        from disco import __file__ as discopath
        from disco.fileutils import DiscoZipFile
        from disco.util import iskv
        def get(key):
            return self.getitem(key, job, **jobargs)
        jobzip = DiscoZipFile()
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
        return cPickle.dumps((self, job, jobargs), -1)

    def start(self, job, task):
        from disco.sysutil import set_mem_limit
        set_mem_limit(job.settings['DISCO_WORKER_MAX_MEM'])
        if self['profile']:
            from cProfile import runctx
            name = 'profile-%s' % task.uid
            path = task.path(name)
            runctx('self.run(task)', globals(), locals(), path)
            task.put(name, open(path).read())
        else:
            self.run(task)
        self.end(task)

    def run(self, task):
        self[task.mode](task)

    def end(self, task):
        from disco.events import Status
        if not self['save'] or (task.mode == 'map' and self['reduce']):
            task.send()
            Status("Results sent to master").send()
        else:
            task.save()
            Status("Results saved to DDFS").send()

    @classmethod
    def unpack(cls, jobpack):
        worker, job, jobargs = cPickle.loads(jobpack.jobdata)
        for key in worker:
            worker[key] = worker.getitem(key, job, **jobargs)
        return worker, job

    @classmethod
    def main(cls):
        # we have a bootstrapping problem if we send the disco lib and someone subclasses Worker
        # add jobenv to jobpack, master should set these for the process
        sys.path.insert(0, 'lib')
        from disco.error import DataError
        from disco.events import AnnouncePID, WorkerDone, DataUnavailable, TaskFailed
        from disco.job import JobPack
        from disco.task import Task
        from disco.util import MessageWriter
        try:
            AnnouncePID(str(os.getpid())).send()
            sys.stdout = MessageWriter()
            worker, job = cls.unpack(JobPack.request())
            worker.start(job, Task.request())
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
