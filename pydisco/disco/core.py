import sys, os, time, cStringIO
from itertools import chain

from disco import func, util
from disco.comm import download, json
from disco.ddfs import DDFS
from disco.error import DiscoError, JobError, CommError
from disco.eventmonitor import EventMonitor
from disco.modutil import find_modules
from disco.netstring import decode_netstring_fd, encode_netstring_fd

class Params(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __getstate__(self):
        return dict((k, util.pack(v))
            for k, v in self.__dict__.iteritems()
                if not k.startswith('_'))

    def __setstate__(self, state):
        for k, v in state.iteritems():
            self.__dict__[k] = util.unpack(v)

class Stats(object):
    def __init__(self, prof_data):
        import marshal
        self.stats = marshal.loads(prof_data)

    def create_stats(self):
        pass

class Continue(Exception):
    pass

class JobSpecifier(list):
    def __init__(self, jobspec):
        super(JobSpecifier, self).__init__([jobspec]
            if type(jobspec) is str else jobspec)

    @property
    def jobnames(self):
        for job in self:
            if type(job) is str:
                yield job
            elif type(job) is list:
                yield job[0]
            else:
                yield job.name

    def __str__(self):
        return '{%s}' % ', '.join(self.jobnames)

class JobDict(util.DefaultDict):
    """
    .. todo:: deprecate nr_reduces, move to nr_partitions
    """
    defaults = {'input': (),
                'map': None,
                'map_init': func.noop,
                'map_reader': func.map_line_reader,
                'map_writer': func.netstr_writer,
                'map_input_stream': (func.map_input_stream, ),
                'map_output_stream': (func.map_output_stream, ),
                'combiner': None,
                'partition': func.default_partition,
                'reduce': None,
                'reduce_init': func.noop,
                'reduce_reader': func.netstr_reader,
                'reduce_writer': func.netstr_writer,
                'reduce_input_stream': (func.reduce_input_stream, ),
                'reduce_output_stream': (func.reduce_output_stream, ),
                'ext_map': False,
                'ext_reduce': False,
                'ext_params': None,
                'mem_sort_limit': 256 * 1024**2,
                'nr_reduces': 1, # XXX: default has changed!
                'params': Params(),
                'prefix': '',
                'profile': False,
                'required_files': None,
                'required_modules': None,
                'scheduler': {'max_cores': '%d' % 2**31},
                'save': False,
                'sort': False,
                'status_interval': 100000,
                'version': '.'.join(str(s) for s in sys.version_info[:2])}
    default_factory = defaults.__getitem__

    functions = set(['map',
                     'map_init',
                     'map_reader',
                     'map_writer',
                     'combiner',
                     'partition',
                     'reduce',
                     'reduce_init',
                     'reduce_reader',
                     'reduce_writer'])

    scheduler_keys = set(['force_local', 'force_remote', 'max_cores'])

    stacks = set(['map_input_stream',
                  'map_output_stream',
                  'reduce_input_stream',
                  'reduce_output_stream'])

    def __init__(self, *args, **kwargs):
        super(JobDict, self).__init__(*args, **kwargs)

        # -- backwards compatibility --
        if 'fun_map' in self and 'map' not in self:
            self['map'] = self.pop('fun_map')

        if 'input_files' in kwargs and 'input' not in self:
            self['input'] = self.pop('input_files')

        if 'chunked' in self:
            raise DeprecationWarning("Argument 'chunked' is deprecated") # XXX: Deprecate properly

        # -- external flags --
        if isinstance(self['map'], dict):
            self['ext_map'] = True
        if isinstance(self['reduce'], dict):
            self['ext_reduce'] = True

        # -- input --
        self['input'] = [url for i in self['input']
                         for url in util.urllist(i, listdirs=bool(self['map']))]

        # XXX: Check for redundant inputs, external & partitioned inputs

        # -- required modules and files --

        if self['required_modules'] is None:
            functions = util.flatten(util.iterify(self[f])
                                     for f in chain(self.functions, self.stacks))
            self['required_modules'] = find_modules([f for f in functions
                                                     if callable(f)])

        if self['required_files']:
            if not isinstance(self['required_files'], dict):
                self['required_files'] = util.pack_files(self['required_files'])
        else:
            self['required_files'] = {}
        self['required_files'].update(util.pack_files(o[1]
                                                      for o in self['required_modules']
                                                      if util.iskv(o)))

        # -- scheduler --
        scheduler = self.__class__.defaults['scheduler'].copy()
        scheduler.update(self['scheduler'])
        if 'nr_maps' in self:
            # XXX: Deprecate properly
            raise DeprecationWarning("Use scheduler = {'max_cores': N} instead or nr_maps.")
            if 'max_cores' not in scheduler:
                scheduler['max_cores'] = self['nr_maps']
        if int(scheduler['max_cores']) < 1:
            raise DiscoError("max_cores must be >= 1")
        self['scheduler'] = scheduler

        # -- sanity checks --
        if not self['map'] and not self['reduce']:
            raise DiscoError("Must specify map and/or reduce")

        for key in self:
            if key not in self.defaults:
                raise DiscoError("Unknown job argument: %s" % key)

    def pack(self):
        jobpack = {}

        for key in self.defaults:
            if key == 'input':
                jobpack['input'] = ' '.join('\n'.join(reversed(list(util.iterify(url)))) # XXX: why reverse?
                                            for url in self['input'])
            elif key in ('nr_reduces', 'prefix'):
                jobpack[key] = str(self[key])
            elif key == 'scheduler':
                scheduler = self['scheduler']
                for key in scheduler:
                    jobpack['sched_%s' % key] = str(scheduler[key])
            elif self[key] is None:
                pass
            elif key in self.stacks:
                jobpack[key] = util.pack_stack(self[key])
            else:
                jobpack[key] = util.pack(self[key])
        return encode_netstring_fd(jobpack)

    @classmethod
    def unpack(cls, jobpack, globals={}):
        jobdict = cls.defaults.copy()
        jobdict.update(**decode_netstring_fd(jobpack))

        for key in cls.defaults:
            if key == 'input':
                jobdict['input'] = [i.split()
                                    for i in jobdict['input'].split(' ')]
            elif key == 'nr_reduces':
                jobdict[key] = int(jobdict[key])
            elif key == 'scheduler':
                for key in cls.scheduler_keys:
                    if 'sched_%s' % key in jobdict:
                        jobdict['scheduler'][key] = jobdict.pop('sched_%s' % key)
            elif key == 'prefix':
                pass
            elif jobdict[key] is None:
                pass
            elif key in cls.stacks:
                jobdict[key] = util.unpack_stack(jobdict[key], globals=globals)
            else:
                jobdict[key] = util.unpack(jobdict[key], globals=globals)
        return cls(**jobdict)

class Job(object):
    proxy_functions = ('clean',
                       'events',
                       'kill',
                       'jobinfo',
                       'jobspec',
                       'oob_get',
                       'oob_list',
                       'parameters',
                       'profile_stats',
                       'purge',
                       'results',
                       'wait')

    def __init__(self, master, name='', **kwargs):
        self.master  = master
        self.name    = name
        self.jobdict = JobDict(prefix=name, **kwargs)
        self._run()

    def __getattr__(self, attr):
        if attr in self.proxy_functions:
            from functools import partial
            return partial(getattr(self.master, attr), self.name)
        raise AttributeError("%r has no attribute %r" % (self, attr))

    def _run(self):
        jobpack = self.jobdict.pack()
        reply = json.loads(self.master.request('/disco/job/new', jobpack))
        if reply[0] != 'ok':
            raise DiscoError("Failed to start a job. Server replied: " + reply)
        self.name = reply[1]

class Disco(object):
    def __init__(self, host):
        self.host = host

    def request(self, url, data = None, redir = False, offset = 0):
        try:
            return download(self.host + url, data=data, redir=redir, offset=offset)
        except CommError, e:
            e.msg += " (is disco master running at %s?)" % self.host
            raise

    def get_config(self):
        return json.loads(self.request('/disco/ctrl/load_config_table'))

    def set_config(self, config):
        response = json.loads(self.request('/disco/ctrl/save_config_table', json.dumps(config)))
        if response != 'table saved!':
            raise DiscoError(response)

    config = property(get_config, set_config)

    def nodeinfo(self):
        return json.loads(self.request('/disco/ctrl/nodeinfo'))

    def joblist(self):
        return json.loads(self.request("/disco/ctrl/joblist"))

    def blacklist(self, node):
        self.request('/disco/ctrl/blacklist', '"%s"' % node)

    def whitelist(self, node):
        self.request('/disco/ctrl/whitelist', '"%s"' % node)

    def oob_get(self, name, key):
        try:
            return util.load_oob(self.host, name, key)
        except CommError, e:
            if e.code == 404:
                raise DiscoError("Unknown key or job name")
            raise

    def oob_list(self, name):
        r = self.request('/disco/ctrl/oob_list?name=%s' % name, redir=True)
        return json.loads(r)

    def profile_stats(self, name, mode=''):
        prefix = 'profile-%s' % mode
        f = [s for s in self.oob_list(name) if s.startswith(prefix)]
        if not f:
            raise JobError("No profile data", self.host, name)

        import pstats
        stats = pstats.Stats(Stats(self.oob_get(name, f[0])))
        for s in f[1:]:
            stats.add(Stats(self.oob_get(name, s)))
        return stats

    def new_job(self, **kwargs):
        return Job(self, **kwargs)

    def kill(self, name):
        self.request('/disco/ctrl/kill_job', '"%s"' % name)

    def clean(self, name):
        self.request('/disco/ctrl/clean_job', '"%s"' % name)

    def purge(self, name):
        self.request('/disco/ctrl/purge_job', '"%s"' % name)

    def jobdict(self, name):
        return JobDict.unpack(self.jobpack(name))

    def jobpack(self, name):
        return self.request('/disco/ctrl/parameters?name=%s' % name, redir=True)

    def jobspec(self, name):         # XXX: deprecate this
        return self.jobdict(name)

    def events(self, name, offset = 0):
        def event_iter(events):
            offs = offset
            lines = events.splitlines()
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
                    #if i == len(lines) - 1 and\
                    #    events.endswith("\n"):
                    #    offs -= 1
                    yield offs, event
        try:
            r = self.request("/disco/ctrl/rawevents?name=%s" % name,
                     redir = True,
                     offset = offset)
        except Exception, x:
            return []

        if len(r) < 2:
            return []
        return event_iter(r)

    def results(self, jobspec, timeout = 2000):
        jobspecifier = JobSpecifier(jobspec)
        data = json.dumps([timeout, list(jobspecifier.jobnames)])
        results = json.loads(self.request("/disco/ctrl/get_results", data))

        if type(jobspec) == str:
            return results[0][1]

        others, active = [], []
        for result in results:
            if result[1][0] == 'active':
                active.append(result)
            else:
                others.append(result)
        return others, active

    def jobinfo(self, name):
        return json.loads(self.request("/disco/ctrl/jobinfo?name=%s" % name))

    def check_results(self, name, start_time, timeout, poll_interval):
        status, results = self.results(name, timeout = poll_interval)
        if status == 'ready':
            return results
        if status != 'active':
            raise JobError("Job status %s" % status, self.host, name)
        if timeout and time.time() - start_time > timeout:
            raise JobError("Timeout", self.host, name)
        raise Continue()

    def wait(self, name, show = None, poll_interval = 2, timeout = None, clean = False):
        event_monitor = EventMonitor(show, disco=self, name=name)
        start_time    = time.time()
        while True:
            event_monitor.refresh()
            try:
                return self.check_results(name, start_time, timeout, poll_interval * 1000)
            except Continue:
                continue
            finally:
                if clean:
                    self.clean(name)
                event_monitor.refresh()

    def result_iterator(self, *args, **kwargs):
        kwargs['ddfs'] = self.host
        return result_iterator(*args, **kwargs)

def result_iterator(results,
                    notifier = None,
                    reader = func.netstr_reader,
                    input_stream = [func.map_input_stream],
                    params = None,
                    ddfs = None):
    from disco.task import Task
    task = Task()
    for fun in input_stream:
        fun.func_globals.setdefault('Task', task)

    for result in results:
        for url in util.urllist(result, ddfs=ddfs):
            fd = sze = None
            for fun in input_stream:
                fd, sze, url = fun(fd, sze, url, params)

            if notifier:
                notifier(url)

            for x in reader(fd, sze, url):
                yield x
