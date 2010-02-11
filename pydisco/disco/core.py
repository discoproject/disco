import sys, re, os, modutil, time, types, cPickle, cStringIO, random

from disco import func, util
from disco.comm import download, json
from disco.error import DiscoError, JobError, CommError
from disco.eventmonitor import EventMonitor
from disco.netstring import encode_netstring_fd, encode_netstring_str, decode_netstring_fd
from disco.task import Task

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

class Disco(object):
    def __init__(self, host):
        self.host = util.disco_host(host)

    def request(self, url, data = None, redir = False, offset = 0):
        try:
            return download(self.host + url, data = data, redir = redir, offset = offset)
        except CommError, e:
            raise DiscoError("Got %s, make sure disco master is running at %s" % (e, self.host))

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
            if e.http_code == 404:
                raise DiscoError("Unknown key or job name")
            raise

    def oob_list(self, name):
        r = self.request("/disco/ctrl/oob_list?name=%s" % name, redir=True)
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

    def jobspec(self, name):
        r = self.request('/disco/ctrl/parameters?name=%s' % name, redir=True)
        return decode_netstring_fd(cStringIO.StringIO(r))

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
        data     = json.dumps([timeout, list(jobspecifier.jobnames)])
        results      = json.loads(self.request("/disco/ctrl/get_results", data))

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

    def check_results(self, name, start_time, timeout):
        status, results = self.results(name, timeout=timeout)
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
                return self.check_results(name, start_time, poll_interval * 1000)
            except Continue:
                continue
            finally:
                if clean:
                    self.clean(name)
                event_monitor.refresh()

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

class Job(object):

    funs = ["map", "map_init", "reduce_init", "map_reader", "map_writer",\
        "reduce_reader", "reduce_writer", "reduce", "partition",\
        "combiner", "reduce_input_stream", "reduce_output_stream",\
        "map_input_stream", "map_output_stream"]

    defaults = {"name": None,
            "map": None,
            "input": None,
            "map_init": None,
            "reduce_init": None,
            "map_reader": func.map_line_reader,
            "map_writer": func.netstr_writer,
            "map_input_stream": None,
            "map_output_stream": None,
            "reduce_reader": func.netstr_reader,
            "reduce_writer": func.netstr_writer,
            "reduce_input_stream": None,
            "reduce_output_stream": None,
            "reduce": None,
            "partition": func.default_partition,
            "combiner": None,
            "nr_maps": None,
            "scheduler": {},
#XXX: nr_reduces default has changed!
            "nr_reduces": 1,
            "sort": False,
            "params": Params(),
            "mem_sort_limit": 256 * 1024**2,
            "ext_params": None,
            "status_interval": 100000,
            "required_files": [],
            "required_modules": [],
            "profile": False}

    mapreduce_functions = ("map_init",
                   "map_reader",
                   "map",
                   "map_writer",
                   "partition",
                   "combiner",
                   "reduce_init",
                   "reduce_reader",
                   "reduce",
                   "reduce_writer")

    proxy_functions = ("kill",
               "clean",
               "purge",
               "jobspec",
               "results",
               "jobinfo",
               "wait",
               "oob_get",
               "oob_list",
               "profile_stats",
               "events")

    def __init__(self, master, name='', **kwargs):
        self.master = master
        self.name   = name
        self._run(**kwargs)

    def __getattr__(self, attr):
        if attr in self.proxy_functions:
            from functools import partial
            return partial(getattr(self.master, attr), self.name)
        raise AttributeError("%r has no attribute %r" % (self, attr))

    def pack_stack(self, kw, req, stream):
        if stream in kw:
            req[stream] = encode_netstring_str((f.func_name, util.pack(f))
                               for f in util.iterify(kw[stream]))

    def _run(self, **kwargs):
        jobargs = util.DefaultDict(self.defaults.__getitem__, kwargs)

        # -- check parameters --

        # Backwards compatibility
        # (fun_map == map, input_files == input)
        if "fun_map" in kwargs:
            kwargs["map"] = kwargs["fun_map"]

        if "input_files" in kwargs:
            kwargs["input"] = kwargs["input_files"]

        if "chunked" in kwargs:
            raise DeprecationWarning("Argument 'chunked' is deprecated")

        if "nr_maps" in kwargs:
            sys.stderr.write("Warning: nr_maps is deprecated. "
                     "Use scheduler = {'max_cores': N} instead.\n")
            sched = jobargs["scheduler"].copy()
            if "max_cores" not in sched:
                sched["max_cores"] = int(jobargs["nr_maps"])
            jobargs["scheduler"] = sched

        if not "input" in kwargs:
            raise DiscoError("Argument input is required")

        if not ("map" in kwargs or "reduce" in kwargs):
            raise DiscoError("Specify map and/or reduce")

        for p in kwargs:
            if p not in Job.defaults:
                raise DiscoError("Unknown argument: %s" % p)

        input = kwargs["input"]

        # -- initialize request --

        request = {"prefix": self.name,
               "version": ".".join(map(str, sys.version_info[:2])),
               "params": cPickle.dumps(jobargs['params'], cPickle.HIGHEST_PROTOCOL),
               "sort": str(int(jobargs['sort'])),
               "mem_sort_limit": str(jobargs['mem_sort_limit']),
               "status_interval": str(jobargs['status_interval']),
               "profile": str(int(jobargs['profile']))}

        # -- required modules --

        if 'required_modules' in kwargs:
            rm = kwargs['required_modules']
        else:
            functions = util.flatten(util.iterify(jobargs[f])
                         for f in self.mapreduce_functions)
            rm = modutil.find_modules([f for f in functions\
                if callable(f)])

        send_mod = []
        imp_mod = []
        for mod in rm:
            if type(mod) == tuple:
                send_mod.append(mod[1])
                mod = mod[0]
            imp_mod.append(mod)

        request["required_modules"] = " ".join(imp_mod)
        rf = util.pack_files(send_mod)

        # -- input & output streams --

        for stream in ["map_input_stream", "map_output_stream",
                   "reduce_input_stream", "reduce_output_stream"]:
            self.pack_stack(kwargs, request, stream)

        # -- required files --

        if "required_files" in kwargs:
            if isinstance(kwargs["required_files"], dict):
                rf.update(kwargs["required_files"])
            else:
                rf.update(util.pack_files(\
                    kwargs["required_files"]))
        if rf:
            request["required_files"] = util.pack(rf)

        # -- scheduler --

        sched = jobargs["scheduler"]
        sched_keys = ["max_cores", "force_local", "force_remote"]

        if "max_cores" not in sched:
            sched["max_cores"] = 2**31
        elif sched["max_cores"] < 1:
            raise DiscoError("max_cores must be >= 1")

        for k in sched_keys:
            if k in sched:
                request["sched_" + k] = str(sched[k])

        # -- map --

        if 'map' in kwargs:
            k = 'ext_map' if isinstance(kwargs['map'], dict) else 'map'
            request[k] = util.pack(kwargs['map'])

            for function_name in ('map_init',
                          'map_reader',
                          'map_writer',
                          'partition',
                          'combiner'):
                function = jobargs[function_name]
                if function:
                    request[function_name] = util.pack(function)

            def inputlist(input):
                if hasattr(input, '__iter__'):
                    return ['\n'.join(reversed(list(input)))]
                return util.urllist(input)
            input = [e for i in input for e in inputlist(i)]

        # -- only reduce --

        else:
            # XXX: Check for redundant inputs, external &
            # partitioned inputs
            input = [url for i in input for url in util.urllist(i)]

        request['input'] = ' '.join(input)

        if 'ext_params' in kwargs:
            e = kwargs['ext_params']
            request['ext_params'] = encode_netstring_fd(e)\
                if isinstance(e, dict) else e

        # -- reduce --

        nr_reduces = jobargs['nr_reduces']
        if 'reduce' in kwargs:
            k = 'ext_reduce' if isinstance(kwargs['reduce'], dict) else 'reduce'
            request[k] = util.pack(kwargs['reduce'])

            for function_name in ('reduce_reader', 'reduce_writer', 'reduce_init'):
                function = jobargs[function_name]
                if function:
                    request[function_name] = util.pack(function)

        request['nr_reduces'] = str(nr_reduces)

        # -- encode and send the request --

        reply = self.master.request("/disco/job/new", encode_netstring_fd(request))

        if not reply.startswith('job started:'):
            raise DiscoError("Failed to start a job. Server replied: " + reply)
        self.name = reply.split(':', 1)[1]

def result_iterator(results,
            notifier = None,
            reader = func.netstr_reader,
            input_stream = [func.map_input_stream],
            params = None):

    task = Task(result_iterator = True)
    for fun in input_stream:
        fun.func_globals.setdefault("Task", task)

    res = []
    res = [url for r in results for url in util.urllist(r)]

    for url in res:
        fd = sze = None
        for fun in input_stream:
            fd, sze, url = fun(fd, sze, url, params)

        if notifier:
            notifier(url)

        for x in reader(fd, sze, url):
            yield x

