import sys, re, os, marshal, modutil, time, types, cPickle, cStringIO, random
from disco import func, util, comm, settings
from disco.comm import json
from disco.error import DiscoError, JobException
from disco.eventmonitor import EventMonitor
from disco.netstring import encode_netstring_fd, encode_netstring_str, decode_netstring_fd

class Params(object):
        def __init__(self, **kwargs):
                self._state = {}
                for k, v in kwargs.iteritems():
                        setattr(self, k, v)

        def __setattr__(self, k, v):
                if k[0] == '_':
                        self.__dict__[k] = v
                        return
                st_v = v
                st_k = "n_" + k
                try:
                        st_v = marshal.dumps(v.func_code)
                        st_k = "f_" + k
                except AttributeError:
                        pass
                self._state[st_k] = st_v
                self.__dict__[k] = v

        def __getstate__(self):
                return self._state

        def __setstate__(self, state):
                self._state = {}
                for k, v in state.iteritems():
                        if k.startswith('f_'):
                                t = lambda x: x
                                t.func_code = marshal.loads(v)
                                v = t
                        self.__dict__[k[2:]] = v

class Stats(object):
        def __init__(self, prof_data):
                self.stats = marshal.loads(prof_data)
        def create_stats(self):
                pass

class Disco(object):
        def __init__(self, host):
                self.host = "http://" + util.disco_host(host)[7:]

        def request(self, url, data = None, redir = False, offset = 0):
                try:
                        return comm.download(self.host + url, data = data, redir = redir, offset = offset)
                except KeyboardInterrupt:
                        raise
                except Exception, e:
                        raise DiscoError('Got %s, make sure disco master is running at %s' % (e, self.host))

        def nodeinfo(self):
                return json.loads(self.request("/disco/ctrl/nodeinfo"))

        def joblist(self):
                return json.loads(self.request("/disco/ctrl/joblist"))

        def oob_get(self, name, key):
                try:
                        return util.load_oob(self.host, name, key)
                except comm.CommException, x:
                        if x.http_code == 404:
                                raise DiscoError("Unknown key or job name")
                        raise DiscoError(x)

        def oob_list(self, name):
                r = self.request("/disco/ctrl/oob_list?name=%s" % name,
                        redir = True)
                return json.loads(r)

        def profile_stats(self, name, mode = ""):
                import pstats
                if mode:
                        prefix = "profile-%s-" % mode
                else:
                        prefix = "profile-"
                f = [s for s in self.oob_list(name) if s.startswith(prefix)]
                if not f:
                        raise JobException("No profile data", self.host, name)

                stats = pstats.Stats(Stats(self.oob_get(name, f[0])))
                for s in f[1:]:
                        stats.add(Stats(self.oob_get(name, s)))
                return stats

        def new_job(self, **kwargs):
                return Job(self, **kwargs)

        def kill(self, name):
                self.request("/disco/ctrl/kill_job", '"%s"' % name)

        def clean(self, name):
                self.request("/disco/ctrl/clean_job", '"%s"' % name)

        def purge(self, name):
                self.request("/disco/ctrl/purge_job", '"%s"' % name)

        def jobspec(self, name):
                r = self.request("/disco/ctrl/parameters?name=%s" % name,
                                 redir = True)
                return decode_netstring_fd(cStringIO.StringIO(r))

        def events(self, name, offset = 0):
                def event_iter(events):
                        offs = offset
                        lines = events.splitlines()
                        for i, l in enumerate(lines):
                                offs += len(l) + 1
                                if not len(l):
                                        continue
                                try:
                                        ent = tuple(json.loads(l))
                                except ValueError:
                                        break
                                # HTTP range request doesn't like empty ranges:
                                # Let's ensure that at least the last newline
                                # is always retrieved.
                                if i == len(lines) - 1:
                                        offs -= 1
                                yield offs, ent

                r = self.request("/disco/ctrl/rawevents?name=%s" % name,
                                 redir = True, offset = offset)

                if len(r) < 2:
                        return []
                else:
                        return event_iter(r)

        def results(self, jobspec, timeout = 2000):
                jobspecifier = JobSpecifier(jobspec)
                data         = json.dumps([timeout, list(jobspecifier.jobnames)])
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
                jobinfo = self.request("/disco/ctrl/jobinfo?name=%s" % name)
                if jobinfo:
                        return json.loads(jobinfo)

        def wait(self, name, show = None, poll_interval = 2, timeout = None, clean = False):
                event_monitor = EventMonitor(show, disco=self, name=name)
                start_time    = time.time()
                while True:
                        status, results = self.results(name, timeout = poll_interval * 1000)
                        event_monitor.refresh()
                        if status == 'ready':
                                if clean:
                                        self.clean(name)
                                return results
                        if status != 'active':
                                raise JobException("Job status %s" % status, self.host, name)
                        if timeout and time.time() - start_time > timeout:
                                raise JobException("Timeout", self.host, name)

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
                    "nr_reduces": 0,
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
                if stream not in kw:
                        return
                v = kw[stream]
                if type(v) == list:
                        s = v
                else:
                        s = [v]
                req[stream] = encode_netstring_str(
                        (f.func_name, marshal.dumps(f.func_code)) for f in s)

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
                        print >> sys.stderr, "Warning: nr_maps is deprecated. "\
                                "Use scheduler = {'max_cores': N} instead."
                        sched = d("scheduler").copy()
                        if "max_cores" not in sched:
                                sched["max_cores"] = int(kw["nr_maps"])
                        kw["scheduler"] = sched
                        
                if not "input" in kwargs:
                        raise DiscoError("Argument input is required")

                if not ("map" in kwargs or "reduce" in kwargs):
                        raise DiscoError("Specify map and/or reduce")

                for p in kwargs:
                        if p not in Job.defaults:
                                raise DiscoError("Unknown argument: %s" % p)

                inputs = kwargs["input"]

                # -- initialize request --

                request = {"prefix": self.name,
                           "version": ".".join(map(str, sys.version_info[:2])),
                           "params": cPickle.dumps(jobargs['params'], cPickle.HIGHEST_PROTOCOL),
                           "sort": str(int(jobargs['sort'])),
                           "mem_sort_limit": str(jobargs['mem_sort_limit']),
                           "status_interval": str(jobargs['status_interval']),
                           "profile": str(int(jobargs['profile']))}

                # -- required modules --

                if "required_modules" in kwargs:
                        rm = kwargs["required_modules"]
                else:
                        functions = util.flatten(util.iterify(jobargs[f])
                                                 for f in self.mapreduce_functions)
                        rm = modutil.find_modules(
                                [f for f in functions if type(f) == types.FunctionType])

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
                        if type(kwargs["required_files"]) == dict:
                                rf.update(kwargs["required_files"])
                        else:
                                rf.update(util.pack_files(\
                                        kwargs["required_files"]))
                if rf:
                        request["required_files"] = marshal.dumps(rf)

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

                if "map" in kwargs:
                        if type(kwargs["map"]) == dict:
                                request["ext_map"] = marshal.dumps(kwargs["map"])
                        else:
                                request["map"] = marshal.dumps(kwargs["map"].func_code)

                        for function_name in ('map_init',
                                              'map_reader',
                                              'map_writer',
                                              'partition',
                                              'combiner'):
                                function = jobargs[function_name]
                                if function:
                                        request[function_name] = marshal.dumps(function.func_code)

                        parsed_inputs = []
                        for inp in inputs:
                                if type(inp) == list:
                                        parsed_inputs.append("\n".join(reversed(inp)))
                                elif inp.startswith("dir://"):
                                        parsed_inputs += util.parse_dir(inp)
                                else:
                                        parsed_inputs.append(inp)
                        inputs = parsed_inputs

                # -- only reduce --

                else:
                        ext_inputs = []
                        red_inputs = []
                        for inp in inputs:
                                if type(inp) == list:
                                        raise DiscoError("Reduce doesn't "\
                                                "accept redundant inputs")
                                elif inp.startswith("dir://"):
                                        red_inputs.append(inp)
                                else:
                                        ext_inputs.append(inp)
                        
                        if ext_inputs and red_inputs:
                                raise DiscoError("Can't mix partitioned "\
                                        "inputs with other inputs")
                        elif ext_inputs and kwargs["nr_reduces"] != 1:
                                raise DiscoError("nr_reduces must be 1 when "\
                                        "using non-partitioned inputs "\
                                        "without the map phase")
                        else:
                                inputs = ext_inputs or red_inputs

                request["input"] = " ".join(inputs)

                if "ext_params" in kwargs:
                        if type(kwargs["ext_params"]) == dict:
                                request["ext_params"] =\
                                        encode_netstring_fd(kwargs["ext_params"])
                        else:
                                request["ext_params"] = kwargs["ext_params"]

                # -- reduce --

                nr_reduces = jobargs['nr_reduces']
                if "reduce" in kwargs:
                        if type(kwargs["reduce"]) == dict:
                                request["ext_reduce"] = marshal.dumps(kwargs["reduce"])
                        else:
                                request["reduce"] = marshal.dumps(kwargs["reduce"].func_code)
                        nr_reduces = nr_reduces or min(max(nr_maps / 2, 1), 100)

                        for function_name in ('reduce_reader', 'reduce_writer', 'reduce_init'):
                                function = jobargs[function_name]
                                if function:
                                        request[function_name] = marshal.dumps(function.func_code)

                request["nr_reduces"] = str(nr_reduces)

                # -- encode and send the request --

                reply = self.master.request("/disco/job/new", encode_netstring_fd(request))

                if not reply.startswith('job started:'):
                        raise DiscoError("Failed to start a job. Server replied: " + reply)
                self.name = reply.split(':', 1)[1]

def result_iterator(results, notifier = None, proxy = None,
        reader = func.netstr_reader, input_stream = [func.map_input_stream],
        params = None):

        Task = settings.TaskEnvironment(result_iterator = True)
        for fun in input_stream:
                fun.func_globals.setdefault("Task", Task)

        res = []
        for dir_url in results:
                if dir_url.startswith("dir://"):
                        res += util.parse_dir(dir_url, proxy)
                else:
                        res.append(dir_url)

        for url in res:
                fd = sze = None
                for fun in input_stream:
                        fd, sze, url = fun(fd, sze, url, params)

                if notifier:
                        notifier(url)

                for x in reader(fd, sze, url):
                        yield x

