import sys, re, os, marshal, modutil, time, types, cPickle, cStringIO, random
from disco import func, util, comm
from disco.comm import json
from disco.error import DiscoError, JobException
from disco.eventmonitor import EventMonitor
from disco.netstring import encode_netstring_fd, decode_netstring_fd

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
                except Exception, e:
                        raise DiscoError('Got %s, make sure disco master is running at %s' % (e, self.host))

        def nodeinfo(self):
                return json.loads(self.request("/disco/ctrl/nodeinfo"))

        def joblist(self):
                return json.loads(self.request("/disco/ctrl/joblist"))

        def oob_get(self, name, key):
                return util.load_oob(self.host, name, key)

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
                "combiner"]

        defaults = {"name": None,
                    "map": None,
                    "input": None,
                    "map_init": None,
                    "reduce_init": None,
                    "map_reader": func.map_line_reader,
                    "map_writer": func.netstr_writer,
                    "reduce_reader": func.netstr_reader,
                    "reduce_writer": func.netstr_writer,
                    "reduce": None,
                    "partition": func.default_partition,
                    "combiner": None,
                    "nr_maps": None,
                    "nr_reduces": None,
                    "sort": False,
                    "params": Params(),
                    "mem_sort_limit": 256 * 1024**2,
                    "ext_params": None,
                    "status_interval": 100000,
                    "required_files": [],
                    "required_modules": [],
                    "profile": False}

        def __init__(self, master, **kwargs):
                self.master = master
                if "name" not in kwargs:
                        raise DiscoError("Argument name is required")
                if re.search("\W", kwargs["name"]):
                        raise DiscoError("Only characters in [a-zA-Z0-9_] "\
                              "are allowed in the job name")
                self.name = "%s@%d" % (kwargs["name"], int(time.time()))
                self._run(**kwargs)

        def __getattr__(self, name):
                def r(f):
                        def g(*args, **kw):
                                return f(*tuple([self.name] + list(args)), **kw)
                        return g
                if name in ["kill", "clean", "purge", "jobspec", "results",
                            "jobinfo", "wait", "oob_get", "oob_list",
                            "profile_stats", "events"]:
                        return r(getattr(self.master, name))
                raise AttributeError("%s not found" % name)

        def _run(self, **kw):
                d = lambda x: kw.get(x, Job.defaults[x])

                # -- check parameters --

                # Backwards compatibility
                # (fun_map == map, input_files == input)
                if "fun_map" in kw:
                        kw["map"] = kw["fun_map"]

                if "input_files" in kw:
                        kw["input"] = kw["input_files"]

                if "chunked" in kw:
                        raise DiscoError("Argument 'chunked' is deprecated")

                if not "input" in kw:
                        raise DiscoError("input is required")

                if not ("map" in kw or "reduce" in kw):
                        raise DiscoError("Specify map and/or reduce")

                for p in kw:
                        if p not in Job.defaults:
                                raise DiscoError("Unknown argument: %s" % p)

                inputs = kw["input"]

                # -- initialize request --

                req = {"name": self.name,
                       "version": ".".join(map(str, sys.version_info[:2])),
                       "params": cPickle.dumps(d("params"), cPickle.HIGHEST_PROTOCOL),
                       "sort": str(int(d("sort"))),
                       "mem_sort_limit": str(d("mem_sort_limit")),
                       "status_interval": str(d("status_interval")),
                       "profile": str(int(d("profile")))}

                # -- required modules --

                if "required_modules" in kw:
                        rm = kw["required_modules"]
                else:
                        funlist = []
                        for f in Job.funs:
                                df = d(f)
                                if type(df) == types.FunctionType:
                                        funlist.append(df)
                                elif type(df) == list:
                                        funlist += df
                        rm = modutil.find_modules(funlist)
                send_mod = []
                imp_mod = []
                for mod in rm:
                        if type(mod) == tuple:
                                send_mod.append(mod[1])
                                mod = mod[0]
                        imp_mod.append(mod)

                req["required_modules"] = " ".join(imp_mod)
                rf = util.pack_files(send_mod)

                # -- required files --

                if "required_files" in kw:
                        if type(kw["required_files"]) == dict:
                                rf.update(kw["required_files"])
                        else:
                                rf.update(util.pack_files(\
                                        kw["required_files"]))
                if rf:
                        req["required_files"] = marshal.dumps(rf)

                # -- map --

                if "map" in kw:
                        if type(kw["map"]) == dict:
                                req["ext_map"] = marshal.dumps(kw["map"])
                        else:
                                req["map"] = marshal.dumps(kw["map"].func_code)

                        if "map_init" in kw:
                                req["map_init"] = marshal.dumps(\
                                        kw["map_init"].func_code)

                        req["map_reader"] =\
                                marshal.dumps(d("map_reader").func_code)
                        req["map_writer"] =\
                                marshal.dumps(d("map_writer").func_code)
                        req["partition"] =\
                                marshal.dumps(d("partition").func_code)

                        if "combiner" in kw:
                                req["combiner"] =\
                                        marshal.dumps(kw["combiner"].func_code)

                        parsed_inputs = []
                        for inp in inputs:
                                if type(inp) == list:
                                        parsed_inputs.append(
                                                "\n".join(reversed(inp)))
                                elif inp.startswith("dir://"):
                                        parsed_inputs += util.parse_dir(inp)
                                else:
                                        parsed_inputs.append(inp)
                        inputs = parsed_inputs

                        if "nr_maps" not in kw or kw["nr_maps"] > len(inputs):
                                nr_maps = len(inputs)
                        else:
                                nr_maps = kw["nr_maps"]

                # -- only reduce --

                else:
                        nr_maps = 0
                        ext_inputs = []
                        red_inputs = []
                        for inp in inputs:
                                if type(inp) == list:
                                        raise DiscoError("Reduce doesn't "\
                                                "accept redundant inputs")
                                elif inp.startswith("dir://"):
                                        if inp.endswith(".txt"):
                                                ext_inputs.append(inp)
                                        else:
                                                red_inputs.append(inp)
                                else:
                                        ext_inputs.append(inp)

                        if ext_inputs and red_inputs:
                                raise DiscoError("Can't mix partitioned "\
                                        "inputs with other inputs")
                        elif red_inputs:
                                q = lambda x: int(x.split(":")[-1]) + 1
                                nr_red = q(red_inputs[0])
                                for x in red_inputs:
                                        if q(x) != nr_red:
                                                raise DiscoError(\
                                                "Number of partitions must "\
                                                "match in all inputs")
                                n = d("nr_reduces") or nr_red
                                if n != nr_red:
                                        raise DiscoError(
                                        "Specified nr_reduces = %d but "\
                                        "number of partitions in the input "\
                                        "is %d" % (n, nr_red))
                                kw["nr_reduces"] = nr_red
                                inputs = red_inputs
                        elif d("nr_reduces") != 1:
                                raise DiscoError("nr_reduces must be 1 when "\
                                        "using non-partitioned inputs "\
                                        "without the map phase")
                        else:
                                inputs = ext_inputs

                # shuffle fixes a pathological case in the fifo scheduler:
                # if inputs for a node are consequent, data locality will be
                # lost after K inputs where K is the number of cores.
                # Randomizing the order of inputs makes this pathological case
                # unlikely. This issue will be fixed in the new scheduler.
                random.shuffle(inputs)

                req["input"] = " ".join(inputs)
                req["nr_maps"] = str(nr_maps)

                if "ext_params" in kw:
                        if type(kw["ext_params"]) == dict:
                                req["ext_params"] =\
                                        encode_netstring_fd(kw["ext_params"])
                        else:
                                req["ext_params"] = kw["ext_params"]

                # -- reduce --

                nr_reduces = d("nr_reduces")
                if "reduce" in kw:
                        if type(kw["reduce"]) == dict:
                                req["ext_reduce"] = marshal.dumps(kw["reduce"])
                                req["reduce"] = ""
                        else:
                                req["reduce"] = marshal.dumps(
                                        kw["reduce"].func_code)
                        nr_reduces = nr_reduces or min(max(nr_maps / 2, 1), 100)

                        req["reduce_reader"] =\
                                marshal.dumps(d("reduce_reader").func_code)
                        req["reduce_writer"] =\
                                marshal.dumps(d("reduce_writer").func_code)

                        if "reduce_init" in kw:
                                req["reduce_init"] = marshal.dumps(\
                                        kw["reduce_init"].func_code)
                else:
                        nr_reduces = nr_reduces or 0

                req["nr_reduces"] = str(nr_reduces)

                # -- encode and send the request --

                self.msg = encode_netstring_fd(req)
                reply = self.master.request("/disco/job/new", self.msg)

                if reply != "job started":
                        raise DiscoError("Failed to start a job. Server replied: " + reply)



def result_iterator(results, notifier = None,\
        proxy = None, reader = func.netstr_reader):

        res = []
        for dir_url in results:
                if dir_url.startswith("dir://"):
                        res += util.parse_dir(dir_url, proxy)
                else:
                        res.append(dir_url)

        x, x, root = util.load_conf()

        for url in res:
                if url.startswith("file://"):
                        fname = url[7:]
                        fd = file(fname)
                        sze = os.stat(fname).st_size
                elif url.startswith("disco://"):
                        host, fname = url[8:].split("/", 1)
                        url = util.proxy_url(proxy, fname, host)
                        if util.resultfs_enabled:
                                f = "%s/data/%s" % (root, fname)
                                fd = file(f)
                                sze = os.stat(f).st_size
                        else:
                                sze, fd = comm.open_remote(url)
                else:
                        raise JobException("Invalid result url: %s" % url)

                if notifier:
                        notifier(url)

                for x in reader(fd, sze, fname):
                        yield x

