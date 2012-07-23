import fileinput, os, sys
import clx
import clx.server

from copy import copy
from optparse import OptionValueError, Option as Opt

def check_reify(option, opt, val):
    from disco.util import reify
    try:
        return reify(val)
    except Exception, e:
        raise OptionValueError('%s option: %s' % (opt, str(e)))

class Option(Opt):
    actions = ('setitem', 'setitem2')
    ACTIONS = Opt.ACTIONS + actions
    STORE_ACTIONS = Opt.STORE_ACTIONS + actions
    TYPED_ACTIONS = Opt.TYPED_ACTIONS + actions
    ALWAYS_TYPED_ACTIONS = Opt.ALWAYS_TYPED_ACTIONS + actions
    TYPES = Opt.TYPES + ('reify',)
    TYPE_CHECKER = copy(Opt.TYPE_CHECKER)
    TYPE_CHECKER['reify'] = check_reify

    def take_action(self, action, dest, opt, val, vals, parser):
        if action == 'setitem':
            key = opt.strip('-')
            vals.ensure_value(dest, {})[key] = val
        elif action == 'setitem2':
            key, val = val
            vals.ensure_value(dest, {})[key] = val
        else:
            Opt.take_action(self, action, dest, opt, val, vals, parser)

class OptionParser(clx.OptionParser):
    def __init__(self, **kwargs):
        clx.OptionParser.__init__(self, option_class=Option, **kwargs)
        self.add_option('-t', '--token',
                        help='authorization token to use for tags')

class Program(clx.Program):
    def __init__(self, *args, **kwargs):
        super(Program, self).__init__(*args, **kwargs)
        token = self.options.token
        if token is not None:
            self.settings['DDFS_READ_TOKEN']  = token
            self.settings['DDFS_WRITE_TOKEN'] = token

    @classmethod
    def add_classic_reads(cls, command):
        command.add_option('-R', '--reader',
                           help='input reader to import and use')
        command.add_option('-T', '--stream',
                           default='disco.func.default_stream',
                           help='input stream to import and use')
        return command

    @classmethod
    def add_ignore_missing(cls, command):
        command.add_option('-i', '--ignore-missing',
                           action='store_true',
                           help='ignore missing tags')
        return command

    @classmethod
    def add_job_mode(cls, command):
        from itertools import chain
        command.add_option('-j', '--job-mode',
                           action='store_true',
                           help='accept jobname arguments instead of urls')
        function = command.function
        def job_function(program, *jobnames):
            def results(jobname):
                status, results = program.disco.results(jobname)
                return results
            if program.options.job_mode:
                urls = chain(*(results(program.job_history(jobname))
                               for jobname in jobnames))
                return function(program, *urls)
            return function(program, *jobnames)
        job_function.__doc__ = function.__doc__
        command.function = job_function
        return command

    @classmethod
    def add_prefix_mode(cls, command):
        command.add_option('-p', '--prefix-mode',
                           action='store_true',
                           help='tags are interpreted as prefixes instead of names')
        return command

    @classmethod
    def add_program_blobs(cls, command):
        cls.add_ignore_missing(command)
        cls.add_prefix_mode(command)
        return command

    @classmethod
    def input(cls, *inputs):
        def maybe_list(seq):
            return seq[0] if len(seq) == 1 else seq
        def maybe_lists(inputs):
            return [maybe_list(i.split()) for i in inputs]
        return maybe_lists(inputs or fileinput.input(inputs))

    @classmethod
    def job_command(cls, function):
        def job_function(program, *jobnames):
            return function(program,
                            *(program.job_history(j) for j in jobnames))
        job_function.__doc__ = function.__doc__
        return cls.command(function.__name__)(job_function)

    def blobs(self, *tags):
        ignore_missing = self.options.ignore_missing
        for tag in self.prefix_mode(*tags):
            for replicas in self.ddfs.blobs(tag, ignore_missing=ignore_missing):
                yield replicas

    def default(self, program, *args):
        if args:
            raise Exception("unrecognized command: %s" % ' '.join(args))
        print self.disco

    def job_history(self, jobname):
        if jobname == '@':
            for offset, status, job in self.disco.joblist():
                return job
        elif jobname.startswith('@?'):
            for offset, status, job in self.disco.joblist():
                if jobname[2:] in job:
                    return job
        return jobname

    def prefix_mode(self, *tags):
        from itertools import chain
        if self.options.prefix_mode:
            return chain(match
                         for tag in tags
                         for match in self.ddfs.list(tag))
        return tags

    def separate_tags(self, *urls):
        from disco.util import partition
        from disco.ddfs import istag
        return partition(urls, istag)

    @property
    def ddfs(self):
        from disco.ddfs import DDFS
        return DDFS(settings=self.settings)

    @property
    def disco(self):
        from disco.core import Disco
        return Disco(settings=self.settings)

    @property
    def master(self):
        return Master(self.settings)

    @property
    def scheduler(self):
        return dict((k, eval(v)) for k, v in self.options.scheduler.items())

    @property
    def settings_class(self):
        from disco.settings import DiscoSettings
        return DiscoSettings

    @property
    def tests(self):
        for name in os.listdir(self.tests_path):
            if name.startswith('test_'):
                test, ext = os.path.splitext(name)
                if ext == '.py':
                    yield test

    @property
    def tests_path(self):
        return os.path.join(self.settings['DISCO_HOME'], 'tests')

class Master(clx.server.Server):
    def __init__(self, settings):
        super(Master, self).__init__(settings, settings['DISCO_ROTATE_LOG'])
        self.setid()
        settings.ensuredirs()

    @property
    def args(self):
        return self.basic_args + ['-detached', '-heart', '-kernel']

    @property
    def basic_args(self):
        settings = self.settings
        epath = lambda p: os.path.join(settings['DISCO_MASTER_HOME'], p)
        edep = lambda d: os.path.join(settings['DISCO_MASTER_HOME'], 'deps', d, 'ebin')
        def lager_config(log_dir):
            return ['-lager', 'handlers',
                    '[{lager_console_backend, info},'
                     '{lager_file_backend,'
                      '[{"%s/error.log", error, 1048576000, "$D0", 5},'
                       '{"%s/console.log", debug, 104857600, "$D0", 5}]}]' % (log_dir, log_dir),
                    '-lager', 'crash_log', '"%s/crash.log"' % (log_dir)]
        return settings['DISCO_ERLANG'].split() + \
               lager_config(settings['DISCO_LOG_DIR']) + \
               ['+K', 'true',
                '+P', '10000000',
                '-rsh', 'ssh',
                '-connect_all', 'false',
                '-sname', self.name,
                '-pa', epath('ebin'),
                '-pa', edep('mochiweb'),
                '-pa', edep('lager'),
                '-eval', 'application:start(disco)']

    @property
    def host(self):
        from socket import gethostname
        return gethostname()

    @property
    def port(self):
        return self.settings['DISCO_PORT']

    @property
    def log_dir(self):
        return self.settings['DISCO_LOG_DIR']

    @property
    def pid_dir(self):
        return self.settings['DISCO_PID_DIR']

    @property
    def env(self):
        setting_names = list(self.settings.defaults) + ['DISCO_SETTINGS']
        env = self.settings.env
        env.update({'DISCO_MASTER_PID': self.pid_file,
                    'DISCO_SETTINGS': ','.join(setting_names)})
        return env

    @property
    def name(self):
        return '%s_master' % self.settings['DISCO_NAME']

    @property
    def nodename(self):
        return '%s@%s' % (self.name, self.host.split('.', 1)[0])

    def nodaemon(self):
        return ('' for x in self.start(*self.basic_args))

    def setid(self):
        user = self.settings['DISCO_USER']
        if user != os.getenv('LOGNAME'):
            if os.getuid() != 0:
                raise Exception("Only root can change DISCO_USER")
            try:
                import pwd
                uid, gid, x, home = pwd.getpwnam(user)[2:6]
                os.setgid(gid)
                os.setuid(uid)
                os.environ['HOME'] = home
            except Exception, x:
                raise Exception("Could not switch to the user '%s'" % user)
