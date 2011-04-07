import fileinput, os, sys
import clx
import clx.server

class OptionParser(clx.OptionParser):
    def __init__(self, **kwargs):
        clx.OptionParser.__init__(self, **kwargs)
        self.add_option('-t', '--token',
                        help='authorization token to use for tags')
        self.jobargs = {}

    @classmethod
    def update_jobargs(cls, option, name, val, parser):
        parser.jobargs[name.strip('-')] = True if val is None else val

    @classmethod
    def add_file_mode(cls, command):
        command.add_option('-f', '--file-mode',
                           action='store_true',
                           help='urls specify files to read input from, or stdin')

    @classmethod
    def add_classic_reads(cls, command):
        command.add_option('-R', '--reader',
                           help='input reader to import and use')
        command.add_option('-T', '--stream',
                           default='disco.func.default_stream',
                           help='input stream to import and use')

    @classmethod
    def add_ignore_missing(cls, command):
        command.add_option('-i', '--ignore-missing',
                           action='store_true',
                           help='ignore missing tags')

    @classmethod
    def add_prefix_mode(cls, command):
        command.add_option('-p', '--prefix-mode',
                           action='store_true',
                           help='tags are interpreted as prefixes instead of names')

    @classmethod
    def add_program_blobs(cls, command):
        cls.add_ignore_missing(command)
        cls.add_prefix_mode(command)

class Program(clx.Program):
    def __init__(self, *args, **kwargs):
        super(Program, self).__init__(*args, **kwargs)
        token = self.options.token
        if token is not None:
            self.settings['DDFS_READ_TOKEN']  = token
            self.settings['DDFS_WRITE_TOKEN'] = token

    def blobs(self, *tags):
        ignore_missing = self.options.ignore_missing
        for tag in self.prefix_mode(*tags):
            for replicas in self.ddfs.blobs(tag, ignore_missing=ignore_missing):
                yield replicas

    def default(self, program, *args):
        if args:
            raise Exception("unrecognized command: %s" % ' '.join(args))
        print self.disco

    def file_mode(self, *urls):
        if self.options.file_mode:
            return fileinput.input(urls)
        return urls

    @classmethod
    def job_command(self, function):
        def job_function(program, *jobnames):
            return function(program,
                            *(program.job_history(j) for j in jobnames))
        job_function.__doc__ = function.__doc__
        return self.command(function.__name__)(job_function)

    def job_history(self, jobname):
        if jobname == '@':
            for offset, status, job in self.disco.joblist():
                return job
        elif jobname.startswith('@?'):
            for offset, status, job in self.disco.joblist():
                if jobname[2:] in job:
                    return job
        return jobname

    def job_input(self, *inputs):
        def maybe_list(seq):
            return seq[0] if len(seq) == 1 else seq
        return inputs or [maybe_list(line.split())
                          for line in fileinput.input(inputs)]

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
        super(Master, self).__init__(settings)
        self.setid()

    @property
    def args(self):
        return self.basic_args + ['-detached',
                                  '-heart',
                                  '-kernel', 'error_logger', '{file, "%s"}' % self.log_file]

    @property
    def basic_args(self):
        settings = self.settings
        ebin = lambda d: os.path.join(settings['DISCO_MASTER_HOME'], 'ebin', d)
        return settings['DISCO_ERLANG'].split() + \
               ['+K', 'true',
                '+P', '10000000',
                '-rsh', 'ssh',
                '-connect_all', 'false',
                '-sname', self.name,
                '-pa', ebin(''),
                '-pa', ebin('mochiweb'),
                '-pa', ebin('ddfs'),
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
        env = self.settings.env
        env.update({'DISCO_MASTER_PID': self.pid_file,
                    'DISCO_SETTINGS': ','.join(self.settings.defaults)})
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
