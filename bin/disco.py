#!/usr/bin/env python

import optparse, os, subprocess, signal, socket, sys
from itertools import chain

class DiscoError(Exception):
    pass

class server(object):
    def __init__(self, disco_settings, port=None):
        self.disco_settings = disco_settings
        self.host = socket.gethostname()
        self.port = port

    def setid(self):
        user = self.disco_settings['DISCO_USER']
        if user != os.getenv('LOGNAME'):
            if os.getuid() != 0:
                raise DiscoError("Only root can change DISCO_USER")
            try:
                import pwd
                uid, gid, x, home = pwd.getpwnam(user)[2:6]
                os.setgid(gid)
                os.setuid(uid)
                os.environ['HOME'] = home
            except Exception, x:
                raise DiscoError("Could not switch to the user '%s'" % user)

    @property
    def env(self):
        return self.disco_settings.env

    @property
    def id(self):
        return self.__class__.__name__, self.host, self.port

    @property
    def pid(self):
        return int(open(self.pid_file).readline().strip())

    @property
    def log_file(self):
        return os.path.join(self.disco_settings['DISCO_LOG_DIR'], '%s-%s_%s.log' % self.id)

    @property
    def pid_file(self):
        return os.path.join(self.disco_settings['DISCO_PID_DIR'], '%s-%s_%s.pid' % self.id)

    def conf_path(self, filename):
        return os.path.join(self.disco_settings['DISCO_CONF'], filename)

    def restart(self):
        return chain(self.stop(), self.start())

    def send(self, command):
        return getattr(self, command)()

    def start(self, args=None, **kwargs):
        self.assert_status('stopped')
        if not args:
            args = self.args
        try:
            process = subprocess.Popen(args, env=self.env, **kwargs)
        except OSError, x:
            if x.errno == 2:
                    raise DiscoError("%s not found. "\
                            "Is it in your PATH?" % args[0])
            else:
                    raise
        if process.wait():
            raise DiscoError("Failed to start %s" % self)
        yield '%s started' % self

    def assert_status(self, status):
        if self._status != status:
            raise DiscoError("%s already %s" % (self, self._status))

    @property
    def _status(self):
        try:
            os.getpgid(self.pid)
            return 'running'
        except Exception:
            return 'stopped'

    def status(self):
        yield '%s %s' % (self, self._status)

    def stop(self):
        try:
            os.kill(self.pid, signal.SIGTERM)
            while self._status == 'running':
                pass
        except Exception:
            pass
        for msg in chain(self.status()):
            yield msg

    def __str__(self):
        return ' '.join(self.args)

class lighttpd(server):
    def __init__(self, disco_settings, port, config_file):
        super(lighttpd, self).__init__(disco_settings, port)
        self.config_file = config_file

    @property
    def args(self):
        return [self.disco_settings['DISCO_HTTPD'], '-f', self.config_file]

    @property
    def env(self):
        env = self.disco_settings.env
        env.update({'DISCO_HTTP_LOG': self.log_file,
                    'DISCO_HTTP_PID': self.pid_file,
                    'DISCO_HTTP_PORT': str(self.port)})
        return env

class master(server):
    def __init__(self, disco_settings):
        super(master, self).__init__(disco_settings, disco_settings['DISCO_SCGI_PORT'])
        self.setid()

    @property
    def args(self):
        return self.basic_args + ['-detached',
                                  '-heart',
                                  '-kernel', 'error_logger', '{file, "%s"}' % self.log_file]
    @property
    def basic_args(self):
        settings = self.disco_settings
        return settings['DISCO_ERLANG'].split() + \
               ['+K', 'true',
                '-rsh', 'ssh',
                '-connect_all', 'false',
                '-pa', os.path.join(settings['DISCO_MASTER_HOME'], 'ebin'),
                '-sname', self.name,
                '-eval', '[handle_job, handle_ctrl]',
                '-eval', 'application:start(disco)']

    @property
    def env(self):
        env = self.disco_settings.env
        env.update({'DISCO_MASTER_PID': self.pid_file})
        return env

    @property
    def lighttpd(self):
        return lighttpd(self.disco_settings,
                        self.disco_settings['DISCO_PORT'],
                        self.conf_path('lighttpd-master.conf'))

    @property
    def name(self):
        return '%s_master' % self.disco_settings['DISCO_NAME']

    @property
    def nodename(self):
        return '%s@%s' % (self.name, self.host.split('.', 1)[0])

    def nodaemon(self):
        return chain(self.lighttpd.start(),
                     ('' for x in self.start(self.basic_args)), # suppress output
                     self.lighttpd.stop())

    def send(self, command):
        if command in ('nodaemon', 'remsh'):
            return getattr(self, command)()
        return chain(getattr(self, command)(), self.lighttpd.send(command))

    def __str__(self):
        return 'disco master'

class worker(server):
    def __init__(self, disco_settings):
        super(worker, self).__init__(disco_settings)
        self.setid()

    @property
    def lighttpd(self):
        return lighttpd(self.disco_settings,
                        self.disco_settings['DISCO_PORT'],
                        self.conf_path('lighttpd-worker.conf'))

    @property
    def name(self):
        return '%s_slave' % self.disco_settings['DISCO_NAME']

    def send(self, command):
        return self.lighttpd.send(command)

class debug(object):
    def __init__(self, disco_settings):
        self.disco_settings = disco_settings

    @property
    def name(self):
        return '%s_remsh' % os.getpid()

    def send(self, command):
        discomaster = master(self.disco_settings)
        nodename = discomaster.nodename
        if command != 'status':
            nodename = '%s@%s' % (discomaster.name, command)
        args = self.disco_settings['DISCO_ERLANG'].split() + ['-remsh', nodename,
                                                        '-sname', self.name]
        if subprocess.Popen(args).wait():
            raise DiscoError("Could not connect to %s (%s)" % (command, nodename))
        yield 'closing remote shell to %s (%s)' % (command, nodename)

class test(object):
    def __init__(self, disco_settings):
        self.disco_settings = disco_settings

    @property
    def tests_path(self):
        return os.path.join(self.disco_settings['DISCO_HOME'], 'tests')

    @property
    def all_tests(self):
        for name in os.listdir(self.tests_path):
            if name.startswith('test_'):
                test, ext = os.path.splitext(name)
                if ext == '.py':
                    yield test

    def send(self, *names):
        from disco.test import DiscoTestRunner
        os.environ.update(self.disco_settings.env)
        sys.path.insert(0, self.tests_path)
        yield 'searching for names in sys.path:\n%s' % sys.path
        if names == ('status', ):
            names = list(self.all_tests)
        DiscoTestRunner(self.disco_settings).run(*names)

def main():
    DISCO_BIN  = os.path.dirname(os.path.realpath(__file__))
    DISCO_HOME = os.path.dirname(DISCO_BIN)
    DISCO_CONF = os.path.join(DISCO_HOME, 'conf')
    DISCO_PATH = os.path.join(DISCO_HOME, 'pydisco')

    if not os.path.exists(DISCO_CONF):
        DISCO_CONF = "/etc/disco"

    # disco.py and the disco package are ambiguous. Move the local directory
    # to the end of the list, to make sure that the disco package is preferred
    # over disco.py.
    sys.path.append(sys.path.pop(0))
    # Prefer local Disco over system-wide installation
    sys.path.insert(0, DISCO_PATH)

    from disco.settings import DiscoSettings

    usage = """
            %prog [options] [master|worker] [start|stop|restart|status]
            %prog [options] master nodaemon
            %prog [options] debug [hostname]
            """
    option_parser = optparse.OptionParser(usage=usage)
    option_parser.add_option('-s', '--settings',
                             default=os.path.join(DISCO_CONF, 'settings.py'),
                             help='use settings file settings')
    option_parser.add_option('-v', '--verbose',
                             action='store_true',
                             help='print debugging messages')
    option_parser.add_option('-p', '--print-env',
                             action='store_true',
                             help='print the parsed disco environment and exit')
    options, sys.argv = option_parser.parse_args()

    disco_settings = DiscoSettings(options.settings,
                                   DISCO_BIN=DISCO_BIN,
                                   DISCO_HOME=DISCO_HOME,
                                   DISCO_CONF=DISCO_CONF,
                                   DISCO_PATH=DISCO_PATH)

    if options.verbose:
        print(
            """
            It seems that Disco is at {DISCO_HOME}
            Disco settings are at {0}

            If this is not what you want, see the `--help` option
            """.format(options.settings, **disco_settings)) # python2.6+

    if options.print_env:
        for item in sorted(disco_settings.env.iteritems()):
            print('%s = %s' % (item))
        sys.exit(0)

    argdict      = dict(enumerate(sys.argv))
    disco_object = globals()[argdict.pop(0, 'master')](disco_settings)

    for name in disco_settings.must_exist:
        path = disco_settings[name]
        if not os.path.exists(path):
            os.makedirs(path)

    command, args = argdict.pop(1, 'status'), sys.argv[2:]
    for message in disco_object.send(command, *args):
        if options.verbose or command == 'status':
            print(message)

if __name__ == '__main__':
    try:
        main()
    except DiscoError, e:
        sys.exit(e)
    except Exception, e:
        print('Disco encountered a fatal system error:')
        raise
    # Workaround for "disco test" in Python2.5 which doesn't shutdown the 
    # tserver thread properly.
    sys.exit(0)
