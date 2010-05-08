#!/usr/bin/env python

import optparse, os, subprocess, signal, socket, sys

from clx import OptionParser, Program
from clx.server import Server

class DiscoOptionParser(OptionParser):
    def __init__(self, **kwargs):
        OptionParser.__init__(self, **kwargs)
        self.add_option('-p', '--print-env',
                        action='store_true',
                        help='print the parsed disco environment and exit')

class Disco(Program):
    def default(self, program, *args):
        if args:
            raise Exception("unrecognized command: %s" % ' '.join(args))
        print "Disco master located at %s" % self.settings['DISCO_MASTER']

    def disco(self):
        from disco.core import Disco
        return Disco(self.settings['DISCO_MASTER'])

    def main(self):
        self.settings.ensuredirs()
        super(Disco, self).main()

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

class Master(Server):
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
                '-rsh', 'ssh',
                '-connect_all', 'false',
                '-sname', self.name,
                '-pa', ebin(''),
                '-pa', ebin('mochiweb'),
                '-pa', ebin('ddfs'),
                '-eval', 'application:start(disco)']

    @property
    def host(self):
        return socket.gethostname()

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
        env.update({'DISCO_MASTER_PID': self.pid_file})
        return env

    @property
    def name(self):
        return '%s_master' % self.settings['DISCO_NAME']

    @property
    def nodename(self):
        return '%s@%s' % (self.name, self.host.split('.', 1)[0])

    def nodaemon(self):
        return ('' for x in self.start(self.basic_args))

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

@Disco.command
def debug(program, host=''):
    master = program.master
    nodename = '%s@%s' % (master.name, host) if host else master.nodename
    args = program.settings['DISCO_ERLANG'].split() + \
           ['-remsh', nodename,
            '-sname', '%s_remsh' % os.getpid()]
    if subprocess.Popen(args).wait():
        raise Exception("Could not connect to %s (%s)" % (host, nodename))
    print "closing remote shell to %s (%s)" % (host, nodename)

@Disco.command
def nodaemon(program):
    for message in program.master.restart():
        print message

@Disco.command
def restart(program):
    for message in program.master.restart():
        print message

@Disco.command
def start(program):
    for message in program.master.start():
        print message

@Disco.command
def status(program):
    for message in program.master.status():
        print message

@Disco.command
def stop(program):
    for message in program.master.stop():
        print message

@Disco.command
def test(program, *tests):
    """Usage: [testname ...]

    Run the specified tests or the entire test suite if none are specified.
    """
    from disco.test import DiscoTestRunner
    if not tests:
        tests = list(program.tests)
    os.environ.update(program.settings.env)
    sys.path.insert(0, program.tests_path)
    DiscoTestRunner(program.settings).run(*tests)

if __name__ == '__main__':
    Disco(option_parser=DiscoOptionParser()).main()

    # Workaround for "disco test" in Python2.5 which doesn't shutdown the
    # test_server thread properly.
    sys.exit(0) # XXX still needed?
