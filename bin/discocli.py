#!/usr/bin/env python
"""
:mod:`disco <discocli>` -- Disco command line utility
=====================================================

:program:`disco` is a fully-Python startup/configuration script which supports several exciting features.
The new startup script makes it even easier to get up and running with a Disco cluster.

.. note::

   This is the manpage for the :program:`disco` command.
   Please see :ref:`setup` for more information on installing Disco.

.. hint::

   The documentation assumes that the executable ``$DISCO_HOME/bin/disco`` is on your system path.
   If it is not on your path, you can add it::

        ln -s $DISCO_HOME/bin/disco /usr/local/bin

   If ``/usr/local/bin`` is not in your ``$PATH``, use an appropriate replacement.
   Doing so allows you to simply call :program:`disco`, instead of specifying the complete path.


Run :command:`disco help` for information on using the command line utility.

See also: :mod:`disco.settings`

"""

import os, sys
import fileinput

if '.disco-home' in os.listdir('.'):
    sys.path.append('lib')

from clx import OptionParser, Program
from clx.server import Server

class DiscoOptionParser(OptionParser):
    def __init__(self, **kwargs):
        OptionParser.__init__(self, **kwargs)
        self.add_option('-k', '--sort-stats',
                        action='append',
                        default=[],
                        help='keys to use for sorting profiling statistics')
        self.add_option('-o', '--offset',
                        default=0,
                        help='offset to use in requests to disco master')
        self.add_option('-S', '--status',
                        action='store_true',
                        help='show job status when printing jobs')
        self.add_option('-n', '--name',
                        help='prefix to use for submitting a job')
        self.add_option('--save',
                        action='callback',
                        callback=self.update_jobdict,
                        help='save results to DDFS')
        self.add_option('--sort',
                        action='callback',
                        callback=self.update_jobdict,
                        help='sort input to reduce')
        self.add_option('--profile',
                        action='callback',
                        callback=self.update_jobdict,
                        help='enable job profiling')
        self.add_option('--param',
                        action='append',
                        default=[],
                        dest='params',
                        nargs=2,
                        help='add a job parameter')
        self.add_option('--partitions',
                        action='callback',
                        callback=self.update_jobdict,
                        type='int',
                        help='enable job profiling')
        self.add_option('--sched_max_cores',
                        action='callback',
                        callback=self.update_jobdict,
                        type='int',
                        help='enable job profiling')
        self.add_option('--status_interval',
                        action='callback',
                        callback=self.update_jobdict,
                        type='int',
                        help='how often to report status in job')
        self.jobdict = {}

    def update_jobdict(self, option, name, val, parser):
        self.jobdict[name.strip('-')] = True if val is None else val

class Disco(Program):
    def default(self, program, *args):
        if args:
            raise Exception("unrecognized command: %s" % ' '.join(args))
        print "Disco master located at %s" % self.settings['DISCO_MASTER']

    def main(self):
        super(Disco, self).main()

    @property
    def disco(self):
        from disco.core import Disco
        return Disco(self.settings['DISCO_MASTER'])

    @property
    def master(self):
        master = Master(self.settings)
        self.settings.ensuredirs()
        return master

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
        env.update({'DISCO_MASTER_PID': self.pid_file})
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

@Disco.command
def debug(program, host=''):
    """Usage: [host]

    Connect to master Erlang process via remote shell.
    Host is only necessary when master is running on a remote machine.
    """
    from subprocess import Popen
    master = program.master
    nodename = '%s@%s' % (master.name, host) if host else master.nodename
    args = program.settings['DISCO_ERLANG'].split() + \
           ['-remsh', nodename,
            '-sname', '%s_remsh' % os.getpid()]
    if Popen(args).wait():
        raise Exception("Could not connect to %s (%s)" % (host, nodename))
    print "closing remote shell to %s (%s)" % (host, nodename)

@Disco.command
def help(program, *args):
    """Usage: [command]

    Print program or command help.
    """
    command, leftover = program.search(args)
    print command

@Disco.command
def nodaemon(program):
    """
    Start the master in the current process.
    The Erlang shell is opened and log messages are printed to stdout.
    Note: quitting the shell will stop the master.
    """
    for message in program.master.nodaemon():
        print message

@Disco.command
def restart(program):
    """
    Restart the master.
    """
    for message in program.master.restart():
        print message

@Disco.command
def start(program):
    """
    Start the master.
    """
    for message in program.master.start():
        print message

@Disco.command
def status(program):
    """
    Display running state of the master.
    """
    for message in program.master.status():
        print message

@Disco.command
def stop(program):
    """
    Stop the master.
    """
    for message in program.master.stop():
        print message

@Disco.command
def test(program, *tests):
    """Usage: [testname ...]

    Run the specified tests or the entire test suite if none are specified.

    Assumes Disco master is already running and configured.
    Test names is an optional list of names of modules in the ``$DISCO_HOME/tests`` directory (e.g. ``test_simple``).
    Test names may also include the names of specific test cases (e.g. ``test_sort.MemorySortTestCase``).
    """
    from disco.test import DiscoTestRunner
    if not tests:
        tests = list(program.tests)
    os.environ.update(program.settings.env)
    sys.path.insert(0, program.tests_path)
    DiscoTestRunner(program.settings).run(*tests)

@Disco.command
def config(program):
    """Usage:

    Print the disco master configuration.
    """
    for config in program.disco.config:
        print "\t".join(config)

@Disco.command
def deref(program, *files):
    """Usage: [file ...]

    Dereference the dir:// urls in file[s] or stdin and print them to stdout.
    """
    from disco.util import parse_dir
    for line in fileinput.input(files):
        for url in parse_dir(line.strip()):
            print url

@Disco.command
def events(program, jobname):
    """Usage: [-o offset] jobname

    Print the events for the named job.
    """
    print program.disco.rawevents(jobname, offset=int(program.options.offset))

@Disco.command
def jobdict(program, jobname):
    """Usage: jobname

    Print the jobdict for the named job.
    """
    print jobname
    for key, value in program.disco.jobdict(jobname).iteritems():
        print "\t%s\t%s" % (key, value)

@Disco.command
def jobs(program):
    """Usage: [-S]

    Print a list of disco jobs and optionally their statuses.
    """
    for offset, status, job in program.disco.joblist():
        print "%s\t%s" % (job, status) if program.options.status else job

@Disco.command
def kill(program, *jobnames):
    """Usage: jobname ...

    Kill the named jobs.
    """
    for jobname in jobnames:
        program.disco.kill(jobname)

@Disco.command
def mapresults(program, jobname):
    """Usage: jobname

    Print the list of results from the map phase of a job.
    This is useful for resuming a job which has failed during reduce.
    """
    for result in program.disco.mapresults(jobname):
        print result

@Disco.command
def nodeinfo(program):
    """Usage:

    Print the node information.
    """
    for item in program.disco.nodeinfo().iteritems():
        print '%s\t%s' % item

@Disco.command
def oob(program, jobname):
    """Usage: jobname

    Print the oob keys for the named job.
    """
    from disco.core import Job
    for key in Job(program.disco, jobname).oob_list():
        print key

@oob.subcommand
def get(program, key, jobname):
    """Usage: key jobname

    Print the oob value for the given key and jobname.
    """
    from disco.core import Job
    print Job(program.disco, jobname).oob_get(key)

@Disco.command
def pstats(program, jobname):
    """Usage: jobname [-k sort-key]

    Print the profiling statistics for the named job.
    Assumes the job was run with profile flag enabled.
    """
    sort_stats = program.options.sort_stats or ['cumulative']
    program.disco.profile_stats(jobname).sort_stats(*sort_stats).print_stats()

@Disco.command
def purge(program, *jobnames):
    """Usage: jobname ...

    Purge the named jobs.
    """
    for jobname in jobnames:
        program.disco.purge(jobname)

@Disco.command
def results(program, jobname):
    """Usage: jobname

    Print the list of results for a completed job.
    """
    status, results = program.disco.results(jobname)
    for result in results:
           print result

@Disco.command
def run(program, jobclass, *inputs):
    """Usage: jobclass [-n name] [--save] [--sort] [--profile] [--partitions P] [--sched_max_cores C] [--status_interval I] [input ...]

    Create an instance of jobclass and run it.
    Input urls are specified as arguments or read from stdin.
    """
    from disco.core import Params
    from disco.util import reify
    def maybe_list(seq):
        return seq[0] if len(seq) == 1 else seq
    name = program.options.name or jobclass.split('.')[-1]
    input = inputs or [maybe_list(line.split())
                       for line in fileinput.input(inputs)]
    job = reify(jobclass)(program.disco, name)

    try:
        params = job.params
    except AttributeError:
        params = Params()
    params.__dict__.update(**dict(program.options.params))

    job.run(input=input, **program.option_parser.jobdict)
    print job.name

@Disco.command
def wait(program, jobname):
    """Usage: jobname

    Wait for the named job to complete.
    """
    program.disco.wait(jobname)

if __name__ == '__main__':
    Disco(option_parser=DiscoOptionParser()).main()

    # Workaround for "disco test" in Python2.5 which doesn't shutdown the
    # test_server thread properly.
    sys.exit(0) # XXX still needed?
