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

import fileinput, os, sys

if '.disco-home' in os.listdir('.'):
    sys.path.append('lib')

from disco.cli import OptionParser, Program

class Disco(Program):
    pass

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
def nodaemon(program):
    """
    Start the master in the current process.
    The Erlang shell is opened and log messages are printed to stdout.
    Note: quitting the shell will stop the master.
    """
    program.settings.ensuredirs()
    for message in program.master.nodaemon():
        print message

@Disco.command
def restart(program):
    """
    Restart the master.
    """
    program.settings.ensuredirs()
    for message in program.master.restart():
        print message

@Disco.command
def start(program):
    """
    Start the master.
    """
    program.settings.ensuredirs()
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
    from disco.test import TestRunner
    if not tests:
        tests = list(program.tests)
    os.environ.update(program.settings.env)
    sys.path.insert(0, program.tests_path)
    TestRunner(program.settings).run(*tests)

@Disco.command
def config(program):
    """

    Print the disco master configuration.
    """
    for config in program.disco.config:
        print "\t".join(config)

@Disco.command
def deref(program, *urls):
    """Usage: [url ...]

    Dereference the dir:// urls and print them to stdout.
    """
    from disco.util import parse_dir
    for line in program.file_mode(*urls):
        for url in parse_dir(line.strip()):
            print url

OptionParser.add_file_mode(deref)

@Disco.job_command
def events(program, jobname):
    """Usage: jobname

    Print the events for the named job.
    """
    print program.disco.rawevents(jobname, offset=int(program.options.offset))

events.add_option('-o', '--offset',
                  default=0,
                  help='offset to use in requests to disco master')

@Disco.job_command
def jobdict(program, jobname):
    """Usage: jobname

    Print the jobdict for the named job.
    """
    print jobname
    for key, value in program.disco.jobpack(jobname).jobdict.iteritems():
        print "\t%s\t%s" % (key, value)

@Disco.command
def jobs(program):
    """
    Print a list of disco jobs and optionally their statuses.
    """
    for offset, status, job in program.disco.joblist():
        print "%s\t%s" % (job, status) if program.options.status else job

jobs.add_option('-S', '--status',
                action='store_true',
                help='show job status when printing jobs')

@Disco.job_command
def kill(program, *jobnames):
    """Usage: jobname ...

    Kill the named jobs.
    """
    for jobname in jobnames:
        program.disco.kill(jobname)

@Disco.job_command
def mapresults(program, jobname):
    """Usage: jobname

    Print the list of results from the map phase of a job.
    This is useful for resuming a job which has failed during reduce.
    """
    from disco.util import iterify
    for result in program.disco.mapresults(jobname):
        print '\t'.join('%s' % (e,) for e in iterify(result)).rstrip()

@Disco.command
def nodeinfo(program):
    """Usage:

    Print the node information.
    """
    for item in program.disco.nodeinfo().iteritems():
        print '%s\t%s' % item

@Disco.job_command
def oob(program, jobname):
    """Usage: jobname

    Print the oob keys for the named job.
    """
    from disco.core import Job
    for key in Job(name=jobname, master=program.disco).oob_list():
        print key

@oob.subcommand
def get(program, key, jobname):
    """Usage: key jobname

    Print the oob value for the given key and jobname.
    """
    from disco.core import Job
    print Job(name=program.job_history(jobname), master=program.disco).oob_get(key)

@Disco.job_command
def pstats(program, jobname):
    """Usage: jobname

    Print the profiling statistics for the named job.
    Assumes the job was run with profile flag enabled.
    """
    sort_stats = program.options.sort_stats or ['cumulative']
    program.disco.profile_stats(jobname).sort_stats(*sort_stats).print_stats()

pstats.add_option('-k', '--sort-stats',
                  action='append',
                  default=[],
                  help='keys to use for sorting profiling statistics')

@Disco.job_command
def purge(program, *jobnames):
    """Usage: jobname ...

    Purge the named jobs.
    """
    for jobname in jobnames:
        program.disco.purge(jobname)

@Disco.job_command
def results(program, jobname):
    """Usage: jobname

    Print the list of results for a completed job.
    """
    from disco.util import iterify
    status, results = program.disco.results(jobname)
    for result in results:
        print '\t'.join('%s' % (e,) for e in iterify(result)).rstrip()

@Disco.command
def run(program, jobclass, *inputs):
    """Usage: jobclass [input ...]

    Create an instance of jobclass and run it.
    Input urls are specified as arguments or read from stdin.
    """
    from disco.util import reify
    def maybe_list(seq):
        return seq[0] if len(seq) == 1 else seq
    input = inputs or [maybe_list(line.split())
                       for line in fileinput.input(inputs)]
    sys.path.insert(0, '')
    job = reify(jobclass)(name=program.options.name,
                          master=program.disco,
                          settings=program.settings)
    job.run(input=input,
            params=dict(program.options.params),
            **program.option_parser.jobargs)
    print job.name

run.add_option('-n', '--name',
                help='prefix to use for submitting a job')
run.add_option('--save',
                action='callback',
                callback=OptionParser.update_jobargs,
                help='save results to DDFS')
run.add_option('--sort',
                action='callback',
                callback=OptionParser.update_jobargs,
                help='sort input to reduce')
run.add_option('--profile',
                action='callback',
                callback=OptionParser.update_jobargs,
                help='enable job profiling')
run.add_option('--param',
                action='append',
                default=[],
                dest='params',
                nargs=2,
                help='add a job parameter')
run.add_option('--partitions',
                action='callback',
                callback=OptionParser.update_jobargs,
                type='int',
                help='enable job profiling')
run.add_option('--sched_max_cores',
                action='callback',
                callback=OptionParser.update_jobargs,
                type='int',
                help='enable job profiling')
run.add_option('--status_interval',
                action='callback',
                callback=OptionParser.update_jobargs,
                type='int',
                help='how often to report status in job')

@Disco.job_command
def wait(program, jobname):
    """Usage: jobname

    Wait for the named job to complete and print the list of results.
    """
    from disco.util import iterify
    for result in program.disco.wait(jobname):
        print '\t'.join('%s' % (e,) for e in iterify(result)).rstrip()

if __name__ == '__main__':
    Disco(option_parser=OptionParser()).main()

    # Workaround for "disco test" in Python2.5 which doesn't shutdown the
    # test_server thread properly.
    sys.exit(0) # XXX still needed?
