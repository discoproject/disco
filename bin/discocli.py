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

.. seealso::

        The :mod:`ddfs <ddfscli>` command.

        See :mod:`disco.settings` for information about Disco settings.

.. _jobhistory:

Job History
-----------

For commands which take a jobname, or which support :option:`-j`,
the special arguments ``@`` and ``@?<string>``
are replaced by the most recent job name and
the most recent job with name matching ``<string>``, respectively.

For example::

        disco results @

Would get the results for the most recent job, and::

        disco results @?WordCount

Would get the results for the last job with name containing ``WordCount``.
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
    from disco.test import TestRunner
    if not tests:
        tests = list(program.tests)
    sys.path.insert(0, program.tests_path)
    TestRunner(program.settings).run(*tests)

@Disco.command
def config(program):
    """

    Print the disco master configuration.
    """
    for config in program.disco.config:
        print "\t".join(config)

@Disco.add_job_mode
@Disco.command
def deref(program, *urls):
    """Usage: [url ...]

    Dereference the urls and print them to stdout.
    Input urls are specified as arguments or read from stdin.
    """
    from disco.util import deref
    for input in deref(program.input(*urls), resolve=program.options.resolve):
        print "\t".join(input)

deref.add_option('-r', '--resolve',
                 action='store_true',
                 help='resolve disco internal urls')

@Disco.job_command
def events(program, jobname):
    """Usage: jobname

    Print the events for the named job.
    """
    print program.disco.rawevents(jobname, offset=int(program.options.offset))

events.add_option('-o', '--offset',
                  default=0,
                  help='offset to use in requests to disco master')

@Disco.command
def job(program, worker, *inputs):
    """Usage: worker [input ...]

    Create a jobpack and submit it to the master.
    Worker is automatically added to the jobhome.
    Input urls are specified as arguments or read from stdin.
    """
    from disco.fileutils import DiscoZipFile
    from disco.job import JobPack
    def jobzip(*paths):
        jobzip = DiscoZipFile()
        for path in paths:
            jobzip.writepath(path)
        jobzip.close()
        return jobzip
    def jobdata(data):
        if data.startswith('@'):
            return open(data[1:]).read()
        return data
    def prefix(p):
        return p or os.path.basename(worker).split(".")[0]
    jobdict = {'input': program.input(*inputs),
               'worker': worker,
               'map?': program.options.has_map,
               'reduce?': program.options.has_reduce,
               'nr_reduces': program.options.nr_reduces,
               'prefix': prefix(program.options.prefix),
               'scheduler': program.scheduler,
               'owner': program.options.owner or program.settings['DISCO_JOB_OWNER']}
    jobenvs = dict(program.options.env)
    jobzip  = jobzip(worker, *program.options.files)
    jobdata = jobdata(program.options.data)
    jobpack = JobPack(jobdict, jobenvs, jobzip.dumps(), jobdata)
    if program.options.verbose:
        print "jobdict:"
        print "\n".join("\t%s\t%s" % item for item in jobdict.items())
        print "jobenvs:"
        print "\n".join("\t%s\t%s" % item for item in jobenvs.items())
        print "jobzip:"
        print "\n".join("\t%s" % name for name in jobzip.namelist())
        print "jobdata:"
        print "\n".join("\t%s" % line for line in jobdata.splitlines())
    if program.options.dump_jobpack:
        print jobpack.dumps()
    else:
        print program.disco.submit(jobpack.dumps())

job.add_option('-m', '--has-map',
               action='store_true',
               help='sets the map phase flag of the jobdict')
job.add_option('-r', '--has-reduce',
               action='store_true',
               help='sets the reduce phase flag of the jobdict')
job.add_option('-n', '--nr-reduces',
               default=1,
               type='int',
               help='number of reduces in the reduce phase')
job.add_option('-o', '--owner',
               help='owner of the job')
job.add_option('-p', '--prefix',
               default=None,
               help='prefix to use when naming the job')
job.add_option('-S', '--scheduler',
               action='setitem2',
               default={},
               nargs=2,
               help='add a param to the scheduler field of the jobdict')
job.add_option('-e', '--env',
               action='append',
               default=[],
               nargs=2,
               help='add a variable to jobenvs')
job.add_option('-f', '--file',
               action='append',
               default=[],
               dest='files',
               help='path to add to the jobhome (recursively adds directories)')
job.add_option('-d', '--data',
               default='',
               help='additional binary jobdata, read from a file if it starts with "@"')
job.add_option('-D', '--dump-jobpack',
               action='store_true',
               help='dump the jobpack without submitting it to the master')

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
    from disco.job import Job
    for key in Job(name=jobname, master=program.disco).oob_list():
        print key

@oob.subcommand
def get(program, key, jobname):
    """Usage: key jobname

    Print the oob value for the given key and jobname.
    """
    from disco.job import Job
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
    sys.path.insert(0, '')
    job = reify(jobclass)(name=program.options.name,
                          master=program.disco,
                          settings=program.settings)
    input = program.input(*inputs)
    if any(input):
        program.options.jobargs['input'] = input
    if program.options.scheduler:
        program.options.jobargs['scheduler'] = program.scheduler
    job.run(**program.options.jobargs)
    print job.name

run.add_option('-n', '--name',
               help='prefix to use for submitting a job')
run.add_option('-m', '--map',
               action='setitem',
               dest='jobargs',
               type='reify',
               help='the worker map parameter')
run.add_option('-r', '--reduce',
               action='setitem',
               dest='jobargs',
               type='reify',
               help='the worker reduce parameter')
run.add_option('--save',
               action='setitem',
               dest='jobargs',
               type='reify',
               help='save results to DDFS?')
run.add_option('--profile',
               action='setitem',
               dest='jobargs',
               type='reify',
               help='enable job profiling?')
run.add_option('--partitions',
               action='setitem',
               dest='jobargs',
               type='reify',
               help='number of partitions to create, if any')
run.add_option('-S', '--scheduler',
               action='setitem2',
               nargs=2,
               help='add a param to the scheduler field of the jobdict')
run.add_option('-P', '--param',
               action='setitem2',
               dest='jobargs',
               default={},
               nargs=2,
               help='add a job parameter')

@Disco.command
def submit(program, *file):
    """Usage: [file]

    Submit a jobpack to the master.
    Reads the jobpack from file or stdin.
    """
    print program.disco.submit(''.join(fileinput.input(file)))

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
