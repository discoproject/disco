#!/usr/bin/env python
"""
:mod:`discodex <discodexcli>` -- Discodex command line utility
==============================================================

:program:`discodex` is a command line tool for controlling and communicating with the discodex server.

.. note::

   This is the manpage for the :program:`discodex` command.
   Please see :ref:`discodex` for more information on Discodex.

.. hint::

   The documentation assumes that the executable ``$DISCO_HOME/bin/discodex`` is on your system path.
   If it is not on your path, you can add it::

        ln -s $DISCO_HOME/bin/discodex /usr/local/bin

   If ``/usr/local/bin`` is not in your ``$PATH``, use an appropriate replacement.
   Doing so allows you to simply call :program:`discodex`, instead of specifying the complete path.


Run :command:`discodex help` for information on using the command line utility.

See also: :mod:`discodex.settings`
"""

import os, sys
import fileinput
from itertools import chain

from clx import OptionParser, Program
from disco.util import iterify

class DiscodexOptionParser(OptionParser):
    def __init__(self, **kwargs):
        OptionParser.__init__(self, **kwargs)
        self.add_option('-H', '--host',
                        help='host that the client should connect to')
        self.add_option('-P', '--port',
                        help='port that the client should connect to')

class Discodex(Program):
    @classmethod
    def add_stream(cls, command):
        command.add_option('--stream',
                        action='append',
                        default=[],
                        dest='streams',
                        help='add an input stream function')
        return command

    @classmethod
    def input(cls, *inputs):
        from disco.cli import Program
        return Program.input(*inputs)

    @classmethod
    def inquiry(cls, function):
        command = cls.command(function)
        command.add_option('--param',
                           action='append',
                           default=[],
                           dest='params',
                           nargs=2,
                           help='add a param to the job params')
        command.add_option('--reduce',
                           help='reduce used for filtering inquiries')
        cls.add_stream(command)
        return command

    @property
    def client(self):
        from discodex.client import DiscodexClient
        return DiscodexClient(self.options.host, self.options.port)

    @property
    def djangoscgi(self):
        from discodex.server import djangoscgi
        return djangoscgi(self.settings)

    @property
    def lighttpd(self):
        from discodex.server import lighttpd
        return lighttpd(self.settings)

    @property
    def option_dict(self):
        return dict((k, v) for k, v in self.options.__dict__.iteritems()
                    if v is not None)

    @property
    def settings_class(self):
        from discodex.settings import DiscodexSettings
        return DiscodexSettings

    def default(self, program, *args):
        if args:
            raise Exception("unrecognized command: %s" % ' '.join(args))
        print "Discodex at http://%s:%s" % (self.settings['DISCODEX_HTTP_HOST'],
                                            self.settings['DISCODEX_HTTP_PORT'])
        print "Disco master at %s" % self.settings['DISCODEX_DISCO_MASTER']

    def server_command(self, command):
        for message in chain(getattr(self.djangoscgi, command)(),
                             getattr(self.lighttpd, command)()):
            print message

@Discodex.command
def help(program, *args):
    command, path, leftover = program.search(__builtins__.list(args))
    print command
    if not args:
        print """
        <indexspec> is the full URI to an index, or the name of an index on --host and --port.
        [query] is a query in CNF form: ','-separated literals, ' '-separated clauses a,b c == (a or b) and c
        """

@Discodex.command
def restart(program):
    program.server_command('restart')

@Discodex.command
def start(program):
    program.server_command('start')

@Discodex.command
def status(program):
    program.server_command('status')

@Discodex.command
def stop(program):
    program.server_command('stop')

@Discodex.command
def append(program, indexaspec, indexbspec):
    """Usage: <indexaspec> <indexbspec>

    Append a pointer to indexbspec to the index at indexaspec.
    """
    program.client.append(indexaspec, indexbspec)

@Discodex.command
def delete(program, indexspec):
    """Usage: <indexspec>

    Delete index at indexspec.
    """
    program.client.delete(indexspec)

@Discodex.command
def get(program, indexspec):
    """Usage: <indexspec>

    Print the index at indexspec.
    """
    print program.client.get(indexspec)

@Discodex.command
def put(program, indexspec, *urls):
    """Usage: <indexspec> url ...

    Read ichunk urls from url[s], and put them to an index at indexspec.
    """
    from discodex.objects import Index
    index = Index(urls=program.input(*urls))
    program.client.put(indexspec, index)

@Discodex.command
def list(program):
    """Usage:

    Print the names of all indices in discodex.
    """
    for index in program.client.list():
        print index

@Discodex.command
def index(program, *urls):
    """Usage: [url ...]

    Read input urls from urls[s], and index using the specified options.
    """
    from discodex.objects import DataSet
    dataset = DataSet(options=program.option_dict, input=program.input(*urls))
    print program.client.index(dataset)

index.add_option('-n', '--nr-ichunks',
                 help='the number of ichunks to create')
index.add_option('-u', '--unique-items',
                 action='store_true',
                 help='ensure unique items in discodbs')
index.add_option('-p', '--profile',
                 action='store_true',
                 help='turn on job profiling')
index.add_option('--parser',
                 help='parser function')
index.add_option('--demuxer',
                 help='demuxer function')
index.add_option('--balancer',
                 help='balancer function')
Discodex.add_stream(index)

def inquire(program, indexspec, inquiry, query=None):
    for result in program.client.inquire(indexspec, inquiry,
                                         query=query,
                                         streams=program.options.streams,
                                         reduce=program.options.reduce,
                                         params=dict(program.options.params)):
        print '\t'.join('%s' % (e,) for e in iterify(result)).rstrip()

@Discodex.inquiry
def items(program, indexspec):
    """Usage: <indexspec>

    Print the items in the specified index.
    """
    inquire(program, indexspec, 'items')

@Discodex.inquiry
def keys(program, indexspec):
    """Usage: <indexspec>

    Print the keys of the specified index.
    """
    inquire(program, indexspec, 'keys')

@Discodex.inquiry
def values(program, indexspec):
    """Usage: <indexspec>

    Print the values of the specified index.
    """
    inquire(program, indexspec, 'values')

@Discodex.inquiry
def query(program, indexspec, *args):
    """Usage: <indexspec> [query]

    Query the specified index using the given clauses.
    """
    from discodb.query import Q, Clause
    query = Q.parse('&'.join(arg.replace(',', '|') for arg in args))
    inquire(program, indexspec, 'query', query)

@Discodex.inquiry
def metaquery(program, indexspec, *args):
    """Usage: <indexspec> [query]

    Metaquery the specified index using the given clauses.
    """
    from discodb.query import Q, Clause
    query = Q.parse('&'.join(arg.replace(',', '|') for arg in args))
    inquire(program, indexspec, 'metaquery', query)

if __name__ == '__main__':
    Discodex(option_parser=DiscodexOptionParser()).main()
