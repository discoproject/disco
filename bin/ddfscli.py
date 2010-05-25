#!/usr/bin/env python
"""
:mod:`ddfs <ddfscli>` -- DDFS command line utility
==================================================

:program:`ddfs` is a tool for manipulating data stored in :ref:`ddfs`.
Some of the :program:`ddfs` utilities also work with data stored in Disco's temporary filesystem.

.. note::

   This is the manpage for the :program:`ddfs` command.
   Please see :ref:`ddfs` for more general information on DDFS.

.. hint::

   The documentation assumes that the executable ``$DISCO_HOME/bin/ddfs`` is on your system path.
   If it is not on your path, you can add it::

        ln -s $DISCO_HOME/bin/ddfs /usr/local/bin

   If ``/usr/local/bin`` is not in your ``$PATH``, use an appropriate replacement.
   Doing so allows you to simply call :command:`ddfs`, instead of specifying the complete path.


Run :command:`ddfs help` for information on using the command line utility.

See also: :mod:`disco.settings`

"""

import os, sys
from itertools import chain

if '.disco-home' in os.listdir('.'):
    sys.path.append('lib')

from clx import OptionParser, Program

class DDFSOptionParser(OptionParser):
    def __init__(self, **kwargs):
        OptionParser.__init__(self, **kwargs)
        self.add_option('-E', '--exclude',
                        help='exclude match')
        self.add_option('-I', '--include',
                        help='include match')
        self.add_option('-i', '--ignore-missing',
                        action='store_true',
                        help='ignore missing tags')
        self.add_option('-n', '--replicas',
                        help='number of replicas to create when pushing')
        self.add_option('-p', '--prefix',
                        action='store_true',
                        help='prefix mode for commands that take it.')
        self.add_option('-r', '--recursive',
                        action='store_true',
                        help='recursively perform operations')
        self.add_option('-w', '--warn-missing',
                        action='store_true',
                        help='warn about missing tags')
        self.add_option('-x', '--tarballs',
                        action='store_true',
                        help='extract files as tarballs when pushing')
        self.add_option('-z', '--compress',
                        action='store_true',
                        help='compress tar blobs when pushing')

class DDFS(Program):
    @property
    def settings_class(self):
        from disco.settings import DiscoSettings
        return DiscoSettings

    def default(self, program, *args):
        if args:
            raise Exception("unrecognized command: %s" % ' '.join(args))
        print "DDFS located at %s" % self.settings['DISCO_MASTER']

    @property
    def ddfs(self):
        from disco.ddfs import DDFS
        return DDFS(self.settings['DISCO_MASTER'])

    def prefix_mode(self, *tags):
        from itertools import chain
        if self.options.prefix:
            return chain(match
                         for tag in tags
                         for match in self.ddfs.list(tag))
        return tags

@DDFS.command
def blobs(program, *tags):
    """Usage: [-i] [-p] tag ...

    List all blobs reachable from tag[s].
    """
    ignore_missing = program.options.ignore_missing
    for tag in program.prefix_mode(*tags):
        for replicas in program.ddfs.blobs(tag, ignore_missing=ignore_missing):
            print '\t'.join(replicas)

@DDFS.command
def cat(program, *tags):
    """Usage: [-i] [-p] tag ...

    Concatenate the contents of all blobs reachable from tag[s],
    and print to stdout.
    """
    from subprocess import call
    from disco.comm import download

    ignore_missing = program.options.ignore_missing

    def curl(replicas):
        for replica in replicas:
            try:
                return download(replica)
            except Exception, e:
                sys.stderr.write("%s\n" % e)
        raise Exception("Failed downloading all replicas: %s" % replicas)
    for tag in program.prefix_mode(*tags):
        for replicas in program.ddfs.blobs(tag, ignore_missing=ignore_missing):
            sys.stdout.write(curl(replicas))

def cp(program, source_tag, target_tag):
    """Usage: source_tag target_tag

    Copies one tag to another, overwriting it if it exists.
    """
    program.ddfs.put(target_tag, program.ddfs.get(target_tag)['urls'])

def df(program, *args):
    """Usage: <undefined>

    Display statistics about the amount of free space
    available on the filesystems of which tag is part of.
    """
    raise NotImplementedError("API does not yet support this operation")

def du(program, *args):
    """Usage: <undefined>

    Display the disk usage statistics for a tag.
    """
    raise NotImplementedError("API does not yet support this operation")

@DDFS.command
def exists(program, tag):
    """Usage: tag

    Check if a given tag exists.
    Prints 'True' or 'False' and returns the appropriate exit status.
    """
    if not program.ddfs.exists(tag):
        raise Exception("False")
    print "True"

@DDFS.command
def find(program, *tags):
    """Usage: [-i|-w] [-p] tag ...

    Walk the tag hierarchy starting at tag[s].
    Prints each path as it is encountered.

    e.g. to walk all tags with prefix 'data:' and warn about broken links:

        ddfs find -wp data:
    """
    ignore_missing = program.options.ignore_missing
    warn_missing   = program.options.warn_missing

    if warn_missing:
        ignore_missing = True

    for tag in program.prefix_mode(*tags):
        found = program.ddfs.walk(tag, ignore_missing=ignore_missing)
        for tagpath, subtags, blobs in found:
            if subtags == blobs == None:
                print "Tag not found: %s" % "\t".join(tagpath)
            elif subtags == blobs == () and warn_missing:
                print "Tag not found: %s" % "\t".join(tagpath)
            else:
                print os.path.join(*tagpath)

@DDFS.command
def get(program, tag):
    """Usage: tag

    Gets the contents of the tag.
    """
    print program.ddfs.get(tag)

def grep(program, *args):
    """Usage: <undefined>

    Print lines matching a pattern.
    """
    raise NotImplementedError("Distributed grep not yet implemented.")

@DDFS.command
def help(program, *args):
    """Usage: [command]

    Print program or command help.
    """
    command, leftover = program.search(args)
    print command

@DDFS.command
def ls(program, *prefixes):
    """Usage: [-i] [-r] prefix ...

    List all tags starting with prefix[es].

    	-r	lists the blobs reachable from each tag.
    """
    for prefix in prefixes or ('', ):
        for tag in program.ddfs.list(prefix):
            print tag
            if program.options.recursive:
                blobs(program, tag)
                print

@DDFS.command
def push(program, tag, *files):
    """Usage: [-I I] [-E E] [-n N] [-r] [-x] tag file ...

    Push file[s] to DDFS and tag them with the given tag.

        -I I	include tar blobs that contain I.

        -E E	exclude tar blobs that contain E.

    	-n N	creates N replicas instead of the default.

    	-r	recursively push directories.

    	-x	extract files as tarballs.

        -z	compress tar blobs.
    """
    replicas = program.options.replicas
    tarballs = program.options.tarballs

    blobs = [] if tarballs else [file for file in files
                                 if os.path.isfile(file)]

    for file in files:
        if tarballs:
            for name, buf, size in program.ddfs.tarblobs(file,
                                                         include=program.options.include,
                                                         exclude=program.options.exclude):
                print "extracted %s" % name
                blobs += [(buf, name)]
        elif os.path.isdir(file):
            if program.options.recursive:
                blobs += [os.path.join(path, blob)
                          for path, dirs, blobs in os.walk(file)
                          for blob in blobs]
            else:
                print "%s is a directory (not pushing)." % file
    print "pushing..."
    program.ddfs.push(tag, blobs, replicas=replicas)

@DDFS.command
def put(program, tag, *urls):
    """Usage: tag url ...

    Put the urls[s] to the given tag.
    """
    program.ddfs.put(tag, [[url] for url in urls])

@DDFS.command
def rm(program, *tags):
    """Usage: [-i] [-r] tag ...

    Remove the tag[s].
    """
    for tag in tags:
        print program.ddfs.delete(tag)

@DDFS.command
def stat(program, *tags):
    """Usage: tag ...

    Display information about the tag[s].
    """
    for tag in tags:
        tag = program.ddfs.get(tag)
        print '\t'.join('%s' % tag[key] for key in tag.keys() if key != 'urls')

@DDFS.command
def tag(program, tag, *urls):
    """Usage: tag url ...

    Tags the urls[s] with the given tag.
    """
    program.ddfs.tag(tag, [[url] for url in urls])

@DDFS.command
def touch(program, *tags):
    """Usage: tag ...

    Creates the tag[s] if they do not exist.
    """
    for tag in tags:
        program.ddfs.tag(tag, [])

@DDFS.command
def urls(program, *tags):
    """Usage: [-p] tag ...

    List the urls pointed to by the tag[s].
    """
    for tag in program.prefix_mode(*tags):
        for replicas in program.ddfs.get(tag)['urls']:
            print '\t'.join(replicas)

@DDFS.command
def xcat(program, *urls):
    """Usage: [urls ...]

    Concatenate the extracted results stored in url[s],
    and print to stdout.
    """
    from disco.core import result_iterator
    for result in result_iterator(urls):
        print result

if __name__ == '__main__':
    DDFS(option_parser=DDFSOptionParser()).main()
