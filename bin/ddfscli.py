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

import fileinput, os, sys
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
        self.add_option('-f', '--files',
                        action='store_true',
                        help='file mode for commands that take it')
        self.add_option('-i', '--ignore-missing',
                        action='store_true',
                        help='ignore missing tags')
        self.add_option('-n', '--replicas',
                        help='number of replicas to create when pushing')
        self.add_option('-p', '--prefix',
                        action='store_true',
                        help='prefix mode for commands that take it')
        self.add_option('-R', '--reader',
                        help='input reader to import and use')
        self.add_option('-r', '--recursive',
                        action='store_true',
                        help='recursively perform operations')
        self.add_option('-T', '--stream',
                        default='disco.func.default_stream',
                        help='input stream to import and use')
        self.add_option('-t', '--token',
                        help='authorization token for the command')
        self.add_option('-u', '--update',
                        action='store_true',
                        help='whether to perform an update or an append')
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

    def blobs(self, *tags):
        ignore_missing = self.options.ignore_missing
        for tag in self.prefix_mode(*tags):
            for replicas in self.ddfs.blobs(tag,
                                            ignore_missing=ignore_missing,
                                            token=self.options.token):
                yield replicas

    def default(self, program, *args):
        if args:
            raise Exception("unrecognized command: %s" % ' '.join(args))
        print "DDFS located at %s" % self.settings['DISCO_MASTER']

    @property
    def ddfs(self):
        from disco.ddfs import DDFS
        return DDFS(master=self.settings['DISCO_MASTER'])

    def file_mode(self, *urls):
        if self.options.files:
            return fileinput.input(urls)
        return urls

    def prefix_mode(self, *tags):
        if self.options.prefix:
            return chain(match
                         for tag in tags
                         for match in self.ddfs.list(tag))
        return tags

    def separate_tags(self, *urls):
        from disco.util import partition
        from disco.ddfs import istag
        return partition(urls, istag)

@DDFS.command
def attrs(program, tag):
    """Usage: tag

    List the attributes of a tag.
    """
    d = program.ddfs.attrs(tag, token=program.options.token)
    for k,v in d.iteritems():
        print ("%s: %s" % (k, v))

@DDFS.command
def blobs(program, *tags):
    """Usage: [-i] [-p] [tag ...]

    List all blobs reachable from tag[s].
    """
    for replicas in program.blobs(*tags):
        print '\t'.join(replicas)

@DDFS.command
def cat(program, *urls):
    """Usage: [-i] [-p] [url ...]

    Concatenate the contents of all url[s] and print to stdout.
    If any of the url[s] are tags,
    the blobs reachable from the tags will be printed after any non-tag url[s].
    """
    from subprocess import call
    from disco.comm import download

    ignore_missing = program.options.ignore_missing
    tags, urls     = program.separate_tags(*urls)

    def curl(replicas):
        for replica in replicas:
            try:
                return download(replica)
            except Exception, e:
                sys.stderr.write("%s\n" % e)
        if not ignore_missing:
            raise Exception("Failed downloading all replicas: %s" % replicas)
        return ''

    for replicas in chain(([url] for url in urls),
                          program.blobs(*tags)):
        sys.stdout.write(curl(replicas))

@DDFS.command
def chunk(program, tag, *urls):
    """Usage: [-n replicas] [-S stream] [-R reader] [-u] tag [url ...]

    Chunks the contents of the urls, pushes the chunks to ddfs and tags them.
    """
    from disco.util import reify

    tags, urls = program.separate_tags(*urls)
    stream = reify(program.options.stream)
    reader = reify(program.options.reader or 'None')
    tag, blobs = program.ddfs.chunk(tag,
                                    chain(urls, program.blobs(*tags)),
                                    input_stream=stream,
                                    reader=reader,
                                    replicas=program.options.replicas,
                                    update=program.options.update)
    for replicas in blobs:
        print 'created: %s' % '\t'.join(replicas)

@DDFS.command
def cp(program, source_tag, target_tag):
    """Usage: source_tag target_tag

    Copies one tag to another, overwriting it if it exists.
    """
    token = program.options.token
    program.ddfs.put(target_tag,
                     program.ddfs.get(source_tag, token=token)['urls'],
                     token=token)

@DDFS.command
def delattr(program, tag, attr):
    """Usage: tag attr

    Delete an attribute of a tag.
    """
    program.ddfs.delattr(tag, attr, token=program.options.token)

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
    """Usage: [-i|-w] [-p] [tag ...]

    Walk the tag hierarchy starting at tag[s].
    Prints each path as it is encountered.

    e.g. to walk all tags with prefix 'data:' and warn about broken links:

        ddfs find -wp data:
    """
    ignore_missing = program.options.ignore_missing
    warn_missing   = program.options.warn_missing
    token          = program.options.token

    if warn_missing:
        ignore_missing = True

    for tag in program.prefix_mode(*tags):
        found = program.ddfs.walk(tag, ignore_missing=ignore_missing, token=token)
        for tagpath, subtags, blobs in found:
            if subtags == blobs == None:
                print "Tag not found: %s" % "\t".join(tagpath)
            elif subtags == blobs == () and warn_missing:
                print "Tag not found: %s" % "\t".join(tagpath)
            else:
                print '\t'.join(tagpath)

@DDFS.command
def get(program, tag):
    """Usage: tag

    Gets the contents of the tag.
    """
    print program.ddfs.get(tag, token=program.options.token)

@DDFS.command
def getattr(program, tag, attr):
    """Usage: tag attr

    Get an attribute of a tag.
    """
    print program.ddfs.getattr(tag, attr, token=program.options.token)

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
    """Usage: [-i] [-r] [prefix ...]

    List all tags starting with prefix[es].

    	-r	lists the blobs reachable from each tag.
    """
    from disco.error import CommError

    for prefix in prefixes or ('', ):
        for tag in program.ddfs.list(prefix):
            print tag
            if program.options.recursive:
                try:
                    blobs(program, tag)
                except CommError, e:
                    print e
                print

@DDFS.command
def push(program, tag, *files):
    """Usage: [-I I] [-E E] [-n N] [-r] [-x] tag [file ...]

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
    token    = program.options.token

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
    program.ddfs.push(tag, blobs, replicas=replicas, token=token)

@DDFS.command
def put(program, tag, *urls):
    """Usage: tag [-f] [url ...]

    Put the urls[s] to the given tag.
    Urls may be quoted whitespace-separated lists of replicas.
    """
    program.ddfs.put(tag,
                     [url.split() for url in program.file_mode(*urls)],
                     token=program.options.token)

@DDFS.command
def readtoken(program, tag, tok):
    """Usage: tag token

    Set the read token of a tag.
    """
    program.ddfs.setattr(tag, 'ddfs:read-token', tok, token=program.options.token)

@DDFS.command
def rm(program, *tags):
    """Usage: [-i] [-r] [-p] [tag ...]

    Remove the tag[s].
    """
    for tag in program.prefix_mode(*tags):
        print program.ddfs.delete(tag, token=program.options.token)

@DDFS.command
def setattr(program, tag, attr, val):
    """Usage: tag attr val

    Set the value of an attribute of a tag.
    """
    program.ddfs.setattr(tag, attr, val, token=program.options.token)

@DDFS.command
def stat(program, *tags):
    """Usage: [tag ...]

    Display information about the tag[s].
    """
    for tag in tags:
        tag = program.ddfs.get(tag, token=program.options.token)
        print '\t'.join('%s' % tag[key] for key in tag.keys() if key != 'urls')

@DDFS.command
def tag(program, tag, *urls):
    """Usage: tag [-f] [url ...]

    Tags the urls[s] with the given tag.
    Urls may be quoted whitespace-separated lists of replicas.
    """
    program.ddfs.tag(tag,
                     [url.split() for url in program.file_mode(*urls)],
                     token=program.options.token)

@DDFS.command
def touch(program, *tags):
    """Usage: [tag ...]

    Creates the tag[s] if they do not exist.
    """
    for tag in tags:
        program.ddfs.tag(tag, [], token=program.options.token)

@DDFS.command
def urls(program, *tags):
    """Usage: [-p] [tag ...]

    List the urls pointed to by the tag[s].
    """
    for tag in program.prefix_mode(*tags):
        for replicas in program.ddfs.urls(tag, token=program.options.token):
            print '\t'.join(replicas)

@DDFS.command
def writetoken(program, tag, tok):
    """Usage: tag token

    Set the write token of a tag.
    """
    program.ddfs.setattr(tag, 'ddfs:write-token', tok, token=program.options.token)

@DDFS.command
def xcat(program, *urls):
    """Usage: [-i] [-p] [-R reader] [urls ...]

    Concatenate the extracted results stored in url[s] and print to stdout.
    If any of the url[s] are tags,
    the blobs reachable from the tags will be printed after any non-tag url[s].
    """
    from disco.core import RecordIter
    from disco.util import iterify, reify

    tags, urls = program.separate_tags(*urls)
    stream = reify(program.options.stream)
    reader = program.options.reader
    reader = reify('disco.func.chain_reader' if reader is None else reader)

    for record in RecordIter(chain(urls, program.blobs(*tags)),
                             input_stream=stream,
                             reader=reader):
        print '\t'.join('%s' % (e,) for e in iterify(record)).rstrip()

if __name__ == '__main__':
    DDFS(option_parser=DDFSOptionParser()).main()
