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

        ln -s $DISCO_HOME/bin/ddfs /usr/bin

   If ``/usr/bin`` is not in your ``$PATH``, use an appropriate replacement.
   Doing so allows you to simply call :command:`ddfs`, instead of specifying the complete path.

Run :command:`ddfs help` for information on using the command line utility.

.. seealso::

        The :mod:`disco <discocli>` command.

        See :mod:`disco.settings` for information about Disco settings.
"""

import fileinput, os, sys

if '.disco-home' in os.listdir('.'):
    sys.path.append('lib')

from disco.cli import OptionParser, Program

class DDFS(Program):
    pass

@DDFS.command
def attrs(program, tag):
    """Usage: tag

    Get the attributes of a tag.
    """
    for k, v in program.ddfs.attrs(tag).items():
        print('{0}\t{1}'.format(k, v))

@DDFS.add_program_blobs
@DDFS.command
def blobs(program, *tags):
    """Usage: [tag ...]

    List all blobs reachable from tag[s].
    """
    for replicas in program.blobs(*tags):
        print('\t'.join(replicas))

@DDFS.add_job_mode
@DDFS.add_program_blobs
@DDFS.command
def cat(program, *urls):
    """Usage: [url ...]

    Concatenate the contents of all url[s] and print to stdout.
    If any of the url[s] are tags,
    the blobs reachable from the tags will be printed after any non-tag url[s].
    """
    from itertools import chain
    from subprocess import call
    from disco.comm import download
    from disco.util import deref, urlresolve, proxy_url
    from disco.compat import bytes_to_str

    ignore_missing = program.options.ignore_missing
    tags, urls     = program.separate_tags(*urls)

    def curl(replicas):
        for replica in replicas:
            try:
                return download(proxy_url(urlresolve(replica, master=program.ddfs.master),
                                          to_master=False), sleep=9)
            except Exception as e:
                sys.stderr.write("{0}\n".format(e))
        if not ignore_missing:
            raise Exception("Failed downloading all replicas: {0}".format(replicas))
        return ''

    for replicas in deref(chain(urls, program.blobs(*tags))):
        sys.stdout.write(bytes_to_str(curl(replicas)))

@DDFS.command
def chtok(program, tag, token):
    """Usage: tag token

    Change the read/write tokens for a tag.
    """
    if program.options.read:
        program.ddfs.setattr(tag, 'ddfs:read-token', token)
    if program.options.write:
        program.ddfs.setattr(tag, 'ddfs:write-token', token)

chtok.add_option('-r', '--read',
                 action='store_true',
                 help='change the read token')
chtok.add_option('-w', '--write',
                 action='store_true',
                 help='change the write token')

@DDFS.add_classic_reads
@DDFS.add_program_blobs
@DDFS.command
def chunk(program, tag, *urls):
    """Usage: tag [url ...]

    Chunks the contents of the urls, pushes the chunks to ddfs and tags them.
    For chunking a file in the current directory, a './' must be prepended to the
    file name.  Otherwise, ddfs chunk assumes it is a tag name.
    The character '-' can be used to specify that input can be read from stdin
    for example:
        cat chekhov.txt | ddfs chunk chekhov -
    is the same as:
        ddfs chunk chekhov ./chekhov.txt

    and both of them chunk the chekhov.txt file from the local directory into
    ddfs.
    """
    from itertools import chain
    from disco.util import reify

    tags, urls = program.separate_tags(*program.input(*urls))
    stream = reify(program.options.stream)

    def getSizeIfSupplied(value, default):
        if value is not None:
            from disco.fileutils import MB
            return int(float(value) * MB)
        else:
            return default

    from disco.fileutils import CHUNK_SIZE, MAX_RECORD_SIZE
    chunk_size = getSizeIfSupplied(program.options.size, CHUNK_SIZE)
    max_record_size = getSizeIfSupplied(program.options.max_record_size, MAX_RECORD_SIZE)

    reader = reify(program.options.reader or 'None')
    tag, blobs = program.ddfs.chunk(tag,
                                    chain(urls, program.blobs(*tags)),
                                    input_stream=stream,
                                    reader=reader,
                                    replicas=program.options.replicas,
                                    forceon=[] if not program.options.forceon else
                                        [program.options.forceon],
                                    chunk_size=chunk_size,
                                    max_record_size=max_record_size,
                                    update=program.options.update)
    for replicas in blobs:
        print('created: {0}'.format('\t'.join(replicas)))

chunk.add_option('-n', '--replicas',
                 help='number of replicas to create')
chunk.add_option('-F', '--forceon',
                 help='The node we want a replica on.')
chunk.add_option('-u', '--update',
                 action='store_true',
                 help='whether to perform an update or an append')
chunk.add_option('-S', '--size',
                 help='The size of the desired chunks (in megabytes)')
chunk.add_option('-Z', '--max-record-size',
                 help='The maximum permitted record size (in megabytes)')

@DDFS.command
def cp(program, source_tag, target_tag):
    """Usage: source_tag target_tag

    Copies one tag to another, overwriting it if it exists.
    """
    program.ddfs.put(target_tag, program.ddfs.get(source_tag)['urls'])

@DDFS.command
def delattr(program, tag, attr):
    """Usage: tag attr

    Delete an attribute of a tag.
    """
    program.ddfs.delattr(tag, attr)

def df(program, *args):
    """Usage: <undefined>

    Display statistics about the amount of free space
    available on the filesystems of which tag is part of.
    """
    raise NotImplementedError("API does not yet support this operation")

@DDFS.add_prefix_mode
@DDFS.command
def du(program, *tags):
    """Usage: [tag ...]

    Display the disk usage statistics for a tag.
    This will run a job on the cluster, getting the file on disk
    for each of the blobs in the supplied tags.

    eg. to get the size in bytes for the tag `chekhov`
        ddfs du chekov

    eg. to get the size in human readable form
        ddfs du chekov -H

    eg. to increase the amount of cores the job uses
        ddfs du chekov -n 200

    eg. to increase the number of partitions (if there are a lot of files)
        ddfs du chekov -P 100
    """
    from disco.ddfs import FileSizeJob
    from disco.core import result_iterator
    from disco.fileutils import human_readable_size
    for tag in program.prefix_mode(*tags):
        job_name = '_ddfs_du_{0}'.format(tag.replace(':', '_'))
        scheduler = {
            'force_local': True,
            'max_cores': 50
        }
        partitions = 10
        if program.options.cores:
            scheduler['max_cores'] = int(program.options.cores)
        if program.options.partitions:
            partitions = int(program.options.partitions)
        try:
            job = FileSizeJob(
                name=job_name, master=program.options.master).run(
                input=[b for b in program.ddfs.blobs(tag)],
                scheduler=scheduler,
                partitions=partitions)
            f_total = 0
            for _, f_size in result_iterator(job.wait()):
                f_total += f_size
            if program.options.humanreadable:
                f_total = human_readable_size(f_total)
            print("{0}: {1}".format(tag, f_total))
            job.purge()
        except Exception as e:
            import traceback
            print(traceback.format_exc(15))
            print ("Error: {0}".format(e))

du.add_option('-H', '--humanreadable',
              action='store_true',
              help='make the output human readable')
du.add_option('-n', '--cores',
              help='number of cores to operate on')
du.add_option('-P', '--partitions',
              help='number of partitions for the job')

@DDFS.command
def exists(program, tag):
    """Usage: tag

    Check if a given tag exists.
    Prints 'True' or 'False' and returns the appropriate exit status.
    """
    if not program.ddfs.exists(tag):
        raise Exception("False")
    print("True")

@DDFS.add_ignore_missing
@DDFS.add_prefix_mode
@DDFS.command
def find(program, *tags):
    """Usage: [tag ...]

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
                print("Tag not found: {0}".format("\t".join(tagpath)))
            elif subtags == blobs == () and warn_missing:
                print("Tag not found: {0}".format("\t".join(tagpath)))
            else:
                print('\t'.join(tagpath))

find.add_option('-w', '--warn-missing',
                action='store_true',
                help='warn about missing tags')

@DDFS.command
def get(program, tag):
    """Usage: tag

    Gets the contents of the tag.
    """
    print(program.ddfs.get(tag))

@DDFS.command
def getattr(program, tag, attr):
    """Usage: tag attr

    Get an attribute of a tag.
    """
    print(program.ddfs.getattr(tag, attr))

def grep(program, *args):
    """Usage: <undefined>

    Print lines matching a pattern.
    """
    raise NotImplementedError("Distributed grep not yet implemented.")

@DDFS.add_program_blobs
@DDFS.command
def ls(program, *prefixes):
    """Usage: [prefix ...]

    List all tags starting with prefix[es].
    """
    from disco.error import CommError

    for prefix in prefixes or ('', ):
        for tag in program.ddfs.list(prefix):
            print(tag)
            if program.options.recursive:
                try:
                    blobs(program, tag)
                except CommError as e:
                    print(e)
                print()

ls.add_option('-r', '--recursive',
              action='store_true',
              help='lists the blobs reachable from each tag')

@DDFS.command
def push(program, tag, *files):
    """Usage: tag [file ...]

    Push file[s] to DDFS and tag them with the given tag.
    """
    replicas = program.options.replicas
    tarballs = program.options.tarballs
    forceon= [] if not program.options.forceon else [program.options.forceon]

    blobs = []
    for file in files:
        if tarballs:
            for name, buf, size in program.ddfs.tarblobs(file,
                                                         compress=program.options.compress,
                                                         include=program.options.include,
                                                         exclude=program.options.exclude):
                print("extracted {0}".format(name))
                blobs += [(buf, name)]
        elif os.path.isfile(file):
            blobs += [file]
        elif os.path.isdir(file):
            if program.options.recursive:
                blobs += [os.path.join(path, blob)
                          for path, dirs, blobs in os.walk(file)
                          for blob in blobs]
            else:
                print("{0} is a directory (not pushing).".format(file))
        else:
            raise Exception("{0}: No such file or directory".format(file))

    print("pushing...")
    program.ddfs.push(tag, blobs, replicas=replicas, forceon=forceon)

push.add_option('-E', '--exclude',
                help='exclude tar blobs that contain string')
push.add_option('-I', '--include',
                help='include tar blobs that contain string')
push.add_option('-n', '--replicas',
                help='number of replicas to create')
push.add_option('-F', '--forceon',
                 help='The node we want a replica on.')
push.add_option('-r', '--recursive',
                action='store_true',
                help='recursively push directories')
push.add_option('-x', '--tarballs',
                action='store_true',
                help='extract files as tarballs')
push.add_option('-z', '--compress',
                action='store_true',
                help='compress tar blobs when pushing')

@DDFS.command
def put(program, tag, *urls):
    """Usage: tag [url ...]

    Put the urls[s] to the given tag.
    Urls may be quoted whitespace-separated lists of replicas.
    """
    from disco.util import listify
    program.ddfs.put(tag, [listify(i) for i in program.input(*urls)])

@DDFS.add_prefix_mode
@DDFS.command
def rm(program, *tags):
    """Usage: [tag ...]

    Remove the tag[s].
    """
    for tag in program.prefix_mode(*tags):
        print(program.ddfs.delete(tag))

@DDFS.command
def setattr(program, tag, attr, val):
    """Usage: tag attr val

    Set the value of an attribute of a tag.
    """
    program.ddfs.setattr(tag, attr, val)

@DDFS.add_prefix_mode
@DDFS.command
def stat(program, *tags):
    """Usage: [tag ...]

    Display information about the tag[s].
    """
    for tag in program.prefix_mode(*tags):
        tag = program.ddfs.get(tag)
        print('\t'.join('{0}'.format(tag[key]) for key in tag.keys() if key != 'urls'))

@DDFS.command
def tag(program, tag, *urls):
    """Usage: tag [url ...]

    Tags the urls[s] with the given tag.
    Urls may be quoted whitespace-separated lists of replicas.
    """
    from disco.util import listify
    program.ddfs.tag(tag, [listify(i) for i in program.input(*urls)])

@DDFS.command
def touch(program, *tags):
    """Usage: [tag ...]

    Creates the tag[s] if they do not exist.
    """
    for tag in tags:
        program.ddfs.tag(tag, [])

@DDFS.add_prefix_mode
@DDFS.command
def urls(program, *tags):
    """Usage: [tag ...]

    List the urls pointed to by the tag[s].
    """
    for tag in program.prefix_mode(*tags):
        for replicas in program.ddfs.urls(tag):
            print('\t'.join(replicas))

@DDFS.add_job_mode
@DDFS.add_classic_reads
@DDFS.add_program_blobs
@DDFS.command
def xcat(program, *urls):
    """Usage: [urls ...]

    Concatenate the extracted results stored in url[s] and print to stdout.
    If any of the url[s] are tags,
    the blobs reachable from the tags will be printed after any non-tag url[s].
    """
    from itertools import chain
    from disco.core import result_iterator
    from disco.util import iterify, reify

    tags, urls = program.separate_tags(*program.input(*urls))
    stream = reify(program.options.stream)
    reader = program.options.reader
    reader = reify('disco.worker.task_io.chain_reader' if reader is None else reader)
    for record in result_iterator(chain(urls, program.blobs(*tags)),
                                   input_stream=stream,
                                   reader=reader):
        print('\t'.join('{0}'.format(e) for e in iterify(record)).rstrip())

if __name__ == '__main__':
    DDFS(option_parser=OptionParser()).main()
