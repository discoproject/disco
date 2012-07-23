"""
:mod:`disco.util` -- Helper functions
=====================================

This module provides utility functions that are mostly used by Disco
internally.

.. deprecated:: 0.4
                :func:`disco.util.data_err`, :func:`disco.util.err`, and :func:`disco.util.msg`
                will be removed completely in the next release,
                in favor of using normal Python **raise** and **print** statements.
"""
import os, sys, time
import functools, gzip

from cStringIO import StringIO
from itertools import chain, groupby, repeat
from urllib import urlencode

from disco.error import DiscoError, DataError, CommError
from disco.settings import DiscoSettings

class netloc(tuple):
    @classmethod
    def parse(cls, netlocstr):
        netlocstr = netlocstr.split('@', 1)[1] if '@' in netlocstr else netlocstr
        if ':' in netlocstr:
            return cls(netlocstr.split(':'))
        return cls((netlocstr, ''))

    @property
    def host(self):
        return self[0]

    @property
    def port(self):
        return self[1]

    def __nonzero__((host, port)):
        return bool(host)

    def __str__((host, port)):
        return '%s:%s' % (host, port) if port else host

def chainify(iterable):
    return list(chain(*iterable))

def dsorted(iterable, buffer_size=1e6, tempdir='.'):
    from cPickle import dump, load
    from heapq import merge
    from itertools import islice
    from tempfile import TemporaryFile
    def read(handle):
        while True:
            try:
                yield load(handle)
            except EOFError:
                return
    iterator = iter(iterable)
    subiters = []
    while True:
        buffer = sorted(islice(iterator, buffer_size))
        handle = TemporaryFile(dir=tempdir)
        for item in buffer:
            dump(item, handle, -1)
        handle.seek(0)
        subiters.append(read(handle))
        if len(buffer) < buffer_size:
            break
    return merge(*subiters)

def flatten(iterable):
    for item in iterable:
        if isiterable(item):
            for subitem in flatten(item):
                yield subitem
        else:
            yield item

def hexhash(string):
    from hashlib import md5
    return md5(string).hexdigest()[:2]

def identity(object):
    return object

def isiterable(object):
    return hasattr(object, '__iter__')

def iskv(object):
    return isinstance(object, tuple) and len(object) is 2

def iterify(object):
    if isiterable(object):
        return object
    return repeat(object, 1)

def ilen(iter):
    return sum(1 for _ in iter)

def key((k, v)):
    return k

def kvgroup(kviter):
    """
    Group the values of consecutive keys which compare equal.

    Takes an iterator over ``k, v`` pairs,
    and returns an iterator over ``k, vs``.
    Does not sort the input first.
    """
    for k, kvs in groupby(kviter, key):
        yield k, (v for _k, v in kvs)

def kvify(entry):
    return entry if iskv(entry) else (entry, None)

def listify(object):
    return list(iterify(object))

def partition(iterable, fn):
    t, f = [], []
    for item in iterable:
        (t if fn(item) else f).append(item)
    return t, f

def reify(dotted_name, globals=globals()):
    if '.' in dotted_name:
        package, name = dotted_name.rsplit('.', 1)
        return getattr(__import__(package, fromlist=[name]), name)
    return eval(dotted_name, globals)

def shuffled(object):
    from random import shuffle
    shuffled = listify(object)
    shuffle(shuffled)
    return shuffled

def argcount(object):
    if hasattr(object, 'func_code'):
        return object.func_code.co_argcount
    argcount = object.func.func_code.co_argcount
    return argcount - len(object.args or ()) - len(object.keywords or ())

def globalize(object, globals):
    if isinstance(object, functools.partial):
        object = object.func
    if hasattr(object, 'func_globals'):
        for k, v in globals.iteritems():
            object.func_globals.setdefault(k, v)

def urljoin((scheme, netloc, path)):
    return '%s%s%s' % ('%s://' % scheme if scheme else '',
                       '%s/' % (netloc, ) if netloc else '',
                       path)

def schemesplit(url):
    return url.split('://', 1) if '://' in url else ('', url)

def localize(path, ddfs_data=None, disco_data=None):
    prefix, fname = path.split('/', 1)
    if prefix == 'ddfs':
        return os.path.join(ddfs_data, fname)
    return os.path.join(disco_data, fname)

def urlsplit(url, localhost=None, disco_port=None, **kwargs):
    scheme, rest = schemesplit(url)
    locstr, path = rest.split('/', 1)  if '/' in rest else (rest ,'')
    if scheme == 'tag':
        if not path:
            path, locstr = locstr, ''
    else:
        disco_port = disco_port or str(DiscoSettings()['DISCO_PORT'])
        host, port = netloc.parse(locstr)
        if scheme == 'disco' or port == disco_port:
            if localhost == True or locstr == localhost:
                scheme = 'file'
                locstr = ''
                path = localize(path, **kwargs)
            elif scheme == 'disco':
                scheme = 'http'
                locstr = '%s:%s' % (host, disco_port)
    return scheme, netloc.parse(locstr), path

def urlresolve(url, master=None):
    def _master((host, port)):
        if not host:
            return master or DiscoSettings()['DISCO_MASTER']
        if not port:
            return 'disco://%s' % host
        return 'http://%s:%s' % (host, port)
    scheme, netloc, path = urlsplit(url)
    if scheme == 'dir':
        return urlresolve('%s/%s' % (_master(netloc), path))
    if scheme == 'tag':
        return urlresolve('%s/ddfs/tag/%s' % (_master(netloc), path))
    return '%s://%s/%s' % (scheme, netloc, path)

def urltoken(url):
    _scheme, rest = schemesplit(url)
    locstr, _path = rest.split('/', 1)  if '/'   in rest else (rest ,'')
    if '@' in locstr:
        auth = locstr.split('@', 1)[0]
        return auth.split(':')[1] if ':' in auth else auth

def msg(message):
    """
    .. deprecated:: 0.4 use **print** instead.

    Sends the string *message* to the master for logging. The message is
    shown on the web interface. To prevent a rogue job from overwhelming the
    master, the maximum *message* size is set to 255 characters and job is
    allowed to send at most 10 messages per second.
    """
    print message

def err(message):
    """
    .. deprecated:: 0.4
                    raise :class:`disco.error.DiscoError` instead.

    Raises a :class:`disco.error.DiscoError`. This terminates the job.
    """
    raise DiscoError(message)

def data_err(message, url):
    """
    .. deprecated:: 0.4
                    raise :class:`disco.error.DataError` instead.

    Raises a :class:`disco.error.DataError`.
    A data error should only be raised if it is likely that the error is transient.
    Typically this function is used by map readers to signal a temporary failure
    in accessing an input file.
    """
    raise DataError(message, url)

def jobname(url):
    """
    Extracts the job name from *url*.

    This function is particularly useful for using the methods in
    :class:`disco.core.Disco` given only the results of a job.
    A typical case is that you no longer need the results.
    You can tell Disco to delete the unneeded data as follows::

        from disco.core import Disco
        from disco.util import jobname

        Disco().purge(jobname(results[0]))

    """
    scheme, x, path = urlsplit(url)
    if scheme in ('disco', 'dir', 'http'):
        return path.strip('/').split('/')[-2]
    raise DiscoError("Cannot parse jobname from %s" % url)

def external(files):
    from disco.worker.classic.external import package
    return package(files)

def deref(inputs, resolve=False):
    resolve = urlresolve if resolve else identity
    for input in inputlist(inputs):
        yield [resolve(i) for i in iterify(input)]

def parse_dir(dir, partition=None):
    """
    Translates a directory URL (``dir://...``) to a list of normal URLs.

    This function might be useful for other programs that need to parse
    results returned by :meth:`disco.core.Disco.wait`, for instance.

    :param dir: a directory url, such as ``dir://nx02/test_simple@12243344``
    """
    # XXX: guarantee indices are read in the same order (task/labels) (for redundancy)
    return [url for id, url in sorted(read_index(dir)) if partition in (None, id)]

def proxy_url(url, proxy=DiscoSettings()['DISCO_PROXY'], meth='GET', to_master=True):
    scheme, (host, port), path = urlsplit(url)
    if proxy and scheme != "tag":
        if to_master:
            return '%s/%s' % (proxy, path)
        return '%s/proxy/%s/%s/%s' % (proxy, host, meth, path)
    return url

def read_index(dir):
    from disco.comm import open_url
    file = open_url(proxy_url(dir, to_master=False))
    if dir.endswith(".gz"):
        file = gzip.GzipFile(fileobj=file)
    for line in file:
        yield line.split()

def ispartitioned(input):
    if isiterable(input):
        return all(ispartitioned(i) for i in input) and len(input)
    return input.startswith('dir://')

def inputexpand(input, partition=None, settings=DiscoSettings()):
    from disco.ddfs import DDFS, istag
    if ispartitioned(input) and partition is not False:
        return zip(*(parse_dir(i, partition=partition) for i in iterify(input)))
    if isiterable(input):
        return [inputlist(input, partition=partition, settings=settings)]
    if istag(input):
        ddfs = DDFS(settings=settings)
        return chainify(blobs for name, tags, blobs in ddfs.findtags(input))
    return [input]

def inputlist(inputs, **kwargs):
    return filter(None, chainify(inputexpand(input, **kwargs) for input in inputs))

def save_oob(host, name, key, value, ddfs_token=None):
    from disco.ddfs import DDFS
    DDFS(host).push(DDFS.job_oob(name), [(StringIO(value), key)], delayed=True)

def load_oob(host, name, key):
    from disco.ddfs import DDFS
    ddfs = DDFS(host)
    # NB: this assumes that blobs are listed in LIFO order.
    # We want to return the latest version
    for fd in ddfs.pull(ddfs.job_oob(name), blobfilter=lambda x: x == key):
        return fd.read()

def format_size(num):
    for unit in [' bytes','KB','MB','GB','TB']:
        if num < 1024.:
            return "%3.1f%s" % (num, unit)
        num /= 1024.
