"""
:mod:`disco.ddfs` --- Client interface for Disco Distributed Filesystem
=======================================================================

See also: :ref:`DDFS`.

.. note::

        Parameters below which are indicated as tags can be specified as
        a `tag://` URL, or the name of the tag.

.. autoclass:: DDFS(master=None, proxy=None, settings=DiscoSettings())
        :members:
"""
import os, re, random
from cStringIO import StringIO
from urllib import urlencode

from disco.comm import upload, download, json, open_remote
from disco.error import CommError
from disco.settings import DiscoSettings
from disco.util import isiterable, iterify, partition
from disco.util import urljoin, urlsplit, urlresolve

unsafe_re = re.compile(r'[^A-Za-z0-9_\-@:]')

class InvalidTag(Exception):
    pass

def canonizetag(tag):
    if tag:
        if isiterable(tag):
            for tag in tag:
                return canonizetag(tag)
        elif tag.startswith('tag://'):
            return tag
        elif '://' not in tag and '/' not in tag:
            return 'tag://%s' % tag
    raise InvalidTag("Invalid tag: %s" % tag)

def canonizetags(tags):
    return [canonizetag(tag) for tag in iterify(tags)]

def istag(tag):
    try:
        return canonizetag(tag)
    except InvalidTag:
        pass

def tagname(tag):
    scheme, netloc, name = urlsplit(canonizetag(tag))
    return name

def relativizetag(tag, parent):
    _scheme, netloc, name = urlsplit(canonizetag(tag))
    _scheme, parentloc, _ = urlsplit(canonizetag(parent))
    return urljoin(('tag', netloc or parentloc, name))

def relativizetags(tags, parent):
    return [relativizetag(tag, parent) for tag in iterify(tags)]

class DDFS(object):
    """
    Opens and encapsulates a connection to a DDFS master.

    :param master: address of the master,
                   for instance ``disco://localhost``.
    """
    def __init__(self, master=None,
                 proxy=None,
                 settings=DiscoSettings()):
        self.proxy  = proxy or settings['DISCO_PROXY']
        self.master = self.proxy or master or settings['DISCO_MASTER']
        self.settings = settings
        self.settings['DISCO_MASTER'] = self.master

    @classmethod
    def safe_name(cls, name):
        return unsafe_re.sub('_', name)

    @classmethod
    def blob_name(cls, url):
        return url.split('/')[-1].split('$')[0]

    def attrs(self, tag, token=None):
        """Get a list of the attributes of the tag ``tag`` and their values."""
        return self._download(canonizetag(tag), token=token).get('user-data')

    def blobs(self, tag, ignore_missing=True, token=None):
        """
        Walks the tag graph starting at `tag`.

        Yields only the terminal nodes of the graph (`blobs`).

        :type  ignore_missing: bool
        :param ignore_missing: Whether or not missing tags will raise a
                               :class:`disco.error.CommError`.
        """
        for path, tags, blobs in self.walk(tag,
                                           ignore_missing=ignore_missing,
                                           token=token):
            if tags != blobs:
                for replicas in blobs:
                    yield replicas

    def delattr(self, tag, attr, token=None):
        """Delete the attribute ``attr` of the tag ``tag``."""
        return self._download(self._tagattr(tag, attr),
                              method='DELETE',
                              token=token)

    def chunk(self, tag, urls,
              replicas=None,
              retries=10,
              delayed=False,
              update=False,
              token=None,
              **kwargs):
        """
        Chunks the contents of `urls`,
        pushes the chunks to ddfs and tags them with `tag`.
        """
        from disco.core import ChunkIter

        def chunk_name(replicas, n):
            url = list(iterify(replicas))[0]
            return self.safe_name('%s-%s' % (os.path.basename(url), n))

        blobs = [self._push((StringIO(chunk), chunk_name(reps, n)),
                            replicas=replicas,
                            retries=retries)
                 for reps in urls
                 for n, chunk in enumerate(ChunkIter(reps, **kwargs))]
        return (self.tag(tag,
                         blobs,
                         delayed=delayed,
                         update=update,
                         token=token),
                blobs)

    def delete(self, tag, token=None):
        """Delete ``tag``."""
        return self._download(canonizetag(tag), method='DELETE', token=token)

    def exists(self, tag):
        """Returns whether or not ``tag`` exists."""
        try:
            if open_remote(self._resolve(canonizetag(tag))):
                return True
        except CommError, e:
            if e.code == 401:
                return True
            if e.code not in (403, 404):
                raise
        return False

    def findtags(self, tags=(), ignore_missing=True, token=None):
        import sys
        """
        Finds the nodes of the tag graph starting at `tags`.

        Yields a 3-tuple `(tag, tags, blobs)`.
        """
        seen = set()

        tag_queue = canonizetags(tags)

        for tag in tag_queue:
            if tag not in seen:
                try:
                    urls        = self.get(tag, token=token).get('urls', [])
                    tags, blobs = partition(urls, istag)
                    tags        = canonizetags(tags)
                    yield tag, tags, blobs

                    tag_queue += relativizetags(tags, tag)
                    seen.add(tag)
                except CommError, e:
                    if ignore_missing and e.code == 404:
                        tags = blobs = ()
                    else:
                        raise

    def get(self, tag, token=None):
        """Return the tag object stored at ``tag``."""
        return self._download(canonizetag(tag), token=token)

    def getattr(self, tag, attr, token=None):
        """Return the value of the attribute ``attr` of the tag ``tag``."""
        return self._download(self._tagattr(tag, attr), token=token)

    def urls(self, tag, token=None):
        """Return the urls in the ``tag``."""
        return self.get(tag, token=token)['urls']

    def list(self, prefix=''):
        """Return a list of all tags starting wtih ``prefix``."""
        return self._download('%s/ddfs/tags/%s' % (self.master, prefix))

    def pull(self, tag, blobfilter=lambda x: True, token=None):
        for repl in self.urls(tag, token=token):
            if blobfilter(self.blob_name(repl[0])):
                random.shuffle(repl)
                for url in repl:
                    try:
                        yield open_remote(url)
                        break
                    except CommError, error:
                        continue
                else:
                    raise error

    def push(self,
             tag,
             files,
             replicas=None,
             retries=10,
             delayed=False,
             update=False,
             token=None):
        """
        Pushes a bunch of files to ddfs and tags them with `tag`.

        :type  files: a list of ``paths``, ``(path, name)``-tuples, or
                      ``(fileobject, name)``-tuples.
        :param files: the files to push as blobs to DDFS.
                      If names are provided,
                      they will be used as prefixes by DDFS for the blobnames.
                      Names may only contain chars in ``r'[^A-Za-z0-9_\-@:]'``.
        """
        def aim(tuple_or_path):
            if isinstance(tuple_or_path, basestring):
                source = tuple_or_path
                target = self.safe_name(os.path.basename(source))
            else:
                source, target = tuple_or_path
            return source, target

        urls = [self._push(aim(f), replicas=replicas, retries=retries)
                for f in files]
        return self.tag(tag, urls, delayed=delayed, update=update, token=token), urls

    def put(self, tag, urls, token=None):
        """Put the list of ``urls`` to the tag ``tag``.

        .. warning::

                Generally speaking, concurrent applications should use
                :meth:`DDFS.tag` instead.
        """
        return self._upload(canonizetag(tag),
                            StringIO(json.dumps(urls)),
                            token=token)

    def setattr(self, tag, attr, val, token=None):
        """Set the value of the attribute ``attr` of the tag ``tag``."""
        return self._upload(self._tagattr(tag, attr),
                            StringIO(json.dumps(val)),
                            token=token)

    def tag(self, tag, urls, token=None,  **kwargs):
        """Append the list of ``urls`` to the ``tag``."""
        defaults = {'delayed': False, 'update': False}
        defaults.update(kwargs)
        url = '%s?%s' % (canonizetag(tag),
                         '&'.join('%s=%s' % (k, '1' if v else '')
                                  for k, v in defaults.items()))
        return self._download(url, json.dumps(urls), token=token)

    def tarblobs(self, tarball, compress=True, include=None, exclude=None):
        import tarfile, sys, gzip, os

        tar = tarfile.open(tarball)

        for member in tar:
            if member.isfile():
                if include and include not in member.name:
                    continue
                if exclude and exclude in member.name:
                    continue
                if compress:
                    buf    = StringIO()
                    gz     = gzip.GzipFile(mode='w', compresslevel=2, fileobj=buf)
                    size   = self._copy(tar.extractfile(member), gz)
                    gz.close()
                    buf.seek(0)
                    suffix = '_gz'
                else:
                    buf    = tar.extractfile(member)
                    size   = len(buf)
                    suffix = ''
                name = DDFS.safe_name(member.name) + suffix
                yield name, buf, size

    def walk(self, tag, ignore_missing=True, tagpath=(), token=None):
        """
        Walks the tag graph starting at `tag`.

        Yields a 3-tuple `(tagpath, tags, blobs)`.

        :type  ignore_missing: bool
        :param ignore_missing: Whether or not missing tags will raise a
                               :class:`disco.error.CommError`.
        """
        tagpath += (canonizetag(tag), )

        try:
            urls        = self.get(tag, token=token).get('urls', [])
            tags, blobs = partition(urls, istag)
            tags        = canonizetags(tags)
            yield tagpath, tags, blobs
        except CommError, e:
            if ignore_missing and e.code == 404:
                tags = blobs = ()
                yield tagpath, tags, blobs
            else:
                yield tagpath, None, None
                raise e

        for next_tag in relativizetags(tags, tag):
            for child in self.walk(next_tag,
                                   ignore_missing=ignore_missing,
                                   tagpath=tagpath,
                                   token=token):
                yield child

    def _copy(self, src, dst):
        s = 0
        while True:
            b = src.read(8192)
            if not b:
                break
            s += len(b)
            dst.write(b)
        return s

    def _maybe_proxy(self, url, method='GET'):
        if self.proxy:
            scheme, (host, port), path = urlsplit(url)
            return '%s/proxy/%s/%s/%s' % (self.proxy, host, method, path)
        return url

    def _push(self, (source, target), replicas=None, exclude=[], **kwargs):
        qs = urlencode([(k, v) for k, v in (('exclude', ','.join(exclude)),
                                            ('replicas', replicas)) if v])
        urls = self._download('%s/ddfs/new_blob/%s?%s' % (self.master,
                                                          target,
                                                          qs))

        try:
            return [json.loads(url)
                    for url in self._upload(urls, source, **kwargs)]
        except CommError, e:
            scheme, (host, port), path = urlsplit(e.url)
            return self._push((source, target),
                              replicas=replicas,
                              exclude=exclude + [host],
                              **kwargs)

    def _tagattr(self, tag, attr):
        return '%s/%s' % (self._resolve(canonizetag(tag)), attr)

    def _token(self, token, method):
        if token is None:
            if method == 'GET':
                return self.settings['DDFS_READ_TOKEN']
            return self.settings['DDFS_WRITE_TOKEN']
        return token

    def _resolve(self, url):
        return urlresolve(url, settings=self.settings)

    def _download(self, url, data=None, token=None, method='GET'):
        return json.loads(download(self._resolve(url),
                                   data=data,
                                   method=method,
                                   token=self._token(token, method)))

    def _upload(self, urls, source, token=None, **kwargs):
        urls = [self._resolve(self._maybe_proxy(url, method='PUT'))
                for url in iterify(urls)]
        return upload(urls, source, token=self._token(token, 'PUT'), **kwargs)
