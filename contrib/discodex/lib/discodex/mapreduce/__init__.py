"""
Discodex uses mapreduce jobs to build and query indices...

::

        ichunk parser == func.discodb_reader (iteritems)


        parser:  data -> records       \\
                                        | kvgenerator            \\
        demuxer: record -> k, v ...    /                          |
                                                                  | indexer
                                                                  |
        balancer: (k, ) ... -> (p, (k, )) ...   \                /
                                                 | ichunkbuilder
        ichunker: (p, (k, v)) ... -> ichunks    /
"""
from disco import func
from disco.core import result_iterator, Job, Params

class DiscodexJob(Job):
    @property
    def results(self):
        return result_iterator(self.wait(), reader=self.result_reader)

class Indexer(DiscodexJob):
    """A discodex mapreduce job used to build an index from a dataset."""
    save = True

    def __init__(self, master, name, dataset):
        super(Indexer, self).__init__(master, name)
        self.input      = dataset.input
        self.map_reader = dataset.parser
        self.map        = dataset.demuxer
        self.partition  = dataset.balancer
        self.profile    = dataset.profile
        self.partitions = dataset.nr_ichunks
        self.sort       = dataset.sort
        self.params     = Params(n=0)

        if dataset.k_viter:
            from discodex.mapreduce import demuxers
            self.sort = False
            self.map  = demuxers.iterdemux

    @staticmethod
    def reduce(iterator, out, params):
        # there should be a discodb writer of some sort
        from discodb import DiscoDB, kvgroup
        DiscoDB(kvgroup(iterator)).dump(out)

    def reduce_output_stream(stream, partition, url, params):
        return stream, 'discodb:%s' % url.split(':', 1)[1]
    reduce_output_stream = [func.reduce_output_stream, reduce_output_stream]

class MetaIndexer(DiscodexJob):
    """A discodex mapreduce job used to build a metaindex over an index, given a :class:`discodex.objects.MetaSet`."""
    partitions = 0
    save       = True
    scheduler  = {'force_local': True}

    def __init__(self, master, name, metaset):
        super(MetaIndexer, self).__init__(master, name)
        self.input = metaset.ichunks
        self.map   = metaset.metakeyer

    @staticmethod
    def map_reader(fd, size, fname):
        if hasattr(fd, '__iter__'):
            return fd
        return disco.func.map_line_reader(fd, size, fname)

    @staticmethod
    def combiner(metakey, key, buf, done, params):
        from discodb import DiscoDB, MetaDB
        if done:
            datadb = Task.discodb
            metadb = DiscoDB(buf)
            yield None, MetaDB(datadb, metadb)
        else:
            keys = buf.get(metakey, [])
            keys.append(key)
            buf[metakey] = keys

    @staticmethod
    def map_writer(fd, none, metadb, params):
        metadb.dump(fd)

    def map_output_stream(stream, partition, url, params):
        # there should be a metadb writer
        return stream, 'metadb:%s' % url.split(':', 1)[1]
    map_output_stream = [func.map_output_stream, map_output_stream]

def json_reader(fd, size, filename):
    from disco.func import netstr_reader
    from discodex import json
    for k, v in netstr_reader(fd, size, filename):
        yield json.loads(k), json.loads(v)

def json_writer(fd, key, value, params):
    from disco.func import netstr_writer
    from discodex import json
    netstr_writer(fd,
                  json.dumps(key, default=str),
                  json.dumps(value, default=str),
                  params)

class DiscoDBIterator(DiscodexJob):
    scheduler      = {'force_local': True}
    method         = 'keys'
    mapfilters     = ['kvify']
    reducefilters  = []
    resultsfilters = ['kv_or_v']
    result_reader  = staticmethod(json_reader)

    def __init__(self,
                 master,
                 name,
                 ichunks,
                 target,
                 mapfilters,
                 reducefilters,
                 resultsfilters):
        super(DiscoDBIterator, self).__init__(master, name)
        self.ichunks = ichunks
        if target:
            self.method = '%s/%s' % (target, self.method)
        self.params = Params(mapfilters=mapfilters or self.mapfilters,
                             reducefilters=reducefilters or self.reducefilters)

        if reducefilters:
            self.partitions = max(1, len(self.ichunks) / 8)
            self.reduce     = self._reduce
            self.sort       = True
            self.reduce_writer = json_writer
        else:
            self.map_writer    = json_writer

        if resultsfilters:
            self.resultsfilters = resultsfilters

    @property
    def input(self):
        return [['%s!%s/' % (url, self.method) for url in urls]
                for urls in self.ichunks]

    @staticmethod
    def map_reader(fd, size, fname):
        if hasattr(fd, '__iter__'):
            return fd
        return disco.func.map_line_reader(fd, size, fname)

    @staticmethod
    def map(entry, params):
        from discodex.mapreduce.func import filterchain, funcify
        filterfn = filterchain(funcify(name) for name in params.mapfilters)
        return filterfn(entry)

    @staticmethod
    def _reduce(iterator, out, params):
        from discodex.mapreduce.func import filterchain, funcify, kvgroup
        filterfn = filterchain(funcify(name) for name in params.reducefilters)
        for k_vs in kvgroup(iterator):
            for k, v in filterfn(k_vs):
                out.add(k, v)

    @property
    def results(self):
        from discodex.mapreduce.func import filterchain, funcify, kvgroup
        filterfn = filterchain(funcify(name) for name in self.resultsfilters)
        results  = result_iterator(self.wait(), reader=self.result_reader)
        return filterfn(results)

class KeyIterator(DiscoDBIterator):
    pass

class ValuesIterator(DiscoDBIterator):
    method = 'values'

class ItemsIterator(DiscoDBIterator):
    method     = 'items'
    mapfilters = ['kvungroup']

class Queryer(DiscoDBIterator):
    method = 'query'

    def __init__(self,
                 master,
                 name,
                 ichunks,
                 target,
                 mapfilters,
                 reducefilters,
                 resultsfilters,
                 query):
        super(Queryer, self).__init__(master,
                                      name,
                                      ichunks,
                                      target,
                                      mapfilters,
                                      reducefilters,
                                      resultsfilters)
        self.params.discodb_query = query

class Record(object):
    """Convenient containers for holding bags of [named] attributes."""
    __slots__ = ('fields', 'fieldnames')

    def __init__(self, *fields, **namedfields):
        for name in namedfields:
            if name in self.__slots__:
                raise ValueError('Use of reserved fieldname: %r' % name)
        self.fields = (list(fields) + namedfields.values())
        self.fieldnames = len(fields) * [None] + namedfields.keys()

    def __getattr__(self, attr):
        for n, name in enumerate(self.fieldnames):
            if attr == name:
                return self[n]
        raise AttributeError('%r has no attribute %r' % (self, attr))

    def __getitem__(self, index):
        return self.fields[index]

    def __iter__(self):
        from itertools import izip
        return izip(self.fieldnames, self.fields)

    def __repr__(self):
        return 'Record(%s)' % ', '.join('%s=%r' % (n, f) if n else '%r' % f
                                      for f, n in zip(self.fields, self.fieldnames))

