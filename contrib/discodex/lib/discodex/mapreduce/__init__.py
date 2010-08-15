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
from disco.core import result_iterator, Job, Params
from disco.func import nop_reduce, map_output_stream, reduce_output_stream

class DiscodexJob(Job):
    @property
    def results(self):
        return result_iterator(self.wait(), reader=self.result_reader)

class DiscoDBOutput(object):
    def __init__(self, stream):
        from discodb import DiscoDBConstructor
        self.discodb_constructor = DiscoDBConstructor()
        self.stream = stream

    def add(self, key, val):
        self.discodb_constructor.add(key, val)

    def close(self):
        self.discodb_constructor.finalize().dump(self.stream)

def discodb_output(stream, partition, url, params):
    from discodex.mapreduce import DiscoDBOutput
    return DiscoDBOutput(stream), 'discodb:%s' % url.split(':', 1)[1]

class MetaDBOutput(object):
    def __init__(self, stream, params):
        from discodb import DiscoDBConstructor
        self.metadb_constructor = DiscoDBConstructor()
        self.stream = stream
        self.params = params

    def add(self, metakey, key):
        self.metadb_constructor.add(metakey, key)

    def close(self):
        from discodb import MetaDB
        metadb = self.metadb_constructor.finalize()
        MetaDB(self.params.datadb, metadb).dump(self.stream)

def metadb_output(stream, partition, url, params):
    from discodex.mapreduce import MetaDBOutput
    return MetaDBOutput(stream, params), 'metadb:%s' % url.split(':', 1)[1]

class Indexer(DiscodexJob):
    """A discodex mapreduce job used to build an index from a dataset."""
    save = True

    def __init__(self, master, name, dataset):
        super(Indexer, self).__init__(master, name)
        self.input          = dataset.input
        self.map_reader     = dataset.parser
        self.map            = dataset.demuxer
        self.partition      = dataset.balancer
        self.profile        = dataset.profile
        self.partitions     = dataset.nr_ichunks
        self.required_files = dataset.required_files
        self.params         = Params(n=0)

        if self.partitions:
            self.reduce = nop_reduce
            self.reduce_output_stream = [reduce_output_stream, discodb_output]
        else:
            self.map_output_stream = [map_output_stream, discodb_output]

class MetaIndexer(DiscodexJob):
    """A discodex mapreduce job used to build a metaindex over an index, given a :class:`discodex.objects.MetaSet`."""
    partitions = 0
    save       = True
    scheduler  = {'force_local': True}

    def __init__(self, master, name, metaset):
        super(MetaIndexer, self).__init__(master, name)
        self.input = metaset.ichunks
        self.map   = metaset.metakeyer

    def map_reader(datadb, size, url, params):
        params.datadb = datadb
        return datadb, size, url

    map_output_stream = [map_output_stream, metadb_output]

def json_reader(fd, size, filename, params):
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
    map_reader     = None
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
        self.ichunks = list(ichunks)
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

    def map(entry, params):
        from discodex.mapreduce.func import filterchain, funcify
        filterfn = filterchain(funcify(name) for name in params.mapfilters)
        return filterfn(entry)

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

    def __setattr__(self, attr, value):
        if attr in self.__slots__:
            return super(Record, self).__setattr__(attr, value)
        for n, name in enumerate(self.fieldnames):
            if attr == name:
                self[n] = value
                return
        raise AttributeError('%r has no attribute %r' % (self, attr))

    def __getitem__(self, index):
        return self.fields[index]

    def __setitem__(self, index, value):
        self.fields[index] = value

    def __iter__(self):
        from itertools import izip
        return izip(self.fieldnames, self.fields)

    def __repr__(self):
        return 'Record(%s)' % ', '.join('%s=%r' % (n, f) if n else '%r' % f
                                      for f, n in zip(self.fields, self.fieldnames))

    def __str__(self):
        return '\t'.join('%s:%s' % (n, f) if n else '%s' % f
                         for f, n in zip(self.fields, self.fieldnames))
