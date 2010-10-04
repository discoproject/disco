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
from disco.func import nop_reduce, map_input_stream, map_output_stream, reduce_output_stream

class DiscoDBOutput(object):
    def __init__(self, stream, params):
        from discodb import DiscoDBConstructor
        self.discodb_constructor = DiscoDBConstructor()
        self.stream = stream
        self.params = params

    def add(self, key, val):
        self.discodb_constructor.add(key, val)

    def close(self):
        kwargs = dict(unique_items=self.params.unique_items)
        self.discodb_constructor.finalize(**kwargs).dump(self.stream)

def discodb_output(stream, partition, url, params):
    from discodex.mapreduce import DiscoDBOutput
    return DiscoDBOutput(stream, params), 'discodb:%s' % url.split(':', 1)[1]

class MetaDBOutput(DiscoDBOutput):
    def close(self):
        from discodb import MetaDB
        kwargs = dict(unique_items=self.params.unique_items)
        metadb = self.discodb_constructor.finalize(**kwargs)
        MetaDB(self.params.datadb, metadb).dump(self.stream)

def metadb_output(stream, partition, url, params):
    from discodex.mapreduce import MetaDBOutput
    return MetaDBOutput(stream, params), 'metadb:%s' % url.split(':', 1)[1]

class Indexer(Job):
    """A discodex mapreduce job used to build an index from a dataset."""
    save = True

    def __init__(self, master, name, dataset):
        super(Indexer, self).__init__(master, name)
        self.input            = dataset.input
        self.map_input_stream = dataset.stream
        self.map_reader       = dataset.parser
        self.map              = dataset.demuxer
        self.partition        = dataset.balancer
        self.profile          = dataset.profile
        self.partitions       = dataset.nr_ichunks
        self.required_files   = dataset.required_files
        self.params           = Params(n=0, unique_items=dataset.unique_items)

        if self.partitions:
            self.reduce = nop_reduce
            self.reduce_output_stream = [reduce_output_stream, discodb_output]
        else:
            self.map_output_stream = [map_output_stream, discodb_output]

class MetaIndexer(Job):
    """A discodex mapreduce job used to build a metaindex over an index, given a :class:`discodex.objects.MetaSet`."""
    partitions = 0
    save       = True
    scheduler  = {'force_local': True}

    def __init__(self, master, name, metaset):
        super(MetaIndexer, self).__init__(master, name)
        self.input  = metaset.ichunks
        self.map    = metaset.metakeyer
        self.params = Params(n=0, unique_items=metaset.unique_items)

    def map_reader(datadb, size, url, params):
        params.datadb = datadb
        return datadb, size, url

    map_output_stream = [map_output_stream, metadb_output]

class DiscoDBIterator(Job):
    scheduler      = {'force_local': True}
    map_reader     = None

    def __init__(self, master, name, index, method, arg, streams, reduce, **kwargs):
        super(DiscoDBIterator, self).__init__(master, name)
        self.input = [['%s!%s/%s' % (url, method, arg) for url in urls]
                      for urls in index.ichunks]
        self.map_input_stream = [map_input_stream] + streams
        self.params = Params(**kwargs)

        if reduce:
            self.partitions = len(self.master.nodeinfo())
            self.reduce = reduce

    def map(entry, params):
        from disco.util import kvify
        from discodb import DiscoDBIter
        k, v = kvify(entry)
        yield (k, str(v)) if isinstance(v, DiscoDBIter) else (k, v)

    @property
    def results(self):
        for k, v in result_iterator(self.wait()):
            yield k if v is None else (k, v)

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
