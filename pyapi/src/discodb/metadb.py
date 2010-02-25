"""
A MetaDB is a zipfile containing two DiscoDBs where one is an index over the keys of the other.

Lookups into the MetaDB are first performed in the metadb and then keys are dereferenced in the datadb.

metadb:

======= ======
ks      vs
======= ======
metakey keys
======= ======

datadb:

=== ======
ks  vs
=== ======
key values
=== ======

>>> datadb = DiscoDB({'a': 'bcd', 'e': 'bfg'})
>>> metadb = MetaDB(datadb, {'metakey': 'ae'})
>>> for metakey, metaval in metadb.items():
...     dict((k, list(v)) for k, v in metaval)
"""

from discodb import DiscoDB
from zipfile import ZipFile

class MetaDB(object):
    def __init__(self, datadb, metadb):
        self.metadb = metadb
        self.datadb = datadb

    def __getitem__(self, metakey):
        """an iterator over the keys of metakey and its dereferenced values."""
        for key in self.metadb[metakey]:
            yield key, self.datadb[key]

    def __contains__(self, key):
        return key in self.metadb

    def __iter__(self):
        """an iterator over the [meta]keys."""
        return self.keys()

    def __len__(self):
        return len(self.metadb)

    def items(self):
        """an iterator over the metakeys and their corresponding values."""
        for metakey in self:
            yield metakey, self[metakey]

    def keys(self):
        """an iterator over the metakeys."""
        return self.metadb.keys()

    def values(self):
        """an iterator over the keys of all metakeys and their dereferenced values."""
        for key in self.metadb.values():
            yield key, self.datadb[key]

    def query(self, q):
        """an iterator over the keys whose metakeys satisfy q and their dereferenced values."""
        for key in self.metadb.query(q):
            yield key, self.datadb[key]

    def dump(self, file):
        """write serialization of self to file."""
        zipfile = ZipFile(file, 'w')
        zipfile.writestr('metadb', self.metadb.dumps())
        zipfile.writestr('datadb', self.datadb.dumps())
        zipfile.close()

    @classmethod
    def load(cls, file):
        """a deserialized instance of %s from file.""" % cls
        if isinstance(file, basestring):
            zipfile = ZipFile(open(file, 'r', 0))
        else:
            zipfile = ZipFile(file)
        metazip = zipfile.open('metadb').fileobj
        metadb  = DiscoDB.load(metazip, metazip.tell())
        datazip = zipfile.open('datadb').fileobj
        datadb  = DiscoDB.load(datazip, datazip.tell())
        return cls(datadb, metadb)
