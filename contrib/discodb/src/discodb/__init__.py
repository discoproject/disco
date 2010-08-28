from ._discodb import DiscoDB, DiscoDBConstructor, DiscoDBError
from .discodict import DiscoDict
from .metadb import MetaDB
from .query import Q
from .tools import kvgroup

__all__ = ['DiscoDB',
           'DiscoDBConstructor',
           'DiscoDBError',
           'DiscoDict',
           'Q',
           'kvgroup',
           'MetaDB']

from copy_reg import pickle

def unpickle(string):
    return DiscoDB.loads(string)

def __reduce__(discodb):
    return unpickle, (discodb.dumps(), )

pickle(DiscoDB, __reduce__)
