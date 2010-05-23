:mod:`discodb` -- An efficient, immutable, persistent mapping object
====================================================================

`discodb` is comprised of a low-level data structure implemented in C,
and a high-level :class:`discodb.DiscoDB` class which exposes a dict-like
interface for using the low-level data structure from Python. In contrast
to Python's default dictionary, discodb can handle tens of millions
of key-value pairs without consuming gigabytes of memory.

In addition to basic key-value mappings, discodb supports evaluation of
Boolean queries expressed in `Conjunctive Normal Form
<http://en.wikipedia.org/wiki/Conjunctive_normal_form>`_ (see :class:`discodb.Q`
below). All queries are evaluated lazily using iterators, so you can handle
gigabytes of data in Python with ease.

discodbs are :term:`persistent`, which means that once created in memory,
they can be easily compressed, serialized and written to a file.
The benefit of this is that after they have been persisted,
instantiating them from disk and key lookups are lightning-fast operations,
thanks to `memory mapping <http://en.wikipedia.org/wiki/Memory-mapped_file>`_.

discodbs are also :term:`immutable`, which means that once they are created,
they cannot be modified. A benefit of immutability is that the full
key-space is known when discodb is built, which makes it possible to use
`perfect hashing <http://en.wikipedia.org/wiki/Perfect_hash_function>`_ for
fast *O(1)* key lookups. Specifically, discodb relies on the `CMPH library
<http://cmph.sourceforge.net/>`_ for building minimal perfect hash functions.

The format of a discodb file essentially looks like this:

.. image:: images/discodb_format.png

The benefits of these properties are realized when you need repeated,
random-access to data, especially when the dataset is too large to fit
in memory at once.
discodbs are a key component in Disco's builtin distributed indexing system,
:ref:`discodex`

Example
-------

Here is a simple example that builds a simple discodb and queries it::

    from discodb import DiscoDB, Q

    data = {'mammals': ['cow', 'dog', 'cat', 'whale'],
            'pets': ['dog', 'cat', 'goldfish'],
            'aquatic': ['goldfish', 'whale']}

    db = DiscoDB(data) # create an immutable discodb object

    print list(db.keys()) # => mammals, aquatic, pets
    print list(db['pets']) # => dog, cat, goldfish
    print list(db.query(Q.parse('mammals & aquatic'))) # => whale
    print list(db.query(Q.parse('pets & ~aquatic'))) # => dog, cat
    print list(db.query(Q.parse('pets | aquatic'))) # => dog, cat, whale, goldfish

    db.dump(file('animals.db', 'w')) # dump discodb to a file

Python API
----------

.. automodule:: discodb
   :members:
