:mod:`discodb` -- An efficient, immutable, persistent mapping object
====================================================================

`discodb` is comprised of a low-level data structure implemented in C,
and a high-level :class:`discodb.DiscoDB` class which exposes a dict-like
interface for using the low-level data structure from Python.

discodbs are :term:`persistent`, which means that once created in memory,
they can be easily serialized and written to a file.
The benefit of this is that after they have been persisted,
instantiating them from disk and key lookups are lightning-fast operations.

discodbs are also :term:`immutable`, which means that once they are created,
they cannot be modified.

The format of a discodb file essentially looks like this:

.. image:: images/discodb_format.png

The benefits of these properties are realized when you need repeated,
random-access to data, especially when the dataset is too large to fit
in memory at once.
discodbs are a key component in Disco's builtin distributed indexing system,
:ref:`discodex`

.. automodule:: discodb
   :members:
