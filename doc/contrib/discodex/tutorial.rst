Tutorial
========

#. Install discodex

::

        make install-discodex

#. Start disco

::

        disco start

#. Toy example using rawparse/prefixmetakeyer

::

        echo raw://hello:world | discodex index | xargs discodex get
        discodex list

In general, it is not safe to pipe output of index to get, since the name of the index will return right away, but in real indices it won't build the index so quickly
Using the name of the index you see listed:

::
        discodex clone <index> toyindex
        discodex list

Notice the prefix.
This is the prefix stored in the settings `DISCODEX_INDEX_PREFIX`.
Generally speaking, you can ignore this prefix.
The reason it exists is to provide `discodex` with its own namespace in :ref:`ddfs`, where the indices are stored.

::

        discodex keys toyindex
        discodex values toyindex
        discodex query toyindex hello

If you have :mod:`ddfscli` installed, you can try::

   ddfs ls
   ddfs ls discodex

::

        discodex list | xargs -n 1 discodex delete

#. querying the index

::

        echo raw://hello:world,hello:there,hi:world,hi:mom | discodex index
        discodex clone <index> rawindex
        discodex query rawindex hello
        discodex query rawindex hi
        discodex query rawindex hello hi
        discodex query rawindex hello,hi

Conjunctive normal form


#. Index the docs

::

        find $DISCODEX_ROOT/docs -name \*.rst | discodex index --parser noparse

#. Build a metaindex


#. advanced querying using filters
