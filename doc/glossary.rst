
Glossary
========

.. glossary::

   conjunctive normal form
        See http://en.wikipedia.org/wiki/Conjunctive_normal_form

   disco master
        Master process that takes care of receiving Disco jobs,
        scheduling them and distributing tasks to the cluster. There
        may be many Disco masters running in parallel, as long as they
        manage separate sets of resources (CPUs).

   ichunk
        An :term:`immutable` piece of a distributed :term:`index`, stored in a file.

   immutable
        See http://en.wikipedia.org/wiki/Immutable_object

   index
        A mapping from each key in a set of keys to a multiset of values.
        Indices provide random access into a set of data.
        As an example, search engines are usually implemented using a `web index`_

   metaindex
        A mapping of metakeys to values which are keys in another index.
        A metaindex is also an index, that wraps around another index.

   job
        A sequence of the map :term:`task` and the
        reduce :term:`task`. Started by calling the
        :meth:`disco.core.Disco.new_job` method.

   job functions
        Job functions are the functions that the user can specify in
        :func:`disco.job`. Currently, the functions *map*, *reduce*,
        *combiner*, *partitioner* are referred as job functions. A job
        function needs to be a :term:`pure function`.

   partitioning
        The process of dividing the key-space which results from
        :func:`disco.func.map`.
        By default it is usually assumed that the same key will
        always fall within the same partition.
        How the key-space is actually divided is determined by the
        :func:`disco.func.partition` function.

   persistent
        See http://en.wikipedia.org/wiki/Persistent_data_structure

   pure function
        The pure function always evaluates the same result value given
        the same argument value(s).  The function should not depend on
        any global variables and it must be totally self-contained. In Disco,
        all :term:`job functions` must be pure.

        See http://en.wikipedia.org/wiki/Pure_function for more information.

   task
        A Disco :term:`job` is made of map and reduce *tasks*. A task consists
        of many map or reduce *instances*.

.. _web index: http://en.wikipedia.org/wiki/Index_(search_engine)
