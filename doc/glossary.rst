
Glossary
========

.. glossary::

   disco master
        Master process that takes care of receiving Disco jobs,
        scheduling them and distributing tasks to the cluster. There
        may be many Disco masters running in parallel, as long as they
        manage separate sets of resources (CPUs).
   job
        A sequence of the map :term:`task` and the
        reduce :term:`task`. Started by calling the
        :meth:`disco.core.Disco.new_job` method.

   job functions
        Job functions are the functions that the user can specify in
        :func:`disco.job`. Currently, the functions *map*, *reduce*,
        *combiner*, *partitioner* are referred as job functions. A job
        function needs to be a :term:`pure function`.

   pure function
        The pure function always evaluates the same result value given
        the same argument value(s).  The function should not depend on
        any global variables and it must be totally self-contained. In Disco,
        all :term:`job functions` must be pure.

        See http://en.wikipedia.org/wiki/Pure_function for more information.

   task
        A Disco :term:`job` is made of map and reduce *tasks*. A task consists
        of many map or reduce *instances*.
